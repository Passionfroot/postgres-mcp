import pg from "pg";
import type { TypeId } from "pg-types";
import { parse as parsePgArray } from "postgres-array";

import type { SourceConfig, TunnelHandle } from "./types.js";

import { logger } from "./logger.js";
import { createTunnel, parseDsnHostPort } from "./tunnel.js";

// pg's default parsers turn tz-naive timestamp/date values into JS Dates interpreted in the
// server's local timezone, so JSON output silently shifts them (e.g. UTC-stored "11:43" becomes
// "09:43Z" on a UTC+2 machine, and dates can move a whole day). Return the literal strings instead.
pg.types.setTypeParser(pg.types.builtins.DATE, (value) => value);
pg.types.setTypeParser(pg.types.builtins.TIMESTAMP, (value) => value);
// Array OIDs are missing from pg-types' TypeId enum, hence the casts.
pg.types.setTypeParser(1182 as TypeId, (value) => parsePgArray(value)); // date[]
pg.types.setTypeParser(1115 as TypeId, (value) => parsePgArray(value)); // timestamp[]

function getErrorMessage(err: unknown) {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Rewrite a PostgreSQL DSN's host and port while preserving user, password, database, and query
 * parameters.
 */
export function rewriteDsnHostPort(
  originalDsn: string,
  newHost: string,
  newPort: number
) {
  const url = new URL(originalDsn);
  url.hostname = newHost;
  url.port = String(newPort);
  return url.toString();
}

interface PoolEntry {
  pool: pg.Pool;
  dead: boolean;
}

const KEEPALIVE_INTERVAL_MS = 10_000;

/**
 * These grace values keep statement_timeout < query_timeout < connectionTimeoutMillis. Collapsing
 * them onto one value breaks queries against a healthy database.
 *
 * query_timeout is a client-side timer needing no round trip, so setting it equal to
 * statement_timeout makes it win the race and the server's 57014, along with its "simplify the
 * query" hint, never reaches the caller. It is only a backstop for a half-dead tunnel where the
 * server never answers, so it sits above statement_timeout.
 *
 * connectionTimeoutMillis bounds pg-pool's queue wait as well as the connect. At the default
 * pool_max of 1 a second concurrent query waits in that queue, so any value at or below the
 * longest legitimate query fails it with "timeout exceeded when trying to connect".
 */
const QUERY_TIMEOUT_GRACE_MS = 2_000;
const CONNECT_TIMEOUT_GRACE_MS = 5_000;

/**
 * Manages the full connection lifecycle for all configured database sources.
 *
 * Pools are created lazily on first getPool() call for a given source. SSH-enabled sources get
 * tunneled connections. Readonly sources enforce read-only sessions on every new pool connection.
 * Dead pools (from errors) auto-recreate on next access.
 */
export class ConnectionManager {
  private sources: Map<string, SourceConfig>;
  private pools: Map<string, PoolEntry> = new Map();
  private tunnels: Map<string, TunnelHandle> = new Map();

  constructor(sources: SourceConfig[]) {
    this.sources = new Map(sources.map((s) => [s.id, s]));
  }

  getSource(sourceId: string) {
    const source = this.sources.get(sourceId);
    if (!source) {
      throw new Error(`Unknown source: ${sourceId}`);
    }
    return source;
  }

  async getPool(sourceId: string) {
    const source = this.getSource(sourceId);

    const existing = this.pools.get(sourceId);
    if (existing && !existing.dead) {
      return existing.pool;
    }

    if (existing?.dead) {
      logger.info(`Recreating dead pool for source "${sourceId}"`);
      await this.destroyPoolAndTunnel(sourceId);
    }

    return this.createPool(source);
  }

  async shutdown() {
    logger.info("Shutting down connection manager...");

    const sourceIds = [...this.pools.keys()];

    await Promise.all(
      sourceIds.map(async (sourceId) => {
        try {
          await this.destroyPoolAndTunnel(sourceId);
        } catch (err) {
          logger.error(
            `Error shutting down source "${sourceId}": ${getErrorMessage(err)}`
          );
        }
      })
    );

    logger.info("Connection manager shut down complete");
  }

  private async createPool(source: SourceConfig) {
    let connectionString = source.dsn;

    if (source.sshHost && source.sshUser && source.sshKey) {
      const { host: remoteHost, port: remotePort } = parseDsnHostPort(
        source.dsn
      );

      const tunnel = await createTunnel(
        {
          sshHost: source.sshHost,
          sshUser: source.sshUser,
          sshKeyPath: source.sshKey,
          remoteHost,
          remotePort,
          keepaliveInterval: KEEPALIVE_INTERVAL_MS,
        },
        (reason) => {
          // The tunnel died after establishment (e.g. sleep killed the socket). Mark the pool dead
          // so the next getPool tears both down and recreates them, instead of wedging on the dead
          // tunnel until the process is killed.
          logger.warn(
            `Tunnel down for source "${source.id}" (${reason}); marking pool dead`
          );
          this.markPoolDead(source.id);
        }
      );

      this.tunnels.set(source.id, tunnel);
      connectionString = rewriteDsnHostPort(
        source.dsn,
        tunnel.localHost,
        tunnel.localPort
      );
    }

    const statementTimeoutMs = source.timeout * 1000;
    const queryTimeoutMs = statementTimeoutMs + QUERY_TIMEOUT_GRACE_MS;

    const pool = new pg.Pool({
      connectionString,
      max: source.poolMax,
      idleTimeoutMillis: 5_000,
      statement_timeout: statementTimeoutMs,
      allowExitOnIdle: true,
      // Bound acquiring a connection so a half-dead tunnel, where the peer never answers and the
      // ssh keepalive has not tripped yet, fails instead of hanging. Together with the tunnel's
      // onDown above, the next call recreates the pool and tunnel rather than wedging.
      connectionTimeoutMillis: queryTimeoutMs + CONNECT_TIMEOUT_GRACE_MS,
      query_timeout: queryTimeoutMs,
    });

    pool.on("error", (err) => {
      logger.error(`Pool error for source "${source.id}": ${err.message}`);
      this.markPoolDead(source.id);
    });

    this.pools.set(source.id, { pool, dead: false });

    const isTunneled = this.tunnels.has(source.id);
    logger.info(
      `Pool created for source "${source.id}" (max=${source.poolMax}, timeout=${
        source.timeout
      }s, readonly=${source.readonly}${isTunneled ? ", tunneled" : ""})`
    );

    return pool;
  }

  private markPoolDead(sourceId: string) {
    const entry = this.pools.get(sourceId);
    if (entry) {
      entry.dead = true;
    }
  }

  private async destroyPoolAndTunnel(sourceId: string) {
    const poolEntry = this.pools.get(sourceId);
    if (poolEntry) {
      try {
        await poolEntry.pool.end();
      } catch (err) {
        logger.error(
          `Error ending pool for source "${sourceId}": ${getErrorMessage(err)}`
        );
      }
      this.pools.delete(sourceId);
    }

    const tunnel = this.tunnels.get(sourceId);
    if (tunnel) {
      try {
        await tunnel.close();
      } catch (err) {
        logger.error(
          `Error closing tunnel for source "${sourceId}": ${getErrorMessage(
            err
          )}`
        );
      }
      this.tunnels.delete(sourceId);
    }
  }
}
