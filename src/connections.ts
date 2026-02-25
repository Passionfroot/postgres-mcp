import pg from "pg";

import type { SourceConfig, TunnelHandle } from "./types.js";

import { logger } from "./logger.js";
import { createTunnel, parseDsnHostPort } from "./tunnel.js";

function getErrorMessage(err: unknown) {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Rewrite a PostgreSQL DSN's host and port while preserving user, password, database, and query
 * parameters.
 */
export function rewriteDsnHostPort(originalDsn: string, newHost: string, newPort: number) {
  const url = new URL(originalDsn);
  url.hostname = newHost;
  url.port = String(newPort);
  return url.toString();
}

interface PoolEntry {
  pool: pg.Pool;
  dead: boolean;
}

const KEEPALIVE_INTERVAL_MS = 30_000;

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
          logger.error(`Error shutting down source "${sourceId}": ${getErrorMessage(err)}`);
        }
      })
    );

    logger.info("Connection manager shut down complete");
  }

  private async createPool(source: SourceConfig) {
    let connectionString = source.dsn;

    if (source.sshHost && source.sshUser && source.sshKey) {
      const { host: remoteHost, port: remotePort } = parseDsnHostPort(source.dsn);

      const tunnel = await createTunnel({
        sshHost: source.sshHost,
        sshUser: source.sshUser,
        sshKeyPath: source.sshKey,
        remoteHost,
        remotePort,
        keepaliveInterval: KEEPALIVE_INTERVAL_MS,
      });

      this.tunnels.set(source.id, tunnel);
      connectionString = rewriteDsnHostPort(source.dsn, tunnel.localHost, tunnel.localPort);
    }

    const pool = new pg.Pool({
      connectionString,
      max: source.poolMax,
      idleTimeoutMillis: 5_000,
      statement_timeout: source.timeout * 1000,
      allowExitOnIdle: true,
    });

    pool.on("error", (err) => {
      logger.error(`Pool error for source "${source.id}": ${err.message}`);
      this.markPoolDead(source.id);
    });

    this.pools.set(source.id, { pool, dead: false });

    const isTunneled = this.tunnels.has(source.id);
    logger.info(
      `Pool created for source "${source.id}" (max=${source.poolMax}, timeout=${source.timeout}s, readonly=${source.readonly}${isTunneled ? ", tunneled" : ""})`
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
        logger.error(`Error ending pool for source "${sourceId}": ${getErrorMessage(err)}`);
      }
      this.pools.delete(sourceId);
    }

    const tunnel = this.tunnels.get(sourceId);
    if (tunnel) {
      try {
        await tunnel.close();
      } catch (err) {
        logger.error(`Error closing tunnel for source "${sourceId}": ${getErrorMessage(err)}`);
      }
      this.tunnels.delete(sourceId);
    }
  }
}
