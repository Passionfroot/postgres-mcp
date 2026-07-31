#!/usr/bin/env node
process.env.NODE_NO_WARNINGS = "1";

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

import { parseArgs, printUsage } from "./args.js";
import { createAuditLog } from "./audit-log.js";
import { applyHttpPoolDefaults, loadConfig } from "./config.js";
import { ConnectionManager } from "./connections.js";
import { startHttpServer } from "./http.js";
import { logger } from "./logger.js";
import { createSchemaCache } from "./schema/cache.js";
import { createServer } from "./server.js";
import type { Config } from "./types.js";

/**
 * A source with session_vars pins one tenant's identity (e.g. app.partner_id) for the
 * lifetime of the process, and RLS is the only thing enforcing that boundary. One shared
 * HTTP server would serve every client from that same pinned session, so refuse to start
 * rather than let the combination exist.
 */
function assertHttpSafe(config: Config) {
  const pinned = config.sources.filter(
    (source) => source.sessionVars && Object.keys(source.sessionVars).length > 0
  );
  if (pinned.length === 0) return;

  const details = pinned
    .map((source) => `${source.id} (${Object.keys(source.sessionVars ?? {}).join(", ")})`)
    .join("; ");

  throw new Error(
    `Refusing to start in HTTP mode: session_vars set on ${details}. ` +
      "session_vars pin a per-tenant identity to the process and RLS relies on that, " +
      "so a server shared across clients cannot enforce it. Use --stdio for these sources."
  );
}

let isShuttingDown = false;

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args) {
    printUsage();
    process.exit(1);
  }

  logger.info(`Loading config from ${args.configPath}`);
  let config = loadConfig(args.configPath);
  logger.info(
    `Loaded ${config.sources.length} source(s): ${config.sources.map((s) => s.id).join(", ")}`
  );

  if (args.useHttp) {
    assertHttpSafe(config);
    config = applyHttpPoolDefaults(config);
  }

  const connectionManager = new ConnectionManager(config.sources);
  const schemaCache = await createSchemaCache(config);
  const auditLog = createAuditLog(config.auditLog);

  let closeTransport: () => Promise<void>;

  if (args.useHttp) {
    // One process serves every client here, so an uncaught error must not be allowed to take the
    // whole server down with it. Log it and keep serving; the transport-level handlers still turn
    // per-request failures into per-request errors.
    process.on("uncaughtException", (err) => {
      logger.error(`Uncaught exception (server kept running): ${err instanceof Error ? err.stack ?? err.message : String(err)}`);
    });
    process.on("unhandledRejection", (reason) => {
      logger.error(`Unhandled rejection (server kept running): ${reason instanceof Error ? reason.stack ?? reason.message : String(reason)}`);
    });

    const http = await startHttpServer({
      host: args.host,
      port: args.port,
      token: args.token,
      createServer: () => createServer(config, connectionManager, schemaCache, auditLog),
    });
    closeTransport = http.close;
    logger.info(`postgres-mcp server ready on ${http.url}`);
  } else {
    const server = createServer(config, connectionManager, schemaCache, auditLog);
    const transport = new StdioServerTransport();
    await server.connect(transport);
    closeTransport = () => server.close();
    logger.info("postgres-mcp server ready on stdio");
    process.stdin.on("end", shutdown);
  }

  function shutdown() {
    if (isShuttingDown) return;
    isShuttingDown = true;

    logger.info("Shutting down...");

    const hardTimeout = setTimeout(() => {
      logger.error("Shutdown timed out, forcing exit");
      process.exit(1);
      // Above the transport drain and pool deadlines below it, so this stays a backstop for a
      // genuine hang rather than something a slow-but-working shutdown trips.
    }, 8000);
    hardTimeout.unref();

    // Transport first: stop accepting, end the sessions, drain what is in flight. Ending the pools
    // first meant pool.end() waited on a client that an in-flight query still held, so SIGTERM
    // during a query hit the hard timeout and exited 1.
    closeTransport()
      .then(() => connectionManager.shutdown())
      .then(() => {
        auditLog.close();
      })
      .then(() => {
        logger.info("Clean shutdown complete");
        process.exit(0);
      })
      .catch((err) => {
        const message = err instanceof Error ? err.message : String(err);
        logger.error(`Error during shutdown: ${message}`);
        process.exit(1);
      });
  }

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main().catch((err) => {
  logger.error("Fatal error during startup", {
    err: err instanceof Error ? err.message : String(err),
  });
  process.exit(1);
});
