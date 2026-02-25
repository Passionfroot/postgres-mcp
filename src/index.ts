#!/usr/bin/env node
process.env.NODE_NO_WARNINGS = "1";

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

import { createAuditLog } from "./audit-log.js";
import { loadConfig } from "./config.js";
import { ConnectionManager } from "./connections.js";
import { logger } from "./logger.js";
import { createSchemaCache } from "./schema/cache.js";
import { createServer } from "./server.js";

function printUsage() {
  console.error("Usage: postgres-mcp <config-file>");
  console.error("  config-file: Path to TOML configuration file");
}

let isShuttingDown = false;

async function main() {
  const configPath = process.argv[2];

  if (!configPath) {
    printUsage();
    process.exit(1);
  }

  logger.info(`Loading config from ${configPath}`);
  const config = loadConfig(configPath);
  logger.info(
    `Loaded ${config.sources.length} source(s): ${config.sources.map((s) => s.id).join(", ")}`
  );

  const connectionManager = new ConnectionManager(config.sources);
  const schemaCache = await createSchemaCache(config);
  const auditLog = createAuditLog(config.auditLog);
  const server = createServer(config, connectionManager, schemaCache, auditLog);
  const transport = new StdioServerTransport();

  function shutdown() {
    if (isShuttingDown) return;
    isShuttingDown = true;

    logger.info("Shutting down...");

    const hardTimeout = setTimeout(() => {
      logger.error("Shutdown timed out, forcing exit");
      process.exit(1);
    }, 5000);
    hardTimeout.unref();

    connectionManager
      .shutdown()
      .then(() => {
        auditLog.close();
        return server.close();
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
  process.stdin.on("end", shutdown);

  await server.connect(transport);
  logger.info("postgres-mcp server ready on stdio");
}

main().catch((err) => {
  logger.error("Fatal error during startup", {
    err: err instanceof Error ? err.message : String(err),
  });
  process.exit(1);
});
