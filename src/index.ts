#!/usr/bin/env node
process.env.NODE_NO_WARNINGS = "1";

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

import { createAuditLog } from "./audit-log.js";
import { loadConfig } from "./config.js";
import { ConnectionManager } from "./connections.js";
import { startHttpServer } from "./http.js";
import { logger } from "./logger.js";
import { createSchemaCache } from "./schema/cache.js";
import { createServer } from "./server.js";
import type { Config } from "./types.js";

const DEFAULT_PORT = 7803;
const DEFAULT_HOST = "127.0.0.1";

function printUsage() {
  console.error("Usage: postgres-mcp <config-file> [options]");
  console.error("  config-file        Path to TOML configuration file");
  console.error("");
  console.error("Options:");
  console.error("  --stdio            Serve over stdio, one process per client (default)");
  console.error("  --http             Serve over Streamable HTTP, shared across clients");
  console.error(`  --port <n>         HTTP port (default ${DEFAULT_PORT}, env POSTGRES_MCP_PORT)`);
  console.error(`  --host <addr>      HTTP bind address (default ${DEFAULT_HOST}, env POSTGRES_MCP_HOST)`);
  console.error("  --token <secret>   Require 'Authorization: Bearer <secret>' (env POSTGRES_MCP_TOKEN)");
  console.error("");
  console.error("Note: --http is refused when any source sets session_vars. Those pin a");
  console.error("per-tenant identity to the process, which a shared server cannot honour.");
}

interface ParsedArgs {
  configPath: string;
  useHttp: boolean;
  host: string;
  port: number;
  token?: string;
}

function parseArgs(argv: string[]): ParsedArgs | undefined {
  const positional: string[] = [];
  // stdio stays the default: the per-tenant production path depends on it.
  let useHttp = false;
  let host = process.env.POSTGRES_MCP_HOST ?? DEFAULT_HOST;
  let port = Number(process.env.POSTGRES_MCP_PORT ?? DEFAULT_PORT);
  let token = process.env.POSTGRES_MCP_TOKEN;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--http") useHttp = true;
    else if (arg === "--stdio") useHttp = false;
    else if (arg === "--port") port = Number(argv[++i]);
    else if (arg === "--host") host = argv[++i];
    else if (arg === "--token") token = argv[++i];
    else if (arg === "--help" || arg === "-h") return undefined;
    else if (arg.startsWith("-")) {
      console.error(`Unknown option: ${arg}`);
      return undefined;
    } else positional.push(arg);
  }

  const configPath = positional[0];
  if (!configPath) return undefined;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    console.error(`Invalid port: ${port}`);
    return undefined;
  }

  return { configPath, useHttp, host, port, token };
}

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
  const config = loadConfig(args.configPath);
  logger.info(
    `Loaded ${config.sources.length} source(s): ${config.sources.map((s) => s.id).join(", ")}`
  );

  if (args.useHttp) assertHttpSafe(config);

  const connectionManager = new ConnectionManager(config.sources);
  const schemaCache = await createSchemaCache(config);
  const auditLog = createAuditLog(config.auditLog);

  let closeTransport: () => Promise<void>;

  if (args.useHttp) {
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
    }, 5000);
    hardTimeout.unref();

    connectionManager
      .shutdown()
      .then(() => {
        auditLog.close();
        return closeTransport();
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
