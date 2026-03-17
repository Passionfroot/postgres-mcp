import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";

import type { AuditLogger } from "./audit-log.js";
import type { ConnectionManager } from "./connections.js";
import type { SchemaCache } from "./schema/cache.js";
import type { Config } from "./types.js";

import { logger } from "./logger.js";
import { mcpErrorResult, mcpTextResult, resolveSource } from "./mcp-helpers.js";
import { executeQuery } from "./query.js";
import { registerSchemaResource } from "./schema/resource.js";
import { registerSearchTool } from "./schema/search-tool.js";

/**
 * Merge per-request session vars over config defaults.
 * Config keys act as a whitelist — request keys not declared in config are rejected.
 * Config values serve as defaults; per-request values override them.
 */
export function mergeSessionVars(
  configVars: Record<string, string> | undefined,
  requestVars: Record<string, string> | undefined
): Record<string, string> | undefined {
  if (!configVars) {
    if (requestVars && Object.keys(requestVars).length > 0) {
      throw new Error(
        "This source does not accept session_vars. Declare allowed keys in the source config."
      );
    }
    return undefined;
  }

  if (!requestVars) {
    // Use config defaults, filtering out empty-string placeholders
    const defaults = Object.fromEntries(
      Object.entries(configVars).filter(([, v]) => v !== "")
    );
    return Object.keys(defaults).length > 0 ? defaults : undefined;
  }

  const allowedKeys = new Set(Object.keys(configVars));
  const unknownKeys = Object.keys(requestVars).filter((k) => !allowedKeys.has(k));
  if (unknownKeys.length > 0) {
    throw new Error(
      `Unknown session_vars keys: ${unknownKeys.join(", ")}. Allowed: ${[...allowedKeys].join(", ")}`
    );
  }

  const merged: Record<string, string> = {};
  for (const [key, defaultValue] of Object.entries(configVars)) {
    const value = requestVars[key] ?? defaultValue;
    if (value !== "") {
      merged[key] = value;
    }
  }

  return Object.keys(merged).length > 0 ? merged : undefined;
}

export function createServer(
  config: Config,
  connectionManager: ConnectionManager,
  schemaCache: SchemaCache,
  auditLog: AuditLogger
) {
  const server = new McpServer(
    {
      name: "postgres-mcp",
      version: "1.0.0",
    },
    {
      instructions:
        "Read schema://[database] for a relationship overview of all tables and their FK connections. " +
        "Use search_objects to look up column-level detail for specific tables before writing queries. " +
        "Always check the schema before querying to avoid guessing table or column names.",
    }
  );

  const sourceIds = config.sources.map((s) => s.id);

  const readonlySources = config.sources.filter((s) => s.readonly).map((s) => s.id);
  const readonlyNote =
    readonlySources.length > 0
      ? ` Sources configured as read-only: ${readonlySources.join(", ")}.`
      : "";

  const sourcesWithSessionVars = config.sources.filter(
    (s) => s.sessionVars && Object.keys(s.sessionVars).length > 0
  );
  const sessionVarsNote =
    sourcesWithSessionVars.length > 0
      ? ` Sources that accept session_vars: ${sourcesWithSessionVars.map((s) => `${s.id} (keys: ${Object.keys(s.sessionVars!).join(", ")})`).join("; ")}.`
      : "";

  server.registerTool(
    "execute_sql",
    {
      title: "Execute SQL",
      description: `Execute SQL against a configured PostgreSQL database. Returns JSON rows.${readonlyNote}${sessionVarsNote}`,
      inputSchema: {
        database: z.string().describe(`Database source ID. Available: ${sourceIds.join(", ")}`),
        query: z.string().describe("SQL query to execute"),
        session_vars: z
          .record(z.string(), z.string())
          .optional()
          .describe(
            "Optional per-request session variables (e.g. for RLS). Keys must be declared in the source config."
          ),
      },
    },
    async ({ database, query, session_vars }) => {
      const start = performance.now();
      try {
        const resolved = resolveSource(database, config);
        if (!resolved.ok) return resolved.error;

        const { source } = resolved;

        const mergedSessionVars = mergeSessionVars(
          source.sessionVars,
          session_vars
        );

        const pool = await connectionManager.getPool(database);
        const result = await executeQuery(pool, query, source.maxRows, {
          readonly: source.readonly,
          allowMultiStatements: source.allowMultiStatements,
          role: source.role,
          sessionVars: mergedSessionVars,
        });

        auditLog.log({
          source: database,
          sql: query,
          durationMs: Math.round(performance.now() - start),
          rowCount: result.rowCount,
          truncated: result.truncated,
        });

        return mcpTextResult(JSON.stringify(result, null, 2));
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        logger.error(`execute_sql error for database "${database}": ${message}`);

        auditLog.log({
          source: database,
          sql: query,
          durationMs: Math.round(performance.now() - start),
          rowCount: 0,
          truncated: false,
          error: message,
        });

        return mcpErrorResult(message);
      }
    }
  );

  registerSchemaResource(server, config, connectionManager, schemaCache);
  registerSearchTool(server, config, connectionManager, schemaCache);

  logger.debug("MCP server instance created");

  return server;
}
