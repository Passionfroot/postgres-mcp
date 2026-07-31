import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";

import type { AuditLogger } from "./audit-log.js";
import type { ConnectionManager } from "./connections.js";
import type { SchemaCache } from "./schema/cache.js";
import type { Config } from "./types.js";

import { expandStarColumns } from "./expand-star.js";
import { logger } from "./logger.js";
import { mcpErrorResult, mcpTextResult, resolveSource } from "./mcp-helpers.js";
import { executeQuery, ReadOnlyQueryError } from "./query.js";
import { registerSchemaResource } from "./schema/resource.js";
import { registerSearchTool } from "./schema/search-tool.js";

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

  const restrictedSources = config.sources
    .filter((s) => s.readOnlyQueries)
    .map((s) => s.id);
  const restrictedNote =
    restrictedSources.length > 0
      ? ` Sources that accept ONLY read-only SELECT statements: ${restrictedSources.join(", ")}.` +
        " On these, do not send SET, RESET, SET ROLE, set_config(), DDL, DML, transaction control," +
        " or multiple statements in one call. They are rejected before reaching the database." +
        " EXPLAIN without ANALYZE is allowed; EXPLAIN ANALYZE is not."
      : "";

  server.registerTool(
    "execute_sql",
    {
      title: "Execute SQL",
      description: `Execute SQL against a configured PostgreSQL database. Returns JSON rows.${readonlyNote}${restrictedNote}`,
      inputSchema: {
        database: z.string().describe(`Database source ID. Available: ${sourceIds.join(", ")}`),
        query: z.string().describe("SQL query to execute"),
      },
    },
    async ({ database, query }) => {
      const start = performance.now();
      try {
        const resolved = resolveSource(database, config);
        if (!resolved.ok) return resolved.error;

        const { source } = resolved;
        const pool = await connectionManager.getPool(database);
        const schema = await schemaCache.get(database, pool, {
          role: source.role,
          sessionVars: source.sessionVars,
        });
        const result = await executeQuery(pool, query, source.maxRows, {
          readonly: source.readonly,
          allowMultiStatements: source.allowMultiStatements,
          readOnlyQueries: source.readOnlyQueries,
          role: source.role,
          sessionVars: source.sessionVars,
          expandStar: (sql) => expandStarColumns(sql, schema),
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
        // The guard rejecting a query is a security event, not a syntax error. Both
        // production deployments run without an [audit_log], so emit a distinct
        // greppable line that does not depend on one being configured. Reason and
        // source only: the SQL itself can carry user data.
        if (err instanceof ReadOnlyQueryError) {
          logger.warn("blocked session-mutating query", {
            source: database,
            reason: err.reason,
          });
        }
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
