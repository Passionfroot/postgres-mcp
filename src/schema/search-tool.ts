import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

import { z } from "zod";

import type { ConnectionManager } from "../connections.js";
import type { Config } from "../types.js";
import type { SchemaCache } from "./cache.js";

import { logger } from "../logger.js";
import { mcpErrorResult, mcpTextResult, resolveSource } from "../mcp-helpers.js";
import { formatSearchResults, searchTables } from "./search.js";

export function registerSearchTool(
  server: McpServer,
  config: Config,
  connectionManager: ConnectionManager,
  schemaCache: SchemaCache
) {
  const sourceIds = config.sources.map((s) => s.id);

  server.registerTool(
    "search_objects",
    {
      title: "Search Schema Objects",
      description:
        "Search for tables by Prisma model name or SQL table name. Returns column detail including types, nullability, defaults, and enum values. Use this to look up specific tables before writing queries.",
      inputSchema: {
        database: z.string().describe(`Database source ID. Available: ${sourceIds.join(", ")}`),
        pattern: z
          .string()
          .describe(
            "Table name or Prisma model name to search for (e.g., 'User', 'partnerUsers', 'collaboration')"
          ),
      },
    },
    async ({ database, pattern }) => {
      try {
        const resolved = resolveSource(database, config);
        if (!resolved.ok) return resolved.error;

        const pool = await connectionManager.getPool(database);
        const schema = await schemaCache.get(database, pool);
        const results = searchTables(schema, pattern);
        const enumResolver = (udtName: string) => schemaCache.getEnumValues(udtName);
        const formatted = formatSearchResults(results, enumResolver);

        return mcpTextResult(formatted);
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        logger.error(`search_objects error for database "${database}": ${message}`);

        return mcpErrorResult(message);
      }
    }
  );
}
