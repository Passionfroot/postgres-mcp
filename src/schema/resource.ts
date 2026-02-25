import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

import { ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";

import type { ConnectionManager } from "../connections.js";
import type { Config } from "../types.js";
import type { SchemaCache } from "./cache.js";

import { formatRelationshipMap } from "./format.js";

export function registerSchemaResource(
  server: McpServer,
  config: Config,
  connectionManager: ConnectionManager,
  schemaCache: SchemaCache
) {
  server.registerResource(
    "schema",
    new ResourceTemplate("schema://{database}", {
      list: async () => ({
        resources: config.sources.map((s) => ({
          uri: `schema://${s.id}`,
          name: `Schema: ${s.id}`,
          description: `Relationship map for ${s.id} database`,
        })),
      }),
    }),
    {
      title: "Database Schema",
      description: "Lean relationship map showing tables, Prisma model names, and FK relationships",
      mimeType: "text/plain",
    },
    async (uri, variables) => {
      const database = Array.isArray(variables.database)
        ? variables.database[0]
        : variables.database;

      const source = config.sources.find((s) => s.id === database);
      if (!source) {
        throw new Error(
          `Unknown database: ${database}. Available: ${config.sources.map((s) => s.id).join(", ")}`
        );
      }

      const pool = await connectionManager.getPool(database);
      const schema = await schemaCache.get(database, pool);
      const text = formatRelationshipMap(schema, database);

      return {
        contents: [
          {
            uri: uri.href,
            text,
          },
        ],
      };
    }
  );
}
