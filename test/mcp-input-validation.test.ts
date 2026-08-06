import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { describe, expect, it } from "vitest";

import type { AuditLogger } from "../src/audit-log.js";
import type { ConnectionManager } from "../src/connections.js";
import type { SchemaCache } from "../src/schema/cache.js";
import type { Config, SourceConfig } from "../src/types.js";

import { createServer } from "../src/server.js";

// This exercises the real @modelcontextprotocol/sdk McpServer, not the mocked one in
// server.test.ts. Malformed arguments never reach our handlers: the SDK builds a zod object
// from our raw inputSchema shape and rejects bad input itself. That is exactly the surface a
// zod major bump can silently change (issue shape, message wording, or zod-to-json-schema
// failing to read zod 4 internals) without a single line of our own code breaking.

function makeSource(overrides: Partial<SourceConfig> = {}): SourceConfig {
  return {
    id: "local",
    dsn: "postgresql://example/db",
    readonly: false,
    maxRows: 100,
    timeout: 10,
    poolMax: 1,
    poolMaxExplicit: false,
    maxResponseBytes: 1_000_000,
    allowMultiStatements: false,
    readOnlyQueries: false,
    ...overrides,
  };
}

async function connectRealClient(sources: SourceConfig[]) {
  const config: Config = { sources };
  const connectionManager = {} as unknown as ConnectionManager;
  const schemaCache = {} as unknown as SchemaCache;
  const auditLog = { log: () => undefined } as unknown as AuditLogger;

  const server = createServer(config, connectionManager, schemaCache, auditLog);
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  const client = new Client({ name: "input-validation-test", version: "1.0.0" });
  await Promise.all([client.connect(clientTransport), server.connect(serverTransport)]);

  return {
    client,
    close: async () => {
      await client.close();
      await server.close();
    },
  };
}

describe("execute_sql input validation (real MCP SDK)", () => {
  it("rejects a call missing the required query field with a readable error", async () => {
    const { client, close } = await connectRealClient([makeSource()]);
    try {
      const result = await client.callTool({
        name: "execute_sql",
        arguments: { database: "local" },
      });

      expect(result.isError).toBe(true);
      const text = (result.content as Array<{ text: string }>)[0].text;
      expect(text).toContain("query");
      expect(text).not.toContain("[object Object]");
      expect(text).not.toContain("undefined:undefined");
    } finally {
      await close();
    }
  });

  it("rejects a call where database is the wrong type with a readable error", async () => {
    const { client, close } = await connectRealClient([makeSource()]);
    try {
      const result = await client.callTool({
        name: "execute_sql",
        arguments: { database: 42, query: "SELECT 1" },
      });

      expect(result.isError).toBe(true);
      const text = (result.content as Array<{ text: string }>)[0].text;
      expect(text).toContain("database");
      expect(text).not.toContain("[object Object]");
    } finally {
      await close();
    }
  });
});

describe("search_objects input validation (real MCP SDK)", () => {
  it("rejects a call missing the required pattern field with a readable error", async () => {
    const { client, close } = await connectRealClient([makeSource()]);
    try {
      const result = await client.callTool({
        name: "search_objects",
        arguments: { database: "local" },
      });

      expect(result.isError).toBe(true);
      const text = (result.content as Array<{ text: string }>)[0].text;
      expect(text).toContain("pattern");
      expect(text).not.toContain("[object Object]");
    } finally {
      await close();
    }
  });
});

describe("tool schema listing (real MCP SDK)", () => {
  it("converts both tools' zod input shapes to JSON schema without throwing", async () => {
    const { client, close } = await connectRealClient([makeSource()]);
    try {
      const { tools } = await client.listTools();
      const names = tools.map((t) => t.name);
      expect(names).toContain("execute_sql");
      expect(names).toContain("search_objects");

      const executeSql = tools.find((t) => t.name === "execute_sql")!;
      expect(executeSql.inputSchema.type).toBe("object");
      expect(executeSql.inputSchema.required).toEqual(
        expect.arrayContaining(["database", "query"])
      );

      const searchObjects = tools.find((t) => t.name === "search_objects")!;
      expect(searchObjects.inputSchema.type).toBe("object");
      expect(searchObjects.inputSchema.required).toEqual(
        expect.arrayContaining(["database", "pattern"])
      );
    } finally {
      await close();
    }
  });
});
