import { beforeEach, describe, expect, it, vi } from "vitest";

import type { AuditLogger } from "../src/audit-log.js";
import type { ConnectionManager } from "../src/connections.js";
import type { SchemaCache } from "../src/schema/cache.js";
import type { Config, SourceConfig } from "../src/types.js";

const registeredTools = new Map<string, { config: { description: string }; handler: Function }>();

vi.mock("@modelcontextprotocol/sdk/server/mcp.js", () => ({
  McpServer: class {
    registerTool(name: string, config: { description: string }, handler: Function) {
      registeredTools.set(name, { config, handler });
    }
    registerResource() {}
  },
  ResourceTemplate: class {},
}));

const warn = vi.fn();
const error = vi.fn();
vi.mock("../src/logger.js", () => ({
  logger: {
    debug: vi.fn(),
    info: vi.fn(),
    warn: (...args: unknown[]) => warn(...args),
    error: (...args: unknown[]) => error(...args),
  },
}));

const { createServer } = await import("../src/server.js");

function makeSource(overrides: Partial<SourceConfig> = {}): SourceConfig {
  return {
    id: "zest",
    dsn: "postgresql://example/db",
    readonly: true,
    maxRows: 10,
    timeout: 5,
    poolMax: 1,
    allowMultiStatements: false,
    readOnlyQueries: true,
    ...overrides,
  };
}

function build(sources: SourceConfig[]) {
  registeredTools.clear();
  const config: Config = { sources };
  const connectionManager = {
    getPool: vi.fn().mockResolvedValue({
      connect: vi.fn(),
    }),
  } as unknown as ConnectionManager;
  const schemaCache = { get: vi.fn().mockResolvedValue({}) } as unknown as SchemaCache;
  const auditLog = { log: vi.fn() } as unknown as AuditLogger;

  createServer(config, connectionManager, schemaCache, auditLog);
  return { auditLog, executeSql: registeredTools.get("execute_sql")! };
}

beforeEach(() => {
  warn.mockClear();
  error.mockClear();
});

describe("execute_sql tool description", () => {
  it("names the sources that only accept read-only SELECT", () => {
    const { executeSql } = build([makeSource(), makeSource({ id: "dev", readOnlyQueries: false })]);

    expect(executeSql.config.description).toContain(
      "Sources that accept ONLY read-only SELECT statements: zest."
    );
    expect(executeSql.config.description).toContain("set_config()");
    expect(executeSql.config.description).toContain("EXPLAIN without ANALYZE is allowed");
  });

  it("says nothing when no source is restricted", () => {
    const { executeSql } = build([makeSource({ readOnlyQueries: false })]);

    expect(executeSql.config.description).not.toContain("read-only SELECT statements");
  });
});

describe("blocked query logging", () => {
  it("emits a distinct warn line for a rejected query, without the SQL", async () => {
    const { executeSql, auditLog } = build([makeSource()]);

    const result = await executeSql.handler({
      database: "zest",
      query: "SELECT set_config('app.partner_id', 'tenantB', false)",
    });

    expect(result.isError).toBe(true);
    expect(warn).toHaveBeenCalledWith("blocked session-mutating query", {
      source: "zest",
      reason: expect.stringContaining("set_config("),
    });
    const [, payload] = warn.mock.calls[0] as [string, Record<string, unknown>];
    expect(JSON.stringify(payload)).not.toContain("app.partner_id");
    expect(auditLog.log).toHaveBeenCalled();
  });

  it("does not emit the warn line for an ordinary query error", async () => {
    const { executeSql } = build([makeSource()]);

    await executeSql.handler({ database: "nope", query: "SELECT 1" });

    expect(warn).not.toHaveBeenCalled();
  });
});
