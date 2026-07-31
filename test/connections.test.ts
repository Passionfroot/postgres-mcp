import { beforeEach, describe, expect, it, vi } from "vitest";

import type { SourceConfig } from "../src/types.js";

// vi.mock factories are hoisted above imports, so shared state must come from vi.hoisted.
const shared = vi.hoisted(() => ({
  poolConfigs: [] as Record<string, unknown>[],
}));

vi.mock("pg", () => {
  class FakePool {
    constructor(config: Record<string, unknown>) {
      shared.poolConfigs.push(config);
    }
    on() {}
    async connect() {
      return { query: async () => ({ rows: [] }), release() {} };
    }
    async end() {}
  }
  return {
    default: {
      Pool: FakePool,
      // connections.ts registers tz-naive type parsers at import time.
      types: { setTypeParser: () => {}, builtins: { DATE: 1082, TIMESTAMP: 1114 } },
    },
  };
});

const { ConnectionManager } = await import("../src/connections.js");

function source(overrides: Partial<SourceConfig> = {}): SourceConfig {
  return {
    id: "test",
    dsn: "postgres://localhost/test",
    readonly: false,
    timeout: 10,
    maxRows: 1000,
    poolMax: 1,
    allowMultiStatements: false,
    ...overrides,
  } as SourceConfig;
}

describe("pool timeouts", () => {
  beforeEach(() => {
    shared.poolConfigs.length = 0;
  });

  /**
   * These three collapsed onto one value is what broke queries against a healthy database:
   * connectionTimeoutMillis also bounds pg-pool's queue wait, so at pool_max 1 a second concurrent
   * query failed with "timeout exceeded when trying to connect" while the first was still running.
   */
  it("keeps statement_timeout < query_timeout < connectionTimeoutMillis", async () => {
    const manager = new ConnectionManager([source({ timeout: 10 })]);
    await manager.getPool("test");

    const config = shared.poolConfigs[0];
    const statement = config.statement_timeout as number;
    const query = config.query_timeout as number;
    const connect = config.connectionTimeoutMillis as number;

    expect(statement).toBe(10_000);
    expect(query).toBeGreaterThan(statement);
    expect(connect).toBeGreaterThan(query);
  });

  it("leaves room for a query that runs the full statement_timeout to be queued behind", async () => {
    const manager = new ConnectionManager([source({ timeout: 30 })]);
    await manager.getPool("test");

    const config = shared.poolConfigs[0];
    // A second query queued behind one that runs the full statement_timeout must not be failed by
    // the acquire timer.
    expect(config.connectionTimeoutMillis as number).toBeGreaterThan(30_000);
  });

  it("scales the timeouts with the source timeout", async () => {
    const manager = new ConnectionManager([source({ timeout: 5 })]);
    await manager.getPool("test");

    expect(shared.poolConfigs[0].statement_timeout).toBe(5_000);
    expect(shared.poolConfigs[0].query_timeout as number).toBeGreaterThan(5_000);
  });
});
