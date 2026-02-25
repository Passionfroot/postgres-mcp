import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { SourceConfig } from "../../src/types.js";

import { ConnectionManager } from "../../src/connections.js";
import { executeQuery } from "../../src/query.js";

const TEST_DSN = process.env.POSTGRES_MCP_TEST_DSN ?? "postgresql://localhost/postgres";

const localSource: SourceConfig = {
  id: "local",
  dsn: TEST_DSN,
  readonly: false,
  maxRows: 10,
  timeout: 5,
  poolMax: 1,
  allowMultiStatements: false,
};

const defaultOptions = { readonly: false, allowMultiStatements: false };

async function checkDbAvailable() {
  try {
    const testPool = new pg.Pool({ connectionString: TEST_DSN, max: 1 });
    await testPool.query("SELECT 1");
    await testPool.end();
    return true;
  } catch {
    return false;
  }
}

const isDbAvailable = await checkDbAvailable();

let connectionManager: ConnectionManager;

beforeAll(() => {
  if (isDbAvailable) {
    connectionManager = new ConnectionManager([localSource]);
  }
});

afterAll(async () => {
  if (connectionManager) {
    await connectionManager.shutdown();
  }
});

describe.skipIf(!isDbAvailable)("execute_sql integration", () => {
  it("executes SELECT and returns rows", async () => {
    const pool = await connectionManager.getPool("local");
    const result = await executeQuery(pool, "SELECT 1 as value", 10, defaultOptions);

    expect(result.rows).toEqual([{ value: 1 }]);
    expect(result.rowCount).toBe(1);
    expect(result.truncated).toBe(false);
  });

  it("returns column names as keys", async () => {
    const pool = await connectionManager.getPool("local");
    const result = await executeQuery(
      pool,
      "SELECT 'hello' as greeting, 42 as number",
      10,
      defaultOptions
    );

    expect(result.rows[0]).toEqual({ greeting: "hello", number: 42 });
  });

  it("auto-caps rows at maxRows", async () => {
    const pool = await connectionManager.getPool("local");
    const result = await executeQuery(
      pool,
      "SELECT generate_series(1, 100) as n",
      10,
      defaultOptions
    );

    expect(result.rowCount).toBe(10);
    expect(result.truncated).toBe(true);
    expect(result.rows).toHaveLength(10);
  });

  it("preserves user LIMIT when smaller", async () => {
    const pool = await connectionManager.getPool("local");
    const result = await executeQuery(
      pool,
      "SELECT generate_series(1, 100) as n LIMIT 5",
      10,
      defaultOptions
    );

    expect(result.rowCount).toBe(5);
    expect(result.truncated).toBe(false);
  });

  it("returns empty result", async () => {
    const pool = await connectionManager.getPool("local");
    const result = await executeQuery(pool, "SELECT 1 WHERE false", 10, defaultOptions);

    expect(result.rows).toEqual([]);
    expect(result.rowCount).toBe(0);
    expect(result.truncated).toBe(false);
  });

  it("surfaces PG errors with code", async () => {
    const pool = await connectionManager.getPool("local");

    await expect(
      executeQuery(pool, "SELECT * FROM nonexistent_table_xyz_123", 10, defaultOptions)
    ).rejects.toThrow("42P01");
  });

  it("handles timeout", async () => {
    const timeoutSource: SourceConfig = {
      id: "timeout-test",
      dsn: TEST_DSN,
      readonly: false,
      maxRows: 10,
      timeout: 1,
      poolMax: 1,
      allowMultiStatements: false,
    };
    const timeoutManager = new ConnectionManager([timeoutSource]);

    try {
      const pool = await timeoutManager.getPool("timeout-test");
      await expect(executeQuery(pool, "SELECT pg_sleep(10)", 10, defaultOptions)).rejects.toThrow(
        "timed out"
      );
    } finally {
      await timeoutManager.shutdown();
    }
  });
});

describe.skipIf(!isDbAvailable)("readonly enforcement integration", () => {
  const readonlySource: SourceConfig = {
    id: "readonly-test",
    dsn: TEST_DSN,
    readonly: true,
    maxRows: 10,
    timeout: 5,
    poolMax: 1,
    allowMultiStatements: false,
  };

  const readonlyOptions = { readonly: true, allowMultiStatements: false };

  it("rejects DDL on readonly source", async () => {
    const readonlyManager = new ConnectionManager([readonlySource]);

    try {
      const pool = await readonlyManager.getPool("readonly-test");
      await expect(
        executeQuery(pool, "CREATE TEMP TABLE test_readonly_check (id int)", 10, readonlyOptions)
      ).rejects.toThrow();
    } finally {
      await readonlyManager.shutdown();
    }
  });

  it("rejects INSERT on readonly source", async () => {
    const readonlyManager = new ConnectionManager([readonlySource]);

    try {
      const pool = await readonlyManager.getPool("readonly-test");
      await expect(
        executeQuery(
          pool,
          "INSERT INTO pg_type (typname) VALUES ('test_readonly')",
          10,
          readonlyOptions
        )
      ).rejects.toThrow();
    } finally {
      await readonlyManager.shutdown();
    }
  });

  it("allows SELECT on readonly source", async () => {
    const readonlyManager = new ConnectionManager([readonlySource]);

    try {
      const pool = await readonlyManager.getPool("readonly-test");
      const result = await executeQuery(pool, "SELECT 1 as value", 10, readonlyOptions);

      expect(result.rows).toEqual([{ value: 1 }]);
      expect(result.rowCount).toBe(1);
    } finally {
      await readonlyManager.shutdown();
    }
  });
});
