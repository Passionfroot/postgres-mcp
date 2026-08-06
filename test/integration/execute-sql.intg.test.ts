import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { SourceConfig } from "../../src/types.js";

import { ConnectionManager } from "../../src/connections.js";
import { executeQuery } from "../../src/query.js";
import { resolveTestDb, TEST_DSN as TEST_DSN_ENV } from "./test-db.js";

const TEST_DSN = TEST_DSN_ENV ?? "";

const localSource: SourceConfig = {
  id: "local",
  dsn: TEST_DSN,
  readonly: false,
  maxRows: 10,
  timeout: 5,
  poolMax: 1,
  poolMaxExplicit: false,
  maxResponseBytes: 1_000_000,
  allowMultiStatements: false,
  readOnlyQueries: false,
};

const defaultOptions = { readonly: false, allowMultiStatements: false };

const { isAvailable: isDbAvailable } = await resolveTestDb();

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
    const result = await executeQuery(
      pool,
      "SELECT 1 as value",
      10,
      defaultOptions
    );

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

  it("returns tz-naive timestamps and dates as literal strings", async () => {
    const pool = await connectionManager.getPool("local");
    const result = await executeQuery(
      pool,
      `SELECT
        '2026-07-02 11:43:00.875'::timestamp as naive_ts,
        '2026-07-02'::date as naive_date,
        ARRAY['2026-07-02 11:43:00.875'::timestamp] as naive_ts_array`,
      10,
      defaultOptions
    );

    expect(result.rows[0]).toEqual({
      naive_ts: "2026-07-02 11:43:00.875",
      naive_date: "2026-07-02",
      naive_ts_array: ["2026-07-02 11:43:00.875"],
    });
  });

  it("returns empty result", async () => {
    const pool = await connectionManager.getPool("local");
    const result = await executeQuery(
      pool,
      "SELECT 1 WHERE false",
      10,
      defaultOptions
    );

    expect(result.rows).toEqual([]);
    expect(result.rowCount).toBe(0);
    expect(result.truncated).toBe(false);
  });

  it("surfaces PG errors with code", async () => {
    const pool = await connectionManager.getPool("local");

    await expect(
      executeQuery(
        pool,
        "SELECT * FROM nonexistent_table_xyz_123",
        10,
        defaultOptions
      )
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
      poolMaxExplicit: false,
      maxResponseBytes: 1_000_000,
      allowMultiStatements: false,
      readOnlyQueries: false,
    };
    const timeoutManager = new ConnectionManager([timeoutSource]);

    try {
      const pool = await timeoutManager.getPool("timeout-test");
      await expect(
        executeQuery(pool, "SELECT pg_sleep(10)", 10, defaultOptions)
      ).rejects.toThrow("timed out");
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
    poolMaxExplicit: false,
    maxResponseBytes: 1_000_000,
    allowMultiStatements: false,
    readOnlyQueries: false,
  };

  const readonlyOptions = { readonly: true, allowMultiStatements: false };

  it("rejects DDL on readonly source", async () => {
    const readonlyManager = new ConnectionManager([readonlySource]);

    try {
      const pool = await readonlyManager.getPool("readonly-test");
      await expect(
        executeQuery(
          pool,
          "CREATE TEMP TABLE test_readonly_check (id int)",
          10,
          readonlyOptions
        )
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
      const result = await executeQuery(
        pool,
        "SELECT 1 as value",
        10,
        readonlyOptions
      );

      expect(result.rows).toEqual([{ value: 1 }]);
      expect(result.rowCount).toBe(1);
    } finally {
      await readonlyManager.shutdown();
    }
  });
});

describe.skipIf(!isDbAvailable)(
  "multi-statement batches against a real connection",
  () => {
    const multiStatementSource: SourceConfig = {
      id: "multi-statement-test",
      dsn: TEST_DSN,
      readonly: false,
      maxRows: 10,
      timeout: 5,
      poolMax: 1,
      poolMaxExplicit: false,
      maxResponseBytes: 1_000_000,
      allowMultiStatements: true,
      readOnlyQueries: false,
    };
    const multiStatementOptions = {
      readonly: false,
      allowMultiStatements: true,
      readOnlyQueries: false,
    };

    // Direct connection, used to set up the probes and to read back what the batches did.
    let probePool: pg.Pool;

    beforeAll(async () => {
      probePool = new pg.Pool({ connectionString: TEST_DSN, max: 1 });
      await probePool.query("DROP TABLE IF EXISTS batch_writes");
      await probePool.query("DROP TABLE IF EXISTS batch_touched");
      await probePool.query("DROP TABLE IF EXISTS batch_src");
      await probePool.query("CREATE TABLE batch_writes (n int)");
      await probePool.query("CREATE TABLE batch_touched (n int)");
      await probePool.query("CREATE TABLE batch_src (n int)");
      await probePool.query(
        "INSERT INTO batch_src SELECT generate_series(1, 100)"
      );
      // Volatile, so the server evaluates it once per row it actually produces. Counting the
      // rows in batch_touched is how the tests below see how far the server got.
      await probePool.query(`
      CREATE OR REPLACE FUNCTION batch_touch(v int) RETURNS int
      LANGUAGE sql VOLATILE AS $$
        INSERT INTO batch_touched VALUES (v);
        SELECT v;
      $$
    `);
    });

    afterAll(async () => {
      if (!probePool) return;
      await probePool.query("DROP FUNCTION IF EXISTS batch_touch(int)");
      await probePool.query("DROP TABLE IF EXISTS batch_writes");
      await probePool.query("DROP TABLE IF EXISTS batch_touched");
      await probePool.query("DROP TABLE IF EXISTS batch_src");
      await probePool.end();
    });

    async function countRows(table: string) {
      const result = await probePool.query(
        `SELECT count(*)::int AS c FROM ${table}`
      );
      return result.rows[0].c as number;
    }

    it("executes a genuine two-statement batch and returns the final statement's rows", async () => {
      const manager = new ConnectionManager([multiStatementSource]);

      try {
        const pool = await manager.getPool("multi-statement-test");
        // A real batch sent to the wire as one string containing two statements — this is what
        // node-postgres returns an ARRAY of QueryResults for, which previously threw a TypeError.
        const result = await executeQuery(
          pool,
          "SET statement_timeout = '5000'; SELECT generate_series(1, 3) as n",
          10,
          multiStatementOptions
        );

        expect(result.rows).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
        expect(result.rowCount).toBe(3);
        expect(result.truncated).toBe(false);
      } finally {
        await manager.shutdown();
      }
    });

    it("returns the SELECT from a BEGIN; SELECT; COMMIT batch, not the COMMIT", async () => {
      const manager = new ConnectionManager([multiStatementSource]);

      try {
        const pool = await manager.getPool("multi-statement-test");
        const result = await executeQuery(
          pool,
          "BEGIN; SELECT 7 as n; COMMIT",
          10,
          multiStatementOptions
        );

        expect(result.rows).toEqual([{ n: 7 }]);
        expect(result.rowCount).toBe(1);
      } finally {
        await manager.shutdown();
      }
    });

    it("returns the SELECT from a SHOW; SELECT batch", async () => {
      const manager = new ConnectionManager([multiStatementSource]);

      try {
        const pool = await manager.getPool("multi-statement-test");
        // SHOW produces a row of its own. Keying the ambiguity check on rows rejected this
        // batch outright; SHOW reports session state, so it does not compete for the answer.
        const result = await executeQuery(
          pool,
          "SHOW statement_timeout; SELECT 8 as n",
          10,
          multiStatementOptions
        );

        expect(result.rows).toEqual([{ n: 8 }]);
      } finally {
        await manager.shutdown();
      }
    });

    it("returns the zero-row SELECT, not the SET before it", async () => {
      const manager = new ConnectionManager([multiStatementSource]);

      try {
        const pool = await manager.getPool("multi-statement-test");
        // The SELECT returns no rows but does return fields; the SET returns neither. Both
        // look empty if you only count rows.
        const result = await executeQuery(
          pool,
          "SET statement_timeout = '5000'; SELECT n FROM batch_src WHERE false",
          10,
          multiStatementOptions
        );

        expect(result.rows).toEqual([]);
        expect(result.rowCount).toBe(0);
        expect(result.truncated).toBe(false);
      } finally {
        await manager.shutdown();
      }
    });

    it("caps rows at maxRows for the final statement in a batch", async () => {
      const manager = new ConnectionManager([multiStatementSource]);

      try {
        const pool = await manager.getPool("multi-statement-test");
        const result = await executeQuery(
          pool,
          "SET statement_timeout = '5000'; SELECT generate_series(1, 100) as n",
          10,
          multiStatementOptions
        );

        expect(result.rowCount).toBe(10);
        expect(result.truncated).toBe(true);
        expect(result.rows).toHaveLength(10);
      } finally {
        await manager.shutdown();
      }
    });

    it("stops the server at max_rows instead of slicing a full result set locally", async () => {
      const manager = new ConnectionManager([multiStatementSource]);

      try {
        await probePool.query("DELETE FROM batch_touched");
        const pool = await manager.getPool("multi-statement-test");
        const result = await executeQuery(
          pool,
          "SET statement_timeout = '5000'; SELECT batch_touch(n) as n FROM batch_src",
          10,
          multiStatementOptions
        );

        expect(result.rowCount).toBe(10);
        expect(result.truncated).toBe(true);
        // maxRows + 1 rows produced on the server, not all 100 in batch_src.
        expect(await countRows("batch_touched")).toBe(11);
      } finally {
        await manager.shutdown();
      }
    });

    it("rejects a batch with two row-returning statements", async () => {
      const manager = new ConnectionManager([multiStatementSource]);

      try {
        const pool = await manager.getPool("multi-statement-test");

        await expect(
          executeQuery(
            pool,
            "SELECT 1 as n; SELECT 2 as n",
            10,
            multiStatementOptions
          )
        ).rejects.toThrow("more than one statement that returns rows");
      } finally {
        await manager.shutdown();
      }
    });

    it("rejects an ambiguous write batch before any of it runs", async () => {
      const manager = new ConnectionManager([multiStatementSource]);

      try {
        await probePool.query("DELETE FROM batch_writes");
        const pool = await manager.getPool("multi-statement-test");

        await expect(
          executeQuery(
            pool,
            "INSERT INTO batch_writes (n) VALUES (1) RETURNING n; SELECT n FROM batch_writes",
            10,
            multiStatementOptions
          )
        ).rejects.toThrow("more than one statement that returns rows");

        // The point of deciding this from the AST: told to resend, the caller would otherwise
        // insert a second row, because the first attempt had already committed.
        expect(await countRows("batch_writes")).toBe(0);
      } finally {
        await manager.shutdown();
      }
    });

    it("still rejects a multi-statement batch on a read-only-guarded source (guard not weakened)", async () => {
      // readonly + allowMultiStatements: false is the "guarded" combination — this must keep
      // rejecting multi-statement input outright, never reaching the array-handling code above.
      const guardedSource: SourceConfig = {
        id: "guarded-test",
        dsn: TEST_DSN,
        readonly: true,
        maxRows: 10,
        timeout: 5,
        poolMax: 1,
        poolMaxExplicit: false,
        maxResponseBytes: 1_000_000,
        allowMultiStatements: false,
        readOnlyQueries: false,
      };
      const manager = new ConnectionManager([guardedSource]);

      try {
        const pool = await manager.getPool("guarded-test");

        await expect(
          executeQuery(pool, "SELECT 1 as n; SELECT 2 as n", 10, {
            readonly: true,
            allowMultiStatements: false,
          })
        ).rejects.toThrow("Multi-statement queries are not allowed");
      } finally {
        await manager.shutdown();
      }
    });
  }
);

describe.skipIf(!isDbAvailable)(
  "smuggled batch on a source that did not allow one",
  () => {
    // node-sql-parser lexes \' MySQL-style and keeps consuming the literal; PostgreSQL with
    // standard_conforming_strings=on closes it at the quote and runs what follows as separate
    // statements. ensureLimit therefore sees one SELECT while the server runs three.
    const payload =
      "SELECT 'x\\' FROM smuggle_t WHERE 1=0; SET statement_timeout TO '9000'; " +
      "SELECT * FROM smuggle_t; --'";

    let pool: pg.Pool;

    beforeAll(async () => {
      pool = new pg.Pool({ connectionString: TEST_DSN, max: 1 });
      await pool.query("DROP TABLE IF EXISTS smuggle_t");
      await pool.query("CREATE TABLE smuggle_t (id int, tenant text)");
      await pool.query("INSERT INTO smuggle_t VALUES (1, 'a'), (2, 'b')");
    });

    afterAll(async () => {
      if (!pool) return;
      await pool.query("DROP TABLE IF EXISTS smuggle_t");
      await pool.end();
    });

    it("returns an array of three results at the driver level (the shape being defended against)", async () => {
      const raw = await pool.query(payload);
      const results = Array.isArray(raw) ? raw : [raw];

      expect(results).toHaveLength(3);
      expect(results.map((r) => r.command)).toEqual([
        "SELECT",
        "SET",
        "SELECT",
      ]);
      // Every earlier result is empty, so an "did an earlier statement return rows?" check
      // passes the smuggled SELECT straight through.
      expect(results[0].rows).toHaveLength(0);
      expect(results[2].rows).toHaveLength(2);
    });

    it("rejects it instead of returning the smuggled statement's rows", async () => {
      await expect(
        executeQuery(pool, payload, 10, {
          readonly: false,
          allowMultiStatements: false,
          readOnlyQueries: false,
        })
      ).rejects.toThrow("Multi-statement queries are not allowed");
    });

    it("never runs it at all on a read-only source", async () => {
      await expect(
        executeQuery(pool, payload, 10, {
          readonly: false,
          allowMultiStatements: false,
          readOnlyQueries: true,
        })
      ).rejects.toThrow(/backslash immediately before its closing quote/);
    });
  }
);
