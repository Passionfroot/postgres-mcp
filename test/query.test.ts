import { describe, expect, it, vi } from "vitest";

import {
  assertReadOnlyQuery,
  ensureLimit,
  executeQuery,
  formatPgError,
  ReadOnlyQueryError,
} from "../src/query.js";

describe("ensureLimit", () => {
  it("adds LIMIT to a simple SELECT", () => {
    const result = ensureLimit("SELECT * FROM users", 100, false);
    expect(result).toMatch(/LIMIT 100/i);
  });

  it("preserves existing LIMIT", () => {
    const result = ensureLimit("SELECT * FROM users LIMIT 5", 100, false);
    expect(result).toMatch(/LIMIT 5/i);
    expect(result).not.toMatch(/LIMIT 100/i);
  });

  it("handles SELECT with trailing semicolon", () => {
    const result = ensureLimit("SELECT * FROM users;", 100, false);
    expect(result).toMatch(/LIMIT 100/i);
  });

  it("rejects multi-statement SQL when not allowed", () => {
    expect(() => ensureLimit("SELECT 1; SELECT 2", 100, false)).toThrow(
      "Multi-statement queries are not allowed"
    );
  });

  it("passes multi-statement SQL through when allowed", () => {
    const sql = "SELECT 1; SELECT 2";
    expect(ensureLimit(sql, 100, true)).toBe(sql);
  });

  it("leaves non-SELECT statements unchanged", () => {
    const sql = "EXPLAIN ANALYZE SELECT * FROM users";
    expect(ensureLimit(sql, 100, false)).toBe(sql);
  });

  it("leaves INSERT ... RETURNING unchanged", () => {
    const sql = "INSERT INTO users (name) VALUES ('test') RETURNING id";
    expect(ensureLimit(sql, 100, false)).toBe(sql);
  });

  it("handles CTE (WITH) queries", () => {
    const sql =
      "WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active";
    const result = ensureLimit(sql, 100, false);
    expect(result).toMatch(/LIMIT 100/i);
  });

  it("applies regex fallback LIMIT on parse failure", () => {
    const sql = "THIS IS NOT VALID SQL %%%";
    const result = ensureLimit(sql, 100, false);
    expect(result).toBe("THIS IS NOT VALID SQL %%% LIMIT 100");
  });

  it("regex fallback strips trailing semicolon before appending LIMIT", () => {
    const sql = "SELECT * FROM users @> '{}'::jsonb;";
    const result = ensureLimit(sql, 100, false);
    expect(result).toBe("SELECT * FROM users @> '{}'::jsonb LIMIT 100");
  });

  it("regex fallback preserves existing LIMIT", () => {
    const sql = "SELECT * FROM users @> '{}'::jsonb LIMIT 5";
    const result = ensureLimit(sql, 100, false);
    expect(result).toBe(sql);
  });
});

describe("formatPgError", () => {
  it("formats error with all fields", () => {
    const result = formatPgError({
      code: "42P01",
      message: 'relation "users" does not exist',
      position: "15",
      detail: "Table does not exist in schema",
      hint: "Check the table name spelling",
    });

    expect(result).toBe(
      [
        'PostgreSQL error 42P01: relation "users" does not exist',
        "at position 15",
        "Detail: Table does not exist in schema",
        "Hint: Check the table name spelling",
      ].join("\n")
    );
  });

  it("formats error with only code and message", () => {
    const result = formatPgError({
      code: "42601",
      message: 'syntax error at or near "SELCT"',
    });

    expect(result).toBe(
      'PostgreSQL error 42601: syntax error at or near "SELCT"'
    );
  });

  it("includes position when present", () => {
    const result = formatPgError({
      code: "42601",
      message: "syntax error",
      position: "7",
    });

    expect(result).toContain("at position 7");
  });

  it("includes hint when present", () => {
    const result = formatPgError({
      code: "42703",
      message: 'column "foo" does not exist',
      hint: 'Perhaps you meant to reference the column "bar"',
    });

    expect(result).toContain("Hint:");
    expect(result).toContain('Perhaps you meant to reference the column "bar"');
  });

  it("omits missing optional fields", () => {
    const result = formatPgError({
      code: "42P01",
      message: "table not found",
    });

    expect(result).not.toContain("at position");
    expect(result).not.toContain("Detail:");
    expect(result).not.toContain("Hint:");
  });
});

describe("assertReadOnlyQuery", () => {
  it("allows a plain SELECT", () => {
    expect(() =>
      assertReadOnlyQuery("SELECT * FROM collaborations WHERE id = 1")
    ).not.toThrow();
  });

  it("allows a CTE that only reads", () => {
    expect(() =>
      assertReadOnlyQuery(
        "WITH c AS (SELECT id FROM collaborations) SELECT count(*) FROM c"
      )
    ).not.toThrow();
  });

  it("allows current_setting (a read)", () => {
    expect(() =>
      assertReadOnlyQuery("SELECT current_setting('app.partner_id', true)")
    ).not.toThrow();
  });

  it("blocks set_config()", () => {
    expect(() =>
      assertReadOnlyQuery("SELECT set_config('app.partner_id', 'other', false)")
    ).toThrow(ReadOnlyQueryError);
  });

  it("blocks set_config() hidden inside a CTE", () => {
    expect(() =>
      assertReadOnlyQuery(
        "WITH x AS (SELECT set_config('app.partner_id', 'other', false)) SELECT count(*) FROM collaborations, x"
      )
    ).toThrow(ReadOnlyQueryError);
  });

  it("blocks set_config() in a subquery", () => {
    expect(() =>
      assertReadOnlyQuery(
        "SELECT * FROM t WHERE id = (SELECT set_role('postgres'))"
      )
    ).toThrow(ReadOnlyQueryError);
  });

  it("blocks schema-qualified pg_catalog.set_config()", () => {
    expect(() =>
      assertReadOnlyQuery(
        "SELECT pg_catalog.set_config('role', 'postgres', false)"
      )
    ).toThrow(ReadOnlyQueryError);
  });

  it("blocks the SET command", () => {
    expect(() => assertReadOnlyQuery("SET app.partner_id = 'other'")).toThrow(
      ReadOnlyQueryError
    );
  });

  it("blocks SET ROLE (which the parser cannot parse)", () => {
    expect(() => assertReadOnlyQuery("SET ROLE zest_mcp_reader")).toThrow(
      ReadOnlyQueryError
    );
  });

  it("blocks RESET ROLE", () => {
    expect(() => assertReadOnlyQuery("RESET ROLE")).toThrow(ReadOnlyQueryError);
  });

  it("blocks RESET of a GUC", () => {
    expect(() => assertReadOnlyQuery("RESET app.partner_id")).toThrow(
      ReadOnlyQueryError
    );
  });

  it("fails closed on unparseable SQL", () => {
    expect(() => assertReadOnlyQuery("SELECT FROM WHERE ((")).toThrow();
  });

  // The guard is an allowlist (only SELECT passes), not a denylist of a few
  // commands. These prove the broader surface stays blocked so nobody can
  // weaken the fail-closed behaviour without a test going red.
  it.each([
    ["UPDATE", "UPDATE collaborations SET name = 'x'"],
    ["INSERT", "INSERT INTO collaborations (id) VALUES ('x')"],
    ["DELETE", "DELETE FROM collaborations"],
    ["SET SESSION AUTHORIZATION", "SET SESSION AUTHORIZATION postgres"],
    ["SET TRANSACTION READ WRITE", "SET TRANSACTION READ WRITE"],
    ["SET LOCAL", "SET LOCAL app.partner_id = 'x'"],
    ["RESET ALL", "RESET ALL"],
    ["COPY TO PROGRAM", "COPY collaborations TO PROGRAM 'curl evil'"],
    ["DO block", "DO $$ BEGIN PERFORM 1; END $$"],
    ["CALL", "CALL some_proc()"],
    ["BEGIN", "BEGIN"],
    ["dblink", "SELECT * FROM dblink('host=x', 'select 1') AS t(a int)"],
  ])("rejects %s", (_label, sql) => {
    expect(() => assertReadOnlyQuery(sql)).toThrow();
  });

  // Dangerous functions hidden inside an otherwise-valid SELECT.
  it.each([
    ["pg_read_file", "SELECT pg_read_file('/etc/passwd')"],
    ["pg_ls_dir", "SELECT pg_ls_dir('/')"],
    ["lo_export", "SELECT lo_export(1, '/tmp/x')"],
  ])("blocks %s in a SELECT", (_label, sql) => {
    expect(() => assertReadOnlyQuery(sql)).toThrow(ReadOnlyQueryError);
  });

  // Writes that parse as a top-level `select` (data-modifying CTE, SELECT INTO).
  it.each([
    [
      "UPDATE CTE",
      "WITH x AS (UPDATE t SET a = 1 RETURNING id) SELECT * FROM x",
    ],
    [
      "INSERT CTE",
      "WITH x AS (INSERT INTO t(a) VALUES (1) RETURNING id) SELECT * FROM x",
    ],
    ["DELETE CTE", "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x"],
    [
      "write CTE among reads",
      "WITH a AS (SELECT 1), b AS (UPDATE t SET x = 1 RETURNING id) SELECT * FROM a, b",
    ],
    ["SELECT INTO", "SELECT * INTO newtab FROM t"],
  ])("blocks %s (write hidden in a SELECT)", (_label, sql) => {
    // Blocked either by the write-statement walk or, for statements the parser can't
    // parse (e.g. a DELETE CTE), by the fail-closed parse path.
    expect(() => assertReadOnlyQuery(sql)).toThrow();
  });

  it("allows a read-only CTE with multiple SELECT clauses", () => {
    expect(() =>
      assertReadOnlyQuery(
        "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b"
      )
    ).not.toThrow();
  });
});

describe("executeQuery", () => {
  function createMockPool(queryFn: ReturnType<typeof vi.fn>) {
    const client = {
      query: queryFn,
      release: vi.fn(),
    };
    return {
      connect: vi.fn().mockResolvedValue(client),
      _client: client,
    } as unknown as import("pg").Pool & {
      _client: {
        query: ReturnType<typeof vi.fn>;
        release: ReturnType<typeof vi.fn>;
      };
    };
  }

  const defaultOptions = { readonly: false, allowMultiStatements: false };

  it("rejects a session-mutating query before connecting when readOnlyQueries is on", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [] });
    const pool = createMockPool(queryFn);

    await expect(
      executeQuery(
        pool,
        "SELECT set_config('app.partner_id', 'other', false)",
        100,
        {
          ...defaultOptions,
          readOnlyQueries: true,
        }
      )
    ).rejects.toThrow(ReadOnlyQueryError);

    expect(pool.connect).not.toHaveBeenCalled();
  });

  it("allows a session-mutating query when readOnlyQueries is off", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [] });
    const pool = createMockPool(queryFn);

    await executeQuery(
      pool,
      "SELECT set_config('app.partner_id', 'other', false)",
      100,
      {
        ...defaultOptions,
        readOnlyQueries: false,
      }
    );

    expect(pool.connect).toHaveBeenCalled();
  });

  it("adds LIMIT to queries without one", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(pool, "SELECT * FROM users", 100, defaultOptions);

    const calledSql = queryFn.mock.calls[0][0] as string;
    expect(calledSql).toMatch(/LIMIT 101/i);
  });

  it("preserves existing LIMIT in queries", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(
      pool,
      "SELECT * FROM users LIMIT 5",
      100,
      defaultOptions
    );

    const calledSql = queryFn.mock.calls[0][0] as string;
    expect(calledSql).toMatch(/LIMIT 5/i);
    expect(calledSql).not.toMatch(/LIMIT 101/i);
  });

  it("detects truncation when rows exceed maxRows", async () => {
    const rows = Array.from({ length: 11 }, (_, i) => ({ id: i + 1 }));
    const queryFn = vi.fn().mockResolvedValue({ rows });
    const pool = createMockPool(queryFn);

    const result = await executeQuery(
      pool,
      "SELECT * FROM users",
      10,
      defaultOptions
    );

    expect(result.truncated).toBe(true);
    expect(result.rowCount).toBe(10);
    expect(result.rows).toHaveLength(10);
  });

  it("returns truncated=false when under limit", async () => {
    const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
    const queryFn = vi.fn().mockResolvedValue({ rows });
    const pool = createMockPool(queryFn);

    const result = await executeQuery(
      pool,
      "SELECT * FROM users",
      10,
      defaultOptions
    );

    expect(result.truncated).toBe(false);
    expect(result.rowCount).toBe(3);
    expect(result.rows).toHaveLength(3);
  });

  it("sets readonly session before executing query", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(pool, "SELECT 1", 10, {
      readonly: true,
      allowMultiStatements: false,
    });

    expect(queryFn).toHaveBeenCalledTimes(2);
    expect(queryFn.mock.calls[0][0]).toBe(
      "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"
    );
    expect(queryFn.mock.calls[1][0]).toMatch(/SELECT/);
  });

  it("does not set readonly session when readonly is false", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(pool, "SELECT 1", 10, defaultOptions);

    expect(queryFn).toHaveBeenCalledTimes(1);
  });

  it("releases client after successful query", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [] });
    const pool = createMockPool(queryFn);

    await executeQuery(pool, "SELECT 1", 10, defaultOptions);

    expect(
      (pool as unknown as { _client: { release: ReturnType<typeof vi.fn> } })
        ._client.release
    ).toHaveBeenCalledTimes(1);
  });

  it("releases client after failed query", async () => {
    const pgError = Object.assign(new Error('relation "xyz" does not exist'), {
      code: "42P01",
      position: "15",
    });
    const queryFn = vi.fn().mockRejectedValue(pgError);
    const pool = createMockPool(queryFn);

    await expect(
      executeQuery(pool, "SELECT * FROM xyz", 100, defaultOptions)
    ).rejects.toThrow();

    expect(
      (pool as unknown as { _client: { release: ReturnType<typeof vi.fn> } })
        ._client.release
    ).toHaveBeenCalledTimes(1);
  });

  it("formats PG errors with code/message/position", async () => {
    const pgError = Object.assign(new Error('relation "xyz" does not exist'), {
      code: "42P01",
      position: "15",
    });
    const queryFn = vi.fn().mockRejectedValue(pgError);
    const pool = createMockPool(queryFn);

    await expect(
      executeQuery(pool, "SELECT * FROM xyz", 100, defaultOptions)
    ).rejects.toThrow('PostgreSQL error 42P01: relation "xyz" does not exist');
  });

  it("sets role before executing query and resets after", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(pool, "SELECT 1", 10, {
      ...defaultOptions,
      role: "app_mcp_readonly",
    });

    // SET ROLE, query, RESET ROLE
    expect(queryFn.mock.calls[0][0]).toBe('SET ROLE "app_mcp_readonly"');
    expect(queryFn.mock.calls[1][0]).toMatch(/SELECT/);
    // Cleanup calls happen in finally
    expect(queryFn.mock.calls[2][0]).toBe("RESET ROLE");
  });

  it("sets session vars before executing query and resets after", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(pool, "SELECT 1", 10, {
      ...defaultOptions,
      sessionVars: { "app.current_tenant_id": "tenant_123" },
    });

    expect(queryFn.mock.calls[0][0]).toBe(
      "SET app.current_tenant_id = 'tenant_123'"
    );
    expect(queryFn.mock.calls[1][0]).toMatch(/SELECT/);
    expect(queryFn.mock.calls[2][0]).toBe("RESET app.current_tenant_id");
  });

  it("sets role and session vars together with readonly", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(pool, "SELECT 1", 10, {
      readonly: true,
      allowMultiStatements: false,
      role: "mcp_reader",
      sessionVars: { "app.tenant_id": "t_1" },
    });

    // Order: SET ROLE, SET session var, SET readonly, query
    expect(queryFn.mock.calls[0][0]).toBe('SET ROLE "mcp_reader"');
    expect(queryFn.mock.calls[1][0]).toBe("SET app.tenant_id = 't_1'");
    expect(queryFn.mock.calls[2][0]).toBe(
      "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"
    );
    expect(queryFn.mock.calls[3][0]).toMatch(/SELECT/);
    // Cleanup: RESET session var, RESET ROLE
    expect(queryFn.mock.calls[4][0]).toBe("RESET app.tenant_id");
    expect(queryFn.mock.calls[5][0]).toBe("RESET ROLE");
  });

  it("identifies timeout errors with actionable message", async () => {
    const timeoutError = Object.assign(
      new Error("canceling statement due to statement timeout"),
      {
        code: "57014",
      }
    );
    const queryFn = vi.fn().mockRejectedValue(timeoutError);
    const pool = createMockPool(queryFn);

    await expect(
      executeQuery(pool, "SELECT pg_sleep(999)", 100, defaultOptions)
    ).rejects.toThrow(
      "Query timed out. Simplify the query or add more specific WHERE conditions."
    );
  });

  it("provides actionable hint on column-level permission denied (42501)", async () => {
    const permError = Object.assign(
      new Error('permission denied for column "vatId" of relation "creators"'),
      { code: "42501" }
    );
    const queryFn = vi.fn().mockRejectedValue(permError);
    const pool = createMockPool(queryFn);

    await expect(
      executeQuery(pool, "SELECT * FROM creators", 100, defaultOptions)
    ).rejects.toThrow("search_objects");
  });
});
