import { describe, expect, it, vi } from "vitest";

import { logger } from "../src/logger.js";
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

  it("rejects a batch with two row-returning statements when multi-statement is allowed", () => {
    // Decided from the AST, so it fails before the batch reaches the database.
    expect(() => ensureLimit("SELECT 1; SELECT 2", 100, true)).toThrow(
      "more than one statement that returns rows"
    );
  });

  it("pushes the LIMIT onto the batch's SELECT instead of leaving it unbounded", () => {
    const result = ensureLimit(
      "SET statement_timeout = '5000'; SELECT n FROM t",
      100,
      true
    );
    expect(result).toMatch(/SET statement_timeout = '5000'/i);
    expect(result).toMatch(/LIMIT 100/i);
  });

  it("pushes the LIMIT onto the SELECT inside a transaction batch", () => {
    const result = ensureLimit("BEGIN; SELECT n FROM t; COMMIT", 100, true);
    expect(result).toMatch(/SELECT n FROM "?t"? LIMIT 100/i);
    expect(result).toMatch(/COMMIT/i);
  });

  it("preserves an existing LIMIT on the batch's SELECT", () => {
    const sql = "SET statement_timeout = '5000'; SELECT n FROM t LIMIT 5";
    expect(ensureLimit(sql, 100, true)).toBe(sql);
  });

  it("leaves a batch with no row-returning statement alone", () => {
    const sql = "SET a.b = '1'; SET c.d = '2'";
    expect(ensureLimit(sql, 100, true)).toBe(sql);
  });

  it("does not try to put a LIMIT on SHOW", () => {
    const sql = "SET statement_timeout = '5000'; SHOW statement_timeout";
    expect(ensureLimit(sql, 100, true)).toBe(sql);
  });

  it("passes an unparseable multi-statement batch through the regex fallback", () => {
    // astify cannot read `TABLE t`, so there is no AST to push a LIMIT onto.
    const sql = "SET statement_timeout = '5000'; TABLE t";
    expect(ensureLimit(sql, 100, true)).toBe(
      "SET statement_timeout = '5000'; TABLE t LIMIT 100"
    );
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

  // Statement-boundary bypass. node-sql-parser reads \' as an escaped quote and keeps
  // consuming, so it sees one clean SELECT; PostgreSQL with
  // standard_conforming_strings=on closes the literal at that quote and executes what
  // follows the `;` as further statements. Both of these were proven to re-point tenant
  // scope on a PostgreSQL 16 FORCE ROW LEVEL SECURITY fixture while passing the guard.
  it.each([
    [
      "SET ROLE after a backslash-terminated literal",
      String.raw`SELECT 'x\'; SET ROLE postgres; SELECT * FROM secrets; --'`,
    ],
    [
      "SET of the tenant GUC after a backslash-terminated literal",
      String.raw`SELECT 'x\'; SET app.partner_id TO "tenantB"; SELECT * FROM secrets; --'`,
    ],
    [
      "plain second statement after a backslash-terminated literal",
      String.raw`SELECT 'x\'; SELECT 424242 AS injected; --'`,
    ],
    [
      "odd backslash run of three",
      String.raw`SELECT 'x\\\'; SELECT 424242 AS injected; --'`,
    ],
  ])("rejects %s", (_label, sql) => {
    expect(() => assertReadOnlyQuery(sql)).toThrow(ReadOnlyQueryError);
  });

  it("names the backslash-before-quote reason", () => {
    let reason = "";
    try {
      assertReadOnlyQuery(String.raw`SELECT 'x\'; SELECT 1; --'`);
    } catch (err) {
      reason = (err as ReadOnlyQueryError).reason;
    }
    expect(reason).toContain("backslash immediately before its closing quote");
  });

  // The same divergence in a "..." quoted identifier. node-sql-parser applies MySQL-style
  // backslash escaping there too and reads \" as an escaped quote, so it sees one column
  // with a long name; PostgreSQL closes the identifier at that quote and treats what
  // follows the `;` as further statements. A grammar sweep of 5632 inputs against
  // PostgreSQL 16 found 1008 of these, all of them an odd run of backslashes before the
  // closing quote and none of them an even run. These are a spread of the shapes it found:
  // different surrounding clauses, different smuggled statements, different trailers.
  it.each([
    [
      "bare select item",
      String.raw`SELECT "x\"; SET app.partner_id TO 'tenantB' --"`,
    ],
    [
      "select item with FROM",
      String.raw`SELECT "x\" FROM t; INSERT INTO sideeffect VALUES (1) --"`,
    ],
    [
      "select item with WHERE",
      String.raw`SELECT "x\" WHERE 1=1; DROP TABLE sideeffect --"`,
    ],
    [
      "select item with ORDER BY",
      String.raw`SELECT "x\" ORDER BY 1; SELECT 424242 AS injected --"`,
    ],
    ["qualified column", String.raw`SELECT t."x\" FROM t; RESET ROLE --"`],
    [
      "second of two select items",
      String.raw`SELECT 3, "x\"; UPDATE sideeffect SET n = 2 --"`,
    ],
    [
      "inside a CTE",
      String.raw`WITH c AS (SELECT "x\"; CREATE TABLE zz (i int) --") SELECT * FROM c`,
    ],
    [
      "odd backslash run of three",
      String.raw`SELECT "x\\\"; SELECT 424242 AS injected --"`,
    ],
    [
      "empty identifier body",
      String.raw`SELECT "\"; SET app.partner_id TO 'tenantB' --"`,
    ],
    [
      "block-comment trailer",
      String.raw`SELECT "x\"; SELECT 424242 AS injected /*"*/`,
    ],
  ])("rejects an identifier bypass: %s", (_label, sql) => {
    expect(() => assertReadOnlyQuery(sql)).toThrow(ReadOnlyQueryError);
  });

  it("names the quoted-identifier reason separately from the literal one", () => {
    let reason = "";
    try {
      assertReadOnlyQuery(String.raw`SELECT "x\"; SELECT 1 --"`);
    } catch (err) {
      reason = (err as ReadOnlyQueryError).reason;
    }
    expect(reason).toContain("a quoted identifier");
    expect(reason).toContain("backslash immediately before its closing quote");
  });

  // Even runs and mid-identifier backslashes produce the same statement boundary in both
  // lexers, so tightening past the odd-run rule would only add false positives.
  it.each([
    ["backslash mid-identifier", String.raw`SELECT 1 AS "a\b"`],
    [
      "even backslash run before the closing quote",
      String.raw`SELECT 1 AS "a\\"`,
    ],
    ["Windows path in an identifier", String.raw`SELECT "C:\temp\dir" FROM t`],
  ])("allows %s", (_label, sql) => {
    expect(() => assertReadOnlyQuery(sql)).not.toThrow();
  });

  // A backslash immediately before a DOUBLED quote is rejected even though PostgreSQL
  // and node-sql-parser cannot be made to disagree in the dangerous direction there:
  // PostgreSQL reads "" as one literal quote and keeps consuming, so it always ends the
  // token no earlier than the parser does. It is rejected anyway because the '...' branch
  // has ordered the checks this way since the guard shipped, and the two branches are
  // easier to keep correct while they stay identical. Pinned so the conservatism is a
  // decision rather than an accident.
  it.each([
    ["quoted identifier", String.raw`SELECT 1 AS "a\""b"`],
    ["string literal", String.raw`SELECT 'a\''b'`],
  ])(
    "conservatively rejects a backslash before a doubled quote in a %s",
    (_label, sql) => {
      expect(() => assertReadOnlyQuery(sql)).toThrow(ReadOnlyQueryError);
    }
  );

  // Constructs verified against PostgreSQL 16 to produce identical statement
  // boundaries in node-sql-parser and the server. They must stay allowed so nobody
  // "hardens" them later and adds false positives that close nothing.
  it.each([
    ["backslash mid-literal (regex)", String.raw`SELECT 'a' ~ '\d+'`],
    ["Windows path literal", String.raw`SELECT 'C:\temp\file' AS p`],
    ["E-string with escapes", String.raw`SELECT E'tab\there'`],
    [
      "E-string ending in a backslash escape",
      String.raw`SELECT E'x\'; SELECT 1; --'`,
    ],
    ["dollar-quoted string", `SELECT $$a'; SELECT 1; --$$`],
    ["tagged dollar-quoted string", `SELECT $t$a'; SELECT 1; --$t$`],
    ["doubled quote escape", `SELECT 'x''; SELECT 1; --'`],
    ["backslash-quote inside a line comment", "SELECT 1 -- a\\'; SELECT 2\n"],
    [
      "backslash-quote inside a block comment",
      String.raw`SELECT 1 /* a\'; SELECT 2; */`,
    ],
    [
      "backslash-quote inside a quoted identifier",
      String.raw`SELECT 1 AS "a\'; SELECT 2; --"`,
    ],
    [
      "typed literal after an identifier ending in e",
      `SELECT date'2026-01-01'`,
    ],
  ])("allows %s", (_label, sql) => {
    expect(() => assertReadOnlyQuery(sql)).not.toThrow();
  });

  // Multiple statements that node-sql-parser does see are rejected regardless of the
  // source's allow_multi_statements setting.
  it("rejects multiple statements the parser can see", () => {
    expect(() => assertReadOnlyQuery("SELECT 1; SELECT 2")).toThrow(
      ReadOnlyQueryError
    );
  });

  // The set_config payloads proven to leak on a two-tenant RLS fixture. The third is
  // plan-dependent (it leaked on one fixture, not another); it must be rejected either
  // way.
  it.each([
    [
      "WHERE",
      "SELECT * FROM t WHERE set_config('app.partner_id','tenantB',false) IS NOT NULL",
    ],
    [
      "materialized CTE",
      "WITH x AS MATERIALIZED (SELECT set_config('app.partner_id','tenantB',false)) SELECT * FROM t CROSS JOIN x",
    ],
    [
      "select list",
      "SELECT set_config('app.partner_id','tenantB',false), t.* FROM t",
    ],
  ])("rejects set_config in the %s", (_label, sql) => {
    expect(() => assertReadOnlyQuery(sql)).toThrow(ReadOnlyQueryError);
  });

  describe("EXPLAIN", () => {
    it.each([
      ["bare", "EXPLAIN SELECT * FROM collaborations"],
      [
        "with options",
        "EXPLAIN (COSTS OFF, FORMAT JSON) SELECT * FROM collaborations",
      ],
      ["VERBOSE", "EXPLAIN VERBOSE SELECT * FROM collaborations"],
    ])("allows EXPLAIN %s", (_label, sql) => {
      expect(() => assertReadOnlyQuery(sql)).not.toThrow();
    });

    // ANALYZE actually runs the statement it explains.
    it.each([
      ["legacy syntax", "EXPLAIN ANALYZE SELECT * FROM collaborations"],
      [
        "option syntax",
        "EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM collaborations",
      ],
      ["British spelling", "EXPLAIN ANALYSE SELECT * FROM collaborations"],
    ])("rejects EXPLAIN ANALYZE (%s)", (_label, sql) => {
      expect(() => assertReadOnlyQuery(sql)).toThrow(ReadOnlyQueryError);
    });

    it("still guards the statement behind the EXPLAIN", () => {
      expect(() =>
        assertReadOnlyQuery("EXPLAIN INSERT INTO t (a) VALUES (1)")
      ).toThrow(ReadOnlyQueryError);
      expect(() =>
        assertReadOnlyQuery("EXPLAIN SELECT set_config('a', 'b', false)")
      ).toThrow(ReadOnlyQueryError);
    });
  });

  describe("rejection reasons", () => {
    it.each([
      [
        "the text 'set_config('",
        "SELECT id FROM logs WHERE msg LIKE '%set_config(%'",
        "set_config(",
      ],
      [
        "the text 'set_role('",
        "SELECT id FROM audit WHERE sql ILIKE '%set_role(%'",
        "set_role(",
      ],
      [
        "a leading SET",
        "SET app.partner_id = 'other'",
        "starts with SET or RESET",
      ],
      // Known false positive: `set` is a reserved word to the guard's parser, so this
      // plain SELECT fails closed. The message has to say why rather than telling the
      // caller to "rewrite it as a plain SELECT".
      [
        "why an unparseable plain SELECT was rejected",
        "SELECT count(*) AS set FROM channels",
        "reserved word used as a bare alias",
      ],
      [
        "the statement type",
        "UPDATE collaborations SET name = 'x'",
        "not SELECT",
      ],
      [
        "a nested write",
        "WITH x AS (INSERT INTO t(a) VALUES (1) RETURNING id) SELECT * FROM x",
        "write statement is nested",
      ],
      [
        "the offending function",
        "SELECT pg_read_file('/etc/passwd')",
        "pg_read_file()",
      ],
    ])("names %s", (_label, sql, expected) => {
      let message = "";
      try {
        assertReadOnlyQuery(sql);
      } catch (err) {
        message = (err as Error).message;
      }
      expect(message).toContain(expected);
    });
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

  // The extended protocol is the server-side half of the multi-statement defence: a Parse
  // carrying more than one command is refused with 42601 before anything executes. It is
  // gated on readOnlyQueries because allow_multi_statements is an independent setting.
  it("sends read-only-source queries over the extended protocol", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(pool, "SELECT * FROM users", 100, {
      ...defaultOptions,
      readOnlyQueries: true,
    });

    const executed = queryFn.mock.calls.at(-1)?.[0] as {
      text: string;
      queryMode?: string;
    };
    expect(executed.queryMode).toBe("extended");
    expect(executed.text).toMatch(/LIMIT 101/i);
  });

  it("leaves a multi-statement source on the simple protocol", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(
      pool,
      "SET statement_timeout = '5000'; SELECT 2 LIMIT 1",
      100,
      {
        readonly: false,
        allowMultiStatements: true,
        readOnlyQueries: false,
      }
    );

    expect(queryFn.mock.calls.at(-1)?.[0]).toBe(
      "SET statement_timeout = '5000'; SELECT 2 LIMIT 1"
    );
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

  describe("multi-statement result handling", () => {
    const multiStatementOptions = {
      readonly: false,
      allowMultiStatements: true,
    };

    // Shapes measured against PostgreSQL 16 (`fields` is what separates a row-returning
    // statement from one that only reports a command tag):
    //   SET / BEGIN / COMMIT      rows 0, fields 0
    //   SELECT                    rows n, fields n
    //   SELECT ... WHERE false    rows 0, fields n
    //   SHOW                      rows 1, fields 1
    const setResult = { rows: [], rowCount: null, command: "SET", fields: [] };
    const beginResult = {
      rows: [],
      rowCount: null,
      command: "BEGIN",
      fields: [],
    };
    const commitResult = {
      rows: [],
      rowCount: null,
      command: "COMMIT",
      fields: [],
    };
    const selectResult = (rows: Record<string, unknown>[]) => ({
      rows,
      rowCount: rows.length,
      command: "SELECT",
      fields: [{ name: "id" }],
    });

    it("returns the last row-returning result when node-postgres returns an array", async () => {
      // node-postgres returns an ARRAY of QueryResults (not a single QueryResult) when the
      // server executed more than one command. This reproduces that shape.
      const queryFn = vi
        .fn()
        .mockResolvedValue([setResult, selectResult([{ id: 1 }, { id: 2 }])]);
      const pool = createMockPool(queryFn);

      const result = await executeQuery(
        pool,
        "SET statement_timeout = '5000'; SELECT id FROM users",
        100,
        multiStatementOptions
      );

      expect(result.rows).toEqual([{ id: 1 }, { id: 2 }]);
      expect(result.rowCount).toBe(2);
      expect(result.truncated).toBe(false);
    });

    it("returns the SELECT, not the trailing COMMIT, for a transaction batch", async () => {
      const queryFn = vi
        .fn()
        .mockResolvedValue([
          beginResult,
          selectResult([{ id: 1 }]),
          commitResult,
        ]);
      const pool = createMockPool(queryFn);

      const result = await executeQuery(
        pool,
        "BEGIN; SELECT id FROM users; COMMIT",
        100,
        multiStatementOptions
      );

      expect(result.rows).toEqual([{ id: 1 }]);
      expect(result.rowCount).toBe(1);
    });

    it("returns the SELECT when an earlier statement also returned rows (SHOW; SELECT)", async () => {
      // SHOW returns a row. Keying the ambiguity check on rows rejected this batch; keying it
      // on the AST lets it through and the runtime path picks the last row-returning result.
      const queryFn = vi.fn().mockResolvedValue([
        {
          rows: [{ statement_timeout: "5s" }],
          rowCount: null,
          command: "SHOW",
          fields: [{ name: "statement_timeout" }],
        },
        selectResult([{ id: 1 }]),
      ]);
      const pool = createMockPool(queryFn);

      const result = await executeQuery(
        pool,
        "SHOW statement_timeout; SELECT id FROM users",
        100,
        multiStatementOptions
      );

      expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("treats a zero-row SELECT as the row-returning result, not the SET before it", async () => {
      const queryFn = vi.fn().mockResolvedValue([setResult, selectResult([])]);
      const pool = createMockPool(queryFn);

      const result = await executeQuery(
        pool,
        "SET statement_timeout = '5000'; SELECT id FROM users WHERE false",
        100,
        multiStatementOptions
      );

      expect(result.rows).toEqual([]);
      expect(result.rowCount).toBe(0);
      expect(result.truncated).toBe(false);
    });

    it("applies max_rows truncation to the chosen result set", async () => {
      const rows = Array.from({ length: 11 }, (_, i) => ({ id: i + 1 }));
      const queryFn = vi
        .fn()
        .mockResolvedValue([setResult, selectResult(rows)]);
      const pool = createMockPool(queryFn);

      const result = await executeQuery(
        pool,
        "SET statement_timeout = '5000'; SELECT id FROM users",
        10,
        multiStatementOptions
      );

      expect(result.truncated).toBe(true);
      expect(result.rowCount).toBe(10);
      expect(result.rows).toHaveLength(10);
    });

    it("rejects two row-returning statements before sending anything to the database", async () => {
      const queryFn = vi.fn();
      const pool = createMockPool(queryFn);

      await expect(
        executeQuery(
          pool,
          "SELECT id FROM a; SELECT id FROM b",
          100,
          multiStatementOptions
        )
      ).rejects.toThrow("more than one statement that returns rows");
      expect(queryFn).not.toHaveBeenCalled();
    });

    it("rejects a write batch with two row-returning statements before it can commit", async () => {
      const queryFn = vi.fn();
      const pool = createMockPool(queryFn);

      await expect(
        executeQuery(
          pool,
          "INSERT INTO a (n) VALUES (1) RETURNING n; SELECT n FROM a",
          100,
          multiStatementOptions
        )
      ).rejects.toThrow("more than one statement that returns rows");
      expect(queryFn).not.toHaveBeenCalled();
    });

    it("still rejects two row-returning results at runtime when the parser could not check", async () => {
      // Fallback for batches astify rejects: the ambiguity is only visible in the results.
      const queryFn = vi
        .fn()
        .mockResolvedValue([
          selectResult([{ id: 1 }]),
          selectResult([{ id: 2 }]),
        ]);
      const pool = createMockPool(queryFn);

      await expect(
        executeQuery(pool, "TABLE a; TABLE b", 100, multiStatementOptions)
      ).rejects.toThrow("more than one statement that returns rows");
    });

    it("handles an all-empty batch (e.g. SET; SET) without error", async () => {
      const queryFn = vi.fn().mockResolvedValue([setResult, setResult]);
      const pool = createMockPool(queryFn);

      const result = await executeQuery(
        pool,
        "SET a.b = '1'; SET c.d = '2'",
        100,
        multiStatementOptions
      );

      expect(result.rows).toEqual([]);
      expect(result.rowCount).toBe(0);
      expect(result.truncated).toBe(false);
    });

    it("still rejects multi-statement SQL when allowMultiStatements is false (guard unchanged)", async () => {
      // This exercises ensureLimit's existing guard, not the array handling — it must keep
      // rejecting before a query is ever sent, on both read-only and non-read-only sources.
      const queryFn = vi.fn();
      const pool = createMockPool(queryFn);

      await expect(
        executeQuery(pool, "SELECT 1; SELECT 2", 100, defaultOptions)
      ).rejects.toThrow("Multi-statement queries are not allowed");
      expect(queryFn).not.toHaveBeenCalled();
    });

    it("still rejects multi-statement SQL on a readonly + allowMultiStatements:false source", async () => {
      const queryFn = vi.fn();
      const pool = createMockPool(queryFn);

      await expect(
        executeQuery(pool, "SELECT 1; SELECT 2", 100, {
          readonly: true,
          allowMultiStatements: false,
        })
      ).rejects.toThrow("Multi-statement queries are not allowed");
      expect(queryFn).not.toHaveBeenCalled();
    });

    it("rejects an array of results on a source that did not allow multiple statements", async () => {
      // The smuggling shape: node-sql-parser reads the payload as one SELECT (so ensureLimit
      // sees nothing to reject) and PostgreSQL runs three commands. Selecting the last
      // row-returning result here would hand back the smuggled statement's rows on the
      // success path, where the audit log records it as an ordinary read.
      const queryFn = vi
        .fn()
        .mockResolvedValue([
          selectResult([]),
          setResult,
          selectResult([{ id: 1, tenant: "b" }]),
        ]);
      const pool = createMockPool(queryFn);
      const warn = vi.spyOn(logger, "warn").mockImplementation(() => {});

      await expect(
        executeQuery(
          pool,
          "SELECT 'x\\' FROM t WHERE 1=0; SET app.partner_id TO \"tenantB\"; SELECT * FROM t; --'",
          100,
          defaultOptions
        )
      ).rejects.toThrow("Multi-statement queries are not allowed");

      expect(warn).toHaveBeenCalledWith(
        "Multi-statement batch on a source that does not allow one",
        expect.objectContaining({ statements: 3 })
      );
      warn.mockRestore();
    });
  });
});
