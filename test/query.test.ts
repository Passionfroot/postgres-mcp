import { describe, expect, it, vi } from "vitest";

import { ensureLimit, executeQuery, formatPgError } from "../src/query.js";

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
    const sql = "WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active";
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

    expect(result).toBe('PostgreSQL error 42601: syntax error at or near "SELCT"');
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
      _client: { query: ReturnType<typeof vi.fn>; release: ReturnType<typeof vi.fn> };
    };
  }

  const defaultOptions = { readonly: false, allowMultiStatements: false };

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

    await executeQuery(pool, "SELECT * FROM users LIMIT 5", 100, defaultOptions);

    const calledSql = queryFn.mock.calls[0][0] as string;
    expect(calledSql).toMatch(/LIMIT 5/i);
    expect(calledSql).not.toMatch(/LIMIT 101/i);
  });

  it("detects truncation when rows exceed maxRows", async () => {
    const rows = Array.from({ length: 11 }, (_, i) => ({ id: i + 1 }));
    const queryFn = vi.fn().mockResolvedValue({ rows });
    const pool = createMockPool(queryFn);

    const result = await executeQuery(pool, "SELECT * FROM users", 10, defaultOptions);

    expect(result.truncated).toBe(true);
    expect(result.rowCount).toBe(10);
    expect(result.rows).toHaveLength(10);
  });

  it("returns truncated=false when under limit", async () => {
    const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
    const queryFn = vi.fn().mockResolvedValue({ rows });
    const pool = createMockPool(queryFn);

    const result = await executeQuery(pool, "SELECT * FROM users", 10, defaultOptions);

    expect(result.truncated).toBe(false);
    expect(result.rowCount).toBe(3);
    expect(result.rows).toHaveLength(3);
  });

  it("sets readonly session before executing query", async () => {
    const queryFn = vi.fn().mockResolvedValue({ rows: [{ id: 1 }] });
    const pool = createMockPool(queryFn);

    await executeQuery(pool, "SELECT 1", 10, { readonly: true, allowMultiStatements: false });

    expect(queryFn).toHaveBeenCalledTimes(2);
    expect(queryFn.mock.calls[0][0]).toBe("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY");
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
      (pool as unknown as { _client: { release: ReturnType<typeof vi.fn> } })._client.release
    ).toHaveBeenCalledTimes(1);
  });

  it("releases client after failed query", async () => {
    const pgError = Object.assign(new Error('relation "xyz" does not exist'), {
      code: "42P01",
      position: "15",
    });
    const queryFn = vi.fn().mockRejectedValue(pgError);
    const pool = createMockPool(queryFn);

    await expect(executeQuery(pool, "SELECT * FROM xyz", 100, defaultOptions)).rejects.toThrow();

    expect(
      (pool as unknown as { _client: { release: ReturnType<typeof vi.fn> } })._client.release
    ).toHaveBeenCalledTimes(1);
  });

  it("formats PG errors with code/message/position", async () => {
    const pgError = Object.assign(new Error('relation "xyz" does not exist'), {
      code: "42P01",
      position: "15",
    });
    const queryFn = vi.fn().mockRejectedValue(pgError);
    const pool = createMockPool(queryFn);

    await expect(executeQuery(pool, "SELECT * FROM xyz", 100, defaultOptions)).rejects.toThrow(
      'PostgreSQL error 42P01: relation "xyz" does not exist'
    );
  });

  it("identifies timeout errors with actionable message", async () => {
    const timeoutError = Object.assign(new Error("canceling statement due to statement timeout"), {
      code: "57014",
    });
    const queryFn = vi.fn().mockRejectedValue(timeoutError);
    const pool = createMockPool(queryFn);

    await expect(executeQuery(pool, "SELECT pg_sleep(999)", 100, defaultOptions)).rejects.toThrow(
      "Query timed out. Simplify the query or add more specific WHERE conditions."
    );
  });
});
