import NodeSqlParser from "node-sql-parser";
const { Parser } = NodeSqlParser;
import pg from "pg";

import { logger } from "./logger.js";
import {
  assertSafeGucName,
  escapeIdentifier,
  escapeLiteral,
} from "./sql-helpers.js";

export interface QueryResult {
  rows: Record<string, unknown>[];
  rowCount: number;
  truncated: boolean;
}

interface PgErrorLike {
  code?: string;
  message: string;
  position?: string;
  detail?: string;
  hint?: string;
}

function isPgError(err: unknown): err is PgErrorLike {
  return err instanceof Error && "code" in err;
}

const parser = new Parser();
const PG_OPT = { database: "PostgreSQL" } as const;

const HAS_LIMIT_RE = /\bLIMIT\s+\d/i;
const STARTS_WITH_EXPLAIN_RE = /^\s*EXPLAIN\b/i;

// Functions that mutate session state or reach outside the row set, blocked even
// inside an otherwise-valid SELECT (a plain-statement allowlist can't catch these,
// since they hide in the projection or a subquery). The role/GUC setters are the
// tenant-isolation ones (set_config('app.partner_id', <other tenant>, false)); the
// rest are server-side file/program/large-object/remote-connection reach that an
// unprivileged reader shouldn't be issuing regardless.
const DANGEROUS_FUNCTIONS = new Set([
  "set_config",
  "set_role",
  "set_user",
  "pg_read_file",
  "pg_read_binary_file",
  "pg_ls_dir",
  "pg_stat_file",
  "lo_import",
  "lo_export",
  "dblink",
  "dblink_exec",
]);

// Backstop for what the AST walk can miss: SET ROLE / RESET ROLE do not parse at
// all (so astify throws), and this catches the role/GUC setter functions and the
// command forms before the parser runs. String literals containing these tokens
// fail closed, which is the safe direction for a read-only guard.
const SESSION_MUTATION_RE =
  /\b(?:set_config|set_role|set_user)\s*\(|^\s*(?:set|reset)\b/i;

export class ReadOnlyQueryError extends Error {
  constructor() {
    super(
      "This source only answers read-only SELECT queries. Statements that change " +
        "the role or session (SET, RESET, SET ROLE, set_config), non-SELECT " +
        "statements, and server-side file/program access are not permitted. " +
        "Rewrite it as a plain SELECT."
    );
    this.name = "ReadOnlyQueryError";
  }
}

function collectFunctionNames(node: unknown, acc: Set<string>): void {
  if (!node || typeof node !== "object") return;
  if (Array.isArray(node)) {
    for (const item of node) collectFunctionNames(item, acc);
    return;
  }
  const obj = node as Record<string, unknown>;
  if (obj.type === "function") {
    const nameNode = obj.name as
      | { name?: Array<{ value?: string }> }
      | undefined;
    const parts = nameNode?.name;
    const fnName = Array.isArray(parts)
      ? parts[parts.length - 1]?.value
      : undefined;
    if (typeof fnName === "string") acc.add(fnName.toLowerCase());
  }
  for (const key of Object.keys(obj)) collectFunctionNames(obj[key], acc);
}

/**
 * Allow only read-only SELECT queries. Everything else is rejected: any non-SELECT
 * statement (SET, RESET, COPY, DO, CALL, DML, DDL, transaction control), any query
 * that calls a dangerous function (role/GUC setters, file/program/large-object
 * access), and any query the parser cannot verify. This is an allowlist, not a
 * denylist of specific commands. A source whose tenant scope rides on
 * role/session_vars needs it so a submitted query cannot re-point the scope or
 * leave the restricted role. A parse failure fails closed rather than falling
 * through to the regex LIMIT path.
 */
export function assertReadOnlyQuery(sql: string): void {
  if (SESSION_MUTATION_RE.test(sql)) {
    throw new ReadOnlyQueryError();
  }

  let raw;
  try {
    raw = parser.astify(sql, PG_OPT);
  } catch {
    throw new Error(
      "This source only answers read-only SELECT queries, and this statement could " +
        "not be parsed to verify that. Rewrite it as a plain SELECT."
    );
  }

  const statements = Array.isArray(raw) ? raw : [raw];
  for (const ast of statements) {
    if (!ast || (ast as { type?: string }).type !== "select") {
      throw new ReadOnlyQueryError();
    }
    const fns = new Set<string>();
    collectFunctionNames(ast, fns);
    for (const fn of fns) {
      if (DANGEROUS_FUNCTIONS.has(fn)) throw new ReadOnlyQueryError();
    }
  }
}

/**
 * Parse the SQL, and if it's a single SELECT without a LIMIT, append one.
 *
 * When allowMultiStatements is false, rejects multi-statement queries. On parse failure, falls back
 * to a regex-based LIMIT append rather than running unlimited queries.
 */
export function ensureLimit(
  sql: string,
  limit: number,
  allowMultiStatements: boolean
) {
  try {
    const raw = parser.astify(sql, PG_OPT);

    const statements = Array.isArray(raw) ? raw : [raw];
    if (statements.length > 1) {
      if (!allowMultiStatements) {
        throw new Error(
          "Multi-statement queries are not allowed on this source. Send one statement at a time."
        );
      }
      return sql;
    }
    const ast = statements[0];
    if (ast.type !== "select") return sql;
    if (ast.limit?.value?.length) return sql;

    ast.limit = {
      seperator: "",
      value: [{ type: "number", value: limit }],
    };

    return parser.sqlify(ast, PG_OPT);
  } catch (err) {
    // Re-throw our own multi-statement error
    if (err instanceof Error && err.message.includes("Multi-statement"))
      throw err;

    // Parser failed — apply regex fallback LIMIT instead of running unlimited
    if (!HAS_LIMIT_RE.test(sql) && !STARTS_WITH_EXPLAIN_RE.test(sql)) {
      return `${sql.replace(/;\s*$/, "")} LIMIT ${limit}`;
    }
    return sql;
  }
}

export function formatPgError(err: PgErrorLike) {
  const lines = [`PostgreSQL error ${err.code}: ${err.message}`];

  if (err.position) {
    lines.push(`at position ${err.position}`);
  }

  if (err.detail) {
    lines.push(`Detail: ${err.detail}`);
  }

  if (err.hint) {
    lines.push(`Hint: ${err.hint}`);
  }

  return lines.join("\n");
}

export interface ExecuteQueryOptions {
  readonly: boolean;
  allowMultiStatements: boolean;
  readOnlyQueries?: boolean;
  role?: string;
  sessionVars?: Record<string, string>;
  expandStar?: (sql: string) => string;
}

export async function executeQuery(
  pool: pg.Pool,
  sql: string,
  maxRows: number,
  options: ExecuteQueryOptions
): Promise<QueryResult> {
  if (options.readOnlyQueries) {
    assertReadOnlyQuery(sql);
  }

  const expandedSql = options.expandStar ? options.expandStar(sql) : sql;
  const limitedSql = ensureLimit(
    expandedSql,
    maxRows + 1,
    options.allowMultiStatements
  );

  const client = await pool.connect();
  try {
    if (options.role) {
      await client.query(`SET ROLE ${escapeIdentifier(options.role)}`);
    }

    if (options.sessionVars) {
      for (const [key, value] of Object.entries(options.sessionVars)) {
        assertSafeGucName(key);
        await client.query(`SET ${key} = ${escapeLiteral(value)}`);
      }
    }

    if (options.readonly) {
      await client.query(
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"
      );
    }

    const result = await client.query(limitedSql);
    const rows: Record<string, unknown>[] = result.rows;

    const isTruncated = rows.length > maxRows;
    const slicedRows = isTruncated ? rows.slice(0, maxRows) : rows;

    return {
      rows: slicedRows,
      rowCount: slicedRows.length,
      truncated: isTruncated,
    };
  } catch (err: unknown) {
    if (!isPgError(err)) throw err;

    if (err.code === "57014") {
      throw new Error(
        "Query timed out. Simplify the query or add more specific WHERE conditions."
      );
    }

    if (err.code === "42501") {
      throw new Error(
        formatPgError(err) +
          "\n\nPermission denied. Use search_objects to check which columns are accessible, " +
          "then list them explicitly in your query."
      );
    }

    if (err.code) {
      throw new Error(formatPgError(err));
    }

    throw err;
  } finally {
    try {
      if (options.sessionVars) {
        for (const key of Object.keys(options.sessionVars)) {
          await client.query(`RESET ${key}`);
        }
      }
      if (options.role) {
        await client.query("RESET ROLE");
      }
    } catch (cleanupErr) {
      logger.warn(
        "Failed to reset RLS session state; connection may be discarded by pool",
        {
          error:
            cleanupErr instanceof Error
              ? cleanupErr.message
              : String(cleanupErr),
          role: options.role ?? "",
          sessionVarKeys: options.sessionVars
            ? Object.keys(options.sessionVars).join(", ")
            : "",
        }
      );
    }
    client.release();
  }
}
