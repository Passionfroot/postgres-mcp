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
  readonly reason: string;

  constructor(reason: string) {
    super(
      `This source only answers read-only SELECT queries, and this one was rejected because ${reason}. ` +
        "Statements that change the role or session (SET, RESET, SET ROLE, set_config), non-SELECT " +
        "statements, multiple statements in one call, and server-side file/program access are not permitted."
    );
    this.name = "ReadOnlyQueryError";
    this.reason = reason;
  }
}

// Statement types that write. A top-level one is caught by the `type !== "select"` check, but they
// also hide inside a SELECT: a data-modifying CTE (`WITH x AS (UPDATE ... RETURNING) SELECT * FROM x`)
// parses as a top-level `select`, so we walk the whole tree for any of these.
const WRITE_STATEMENT_TYPES = new Set([
  "insert",
  "update",
  "delete",
  "replace",
  "merge",
  "create",
  "drop",
  "alter",
  "truncate",
  "rename",
  "grant",
  "revoke",
  "call",
  "set",
]);

function hasWriteStatement(node: unknown): boolean {
  if (!node || typeof node !== "object") return false;
  if (Array.isArray(node)) return node.some(hasWriteStatement);
  const obj = node as Record<string, unknown>;
  if (typeof obj.type === "string" && WRITE_STATEMENT_TYPES.has(obj.type)) {
    return true;
  }
  // SELECT ... INTO <table> creates a table.
  if (obj.type === "select") {
    const into = obj.into as { expr?: unknown } | undefined;
    if (into?.expr) return true;
  }
  return Object.keys(obj).some((key) => hasWriteStatement(obj[key]));
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

// Reject inputs where node-sql-parser and PostgreSQL disagree about where a string
// literal ends, because that is where they disagree about where a statement ends.
//
// In a plain '...' literal, node-sql-parser applies MySQL-style backslash escaping and
// reads \' as an escaped quote, so the string keeps consuming. PostgreSQL with
// standard_conforming_strings=on (the default since 9.1) treats the backslash as an
// ordinary character, so the quote CLOSES the literal and everything after the next `;`
// is a separate statement. The guard then vets one clean SELECT while the server
// executes several, which is how
//   SELECT 'x\'; SET app.partner_id TO "tenantB"; SELECT ...; --'
// re-points tenant scope from an unprivileged reader.
//
// The divergence is exactly an ODD run of backslashes immediately before a quote. An
// even run (`'x\\'`) is an escaped backslash to node-sql-parser and two literal
// backslashes to PostgreSQL: the contents differ but both agree the quote closes, so
// no statement can be smuggled. That was verified against PostgreSQL 16 rather than
// assumed, along with the constructs deliberately NOT rejected here: dollar-quoted
// strings ($$...$$, $tag$...$tag$), E'...' escape strings (PostgreSQL honours
// backslash escapes inside those, so the two lexers agree), doubled '' quotes,
// comments and quoted identifiers all produce identical statement boundaries on both
// sides. Rejecting them would add false positives and close nothing.
function findStatementBoundaryHazard(sql: string): string | null {
  const HAZARD =
    "a string literal has a backslash immediately before its closing quote, the one " +
    "spot where the SQL parser and PostgreSQL disagree about where the literal ends. " +
    "Backslashes elsewhere inside a literal are fine; for a value that ends in one, " +
    "write it as an escape string (E'\\\\') or build it with chr(92)";

  let i = 0;

  while (i < sql.length) {
    const char = sql[i];

    if (char === "-" && sql[i + 1] === "-") {
      const newline = sql.indexOf("\n", i);
      i = newline === -1 ? sql.length : newline + 1;
      continue;
    }

    // PostgreSQL block comments nest.
    if (char === "/" && sql[i + 1] === "*") {
      let depth = 1;
      i += 2;
      while (i < sql.length && depth > 0) {
        if (sql[i] === "/" && sql[i + 1] === "*") {
          depth++;
          i += 2;
        } else if (sql[i] === "*" && sql[i + 1] === "/") {
          depth--;
          i += 2;
        } else {
          i++;
        }
      }
      continue;
    }

    // Quoted identifier: "" doubles, backslash means nothing.
    if (char === '"') {
      i++;
      while (i < sql.length) {
        if (sql[i] === '"') {
          if (sql[i + 1] === '"') {
            i += 2;
            continue;
          }
          i++;
          break;
        }
        i++;
      }
      continue;
    }

    // Dollar-quoted string: no escapes at all inside, ends at the matching tag.
    // A bare `$1` placeholder is not a dollar quote.
    const dollarTag = char === "$" ? /^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i)) : null;
    if (dollarTag) {
      const tag = dollarTag[0];
      const close = sql.indexOf(tag, i + tag.length);
      i = close === -1 ? sql.length : close + tag.length;
      continue;
    }

    // E'...' escape string: backslash escapes the next character in PostgreSQL too,
    // so both lexers agree and there is nothing to flag. Only when E starts a token;
    // `date'2026-01-01'` is a typed literal, not an escape string.
    if (
      (char === "E" || char === "e") &&
      sql[i + 1] === "'" &&
      !/[A-Za-z0-9_$]/.test(sql[i - 1] ?? " ")
    ) {
      i += 2;
      while (i < sql.length) {
        if (sql[i] === "\\") {
          i += 2;
          continue;
        }
        if (sql[i] === "'") {
          if (sql[i + 1] === "'") {
            i += 2;
            continue;
          }
          i++;
          break;
        }
        i++;
      }
      continue;
    }

    // Plain '...' literal, lexed the way PostgreSQL does: '' doubles, backslash is an
    // ordinary character. Flag any quote reached across an odd run of backslashes.
    if (char === "'") {
      i++;
      let backslashRun = 0;
      while (i < sql.length) {
        if (sql[i] === "\\") {
          backslashRun++;
          i++;
          continue;
        }
        if (sql[i] === "'") {
          if (backslashRun % 2 === 1) return HAZARD;
          if (sql[i + 1] === "'") {
            i += 2;
            backslashRun = 0;
            continue;
          }
          i++;
          break;
        }
        backslashRun = 0;
        i++;
      }
      continue;
    }

    i++;
  }

  return null;
}

// `EXPLAIN [ ( option [, ...] ) | ANALYZE | VERBOSE ] statement`. ANALYZE actually
// runs the statement, so it stays rejected; everything else is planning only and is
// stripped so the statement behind it gets the full guard.
const EXPLAIN_PREFIX_RE =
  /^\s*EXPLAIN\s*(?:\(([^()]*)\)|((?:\s*\b(?:ANALYZE|ANALYSE|VERBOSE)\b)*))\s+/i;
const EXPLAIN_ANALYZE_OPTION_RE = /\b(?:ANALYZE|ANALYSE)\b/i;

function stripExplainPrefix(sql: string): string {
  const match = EXPLAIN_PREFIX_RE.exec(sql);
  if (!match) return sql;

  const options = match[1] ?? match[2] ?? "";
  if (EXPLAIN_ANALYZE_OPTION_RE.test(options)) {
    throw new ReadOnlyQueryError(
      "EXPLAIN ANALYZE executes the statement it explains; use EXPLAIN without ANALYZE"
    );
  }

  return sql.slice(match[0].length);
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
  const hazard = findStatementBoundaryHazard(sql);
  if (hazard) {
    throw new ReadOnlyQueryError(hazard);
  }

  const body = stripExplainPrefix(sql);

  if (SESSION_MUTATION_RE.test(body)) {
    const fn = /\b(set_config|set_role|set_user)\s*\(/i.exec(body);
    throw new ReadOnlyQueryError(
      fn
        ? `the text '${fn[1].toLowerCase()}(' appears in the statement; this source rejects it even inside a string literal, so split the literal (e.g. '%set_' || 'config(%') if that is what you meant`
        : "the statement starts with SET or RESET, which changes session state"
    );
  }

  let raw;
  try {
    raw = parser.astify(body, PG_OPT);
  } catch {
    throw new Error(
      "This source only answers read-only SELECT queries, and this statement could not be " +
        "parsed to verify that, so it was rejected. The guard's parser is stricter than " +
        "PostgreSQL: a reserved word used as a bare alias (AS set, AS order) or an operator " +
        "it does not know are the usual causes. Quote the alias (AS \"set\") or express the " +
        "same read another way."
    );
  }

  const statements = Array.isArray(raw) ? raw : [raw];
  if (statements.length > 1) {
    throw new ReadOnlyQueryError(
      `it contains ${statements.length} statements; send exactly one SELECT per call`
    );
  }
  for (const ast of statements) {
    const type = (ast as { type?: string } | null)?.type;
    if (type !== "select") {
      throw new ReadOnlyQueryError(
        `the top-level statement is '${type ?? "unknown"}', not SELECT`
      );
    }
    // Catch writes hidden inside a top-level SELECT: a data-modifying CTE or SELECT INTO.
    if (hasWriteStatement(ast)) {
      throw new ReadOnlyQueryError(
        "a write statement is nested inside it (a data-modifying CTE or SELECT INTO)"
      );
    }
    const fns = new Set<string>();
    collectFunctionNames(ast, fns);
    for (const fn of fns) {
      if (DANGEROUS_FUNCTIONS.has(fn)) {
        throw new ReadOnlyQueryError(
          `it calls ${fn}(), which changes session state or reaches outside the row set`
        );
      }
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
