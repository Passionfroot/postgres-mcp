import NodeSqlParser from "node-sql-parser";
const { Parser } = NodeSqlParser;
import pg from "pg";

import { logger } from "./logger.js";
import { assertSafeGucName, escapeIdentifier, escapeLiteral } from "./sql-helpers.js";

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
  const expandedSql = options.expandStar ? options.expandStar(sql) : sql;
  const limitedSql = ensureLimit(
    expandedSql,
    maxRows + 1,
    options.allowMultiStatements
  );

  const client = await pool.connect();
  // Set when the connection cannot be trusted for reuse. Releasing without an error hands it
  // straight back to the pool, and at the default pool_max of 1 it is the only connection there.
  let discardReason: string | undefined;
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
    // pg's client-side query_timeout rejects the caller but leaves the server still executing on
    // this connection, so it must not go back into the pool.
    if (err instanceof Error && err.message === "Query read timeout") {
      discardReason = "client-side query timeout";
    }

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
      // The role or session vars may still be set, so this connection must not serve another
      // caller: leaking a pinned app.partner_id across callers would defeat RLS.
      discardReason = "failed to reset session state";
      logger.warn("Failed to reset RLS session state; discarding connection", {
        error: cleanupErr instanceof Error ? cleanupErr.message : String(cleanupErr),
        role: options.role ?? "",
        sessionVarKeys: options.sessionVars ? Object.keys(options.sessionVars).join(", ") : "",
      });
    }
    client.release(discardReason ? new Error(discardReason) : undefined);
  }
}
