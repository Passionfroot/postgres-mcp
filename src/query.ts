import { Parser } from "node-sql-parser";
import pg from "pg";

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
export function ensureLimit(sql: string, limit: number, allowMultiStatements: boolean) {
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
    if (err instanceof Error && err.message.includes("Multi-statement")) throw err;

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

export async function executeQuery(
  pool: pg.Pool,
  sql: string,
  maxRows: number,
  options: { readonly: boolean; allowMultiStatements: boolean }
): Promise<QueryResult> {
  const limitedSql = ensureLimit(sql, maxRows + 1, options.allowMultiStatements);

  const client = await pool.connect();
  try {
    if (options.readonly) {
      await client.query("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY");
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
      throw new Error("Query timed out. Simplify the query or add more specific WHERE conditions.");
    }

    if (err.code) {
      throw new Error(formatPgError(err));
    }

    throw err;
  } finally {
    client.release();
  }
}
