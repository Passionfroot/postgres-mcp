import NodeSqlParser from "node-sql-parser";
const { Parser } = NodeSqlParser;

import type { MergedSchema } from "./schema/types.js";

const parser = new Parser();
const PG_OPT = { database: "PostgreSQL" } as const;

/* eslint-disable @typescript-eslint/no-explicit-any --
   node-sql-parser's AST types are too loose for strict typing; we validate shape at runtime */
type AstNode = any;

function getTableName(ref: unknown): string | null {
  if (ref === null || ref === undefined) return null;
  if (typeof ref === "string") return ref;
  if (typeof ref === "object" && "value" in (ref as Record<string, unknown>)) {
    return (ref as { value: string }).value;
  }
  return null;
}

function buildColumnNode(columnName: string, tableAlias: string | null) {
  const table = tableAlias
    ? { type: "default", value: tableAlias }
    : null;

  return {
    type: "expr",
    expr: {
      type: "column_ref",
      table,
      column: { expr: { type: "double_quote_string", value: columnName } },
      collate: null,
    },
    as: null,
  };
}

function resolveTableColumns(
  tableName: string,
  schema: MergedSchema
): string[] | null {
  const table = schema.tables.find(
    (t) => t.sqlName.toLowerCase() === tableName.toLowerCase()
  );
  if (!table) return null;
  return table.columns.map((c) => c.sqlName);
}

function resolveAliasToTable(
  alias: string,
  fromClauses: AstNode[]
): string | null {
  for (const f of fromClauses) {
    if (f.as === alias) return f.table;
    if (!f.as && f.table === alias) return f.table;
  }
  return null;
}

/**
 * Expand SELECT * and SELECT t.* into explicit column lists using the introspected schema.
 *
 * This prevents "permission denied for column" errors when the connected user has column-level
 * security and only SELECT on a subset of columns. The schema (already filtered by
 * has_column_privilege) tells us exactly which columns are accessible.
 *
 * Returns the original SQL unchanged when:
 * - The query has no star columns
 * - The query can't be parsed
 * - The referenced table isn't in the schema
 * - The query is not a single SELECT
 */
export function expandStarColumns(
  sql: string,
  schema: MergedSchema
): string {
  let ast;
  try {
    const raw = parser.astify(sql, PG_OPT);
    const statements = Array.isArray(raw) ? raw : [raw];
    if (statements.length !== 1) return sql;
    ast = statements[0];
  } catch {
    return sql;
  }

  if (ast.type !== "select") return sql;
  if (!Array.isArray(ast.columns)) return sql;

  const fromClauses: AstNode[] = Array.isArray(ast.from) ? ast.from : [];

  let hasStars = false;
  for (const col of ast.columns) {
    const expr = col.expr ?? col;
    if (expr.type === "column_ref" && expr.column === "*") {
      hasStars = true;
      break;
    }
  }
  if (!hasStars) return sql;

  const expandedColumns: unknown[] = [];
  let anyExpanded = false;

  for (const col of ast.columns) {
    const expr = col.expr ?? col;
    if (expr.type !== "column_ref" || expr.column !== "*") {
      expandedColumns.push(col);
      continue;
    }

    const tableRef = getTableName(expr.table);

    if (tableRef === null && fromClauses.length === 1) {
      // SELECT * with a single table
      const tableName = fromClauses[0].table;
      const columns = resolveTableColumns(tableName, schema);
      if (!columns) {
        expandedColumns.push(col);
        continue;
      }
      const alias = fromClauses[0].as;
      for (const colName of columns) {
        expandedColumns.push(buildColumnNode(colName, alias));
      }
      anyExpanded = true;
    } else if (tableRef === null && fromClauses.length > 1) {
      // SELECT * with multiple tables: expand each table's columns
      let allResolved = true;
      const multiColumns: unknown[] = [];
      for (const f of fromClauses) {
        const columns = resolveTableColumns(f.table, schema);
        if (!columns) {
          allResolved = false;
          break;
        }
        const alias = f.as;
        for (const colName of columns) {
          multiColumns.push(buildColumnNode(colName, alias ?? f.table));
        }
      }
      if (allResolved) {
        expandedColumns.push(...multiColumns);
        anyExpanded = true;
      } else {
        expandedColumns.push(col);
      }
    } else if (tableRef !== null) {
      // SELECT t.* with alias or table name
      const tableName = resolveAliasToTable(tableRef, fromClauses);
      if (!tableName) {
        expandedColumns.push(col);
        continue;
      }
      const columns = resolveTableColumns(tableName, schema);
      if (!columns) {
        expandedColumns.push(col);
        continue;
      }
      for (const colName of columns) {
        expandedColumns.push(buildColumnNode(colName, tableRef));
      }
      anyExpanded = true;
    } else {
      expandedColumns.push(col);
    }
  }

  if (!anyExpanded) return sql;

  ast.columns = expandedColumns;
  return parser.sqlify(ast, PG_OPT);
}
