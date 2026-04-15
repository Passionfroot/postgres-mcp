import pg from "pg";

import type { DbColumn, DbEnumValue, DbForeignKey, DbMetadata, DbPrimaryKey } from "./types.js";

import { assertSafeGucName, escapeIdentifier, escapeLiteral } from "../sql-helpers.js";
import { logger } from "../logger.js";

/**
 * Only return columns where the connected user (or effective role) has SELECT privilege.
 * This filters out columns hidden by column-level security and tables hidden by RLS.
 *
 * Without this filter, information_schema.columns returns columns where the user has ANY
 * privilege (e.g. REFERENCES from FK constraints), which misleads schema consumers into
 * thinking those columns are queryable.
 */
const COLUMNS_QUERY = `
SELECT table_name, column_name, data_type, udt_name, is_nullable, column_default, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'public'
  AND has_column_privilege(format('%I.%I', table_schema, table_name), column_name, 'SELECT')
ORDER BY table_name, ordinal_position
`;

const PRIMARY_KEYS_QUERY = `
SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
  AND has_table_privilege(format('%I.%I', tc.table_schema, tc.table_name), 'SELECT')
ORDER BY tc.table_name, kcu.ordinal_position
`;

const FOREIGN_KEYS_QUERY = `
SELECT kcu.table_name AS from_table, kcu.column_name AS from_column,
       ccu.table_name AS to_table, ccu.column_name AS to_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
  ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
  AND has_table_privilege(format('%I.%I', tc.table_schema, tc.table_name), 'SELECT')
ORDER BY kcu.table_name, kcu.column_name
`;

const ENUM_VALUES_QUERY = `
SELECT t.typname AS enum_name, e.enumlabel AS enum_value, e.enumsortorder AS sort_order
FROM pg_type t
JOIN pg_enum e ON t.oid = e.enumtypid
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = 'public'
ORDER BY t.typname, e.enumsortorder
`;

interface ColumnRow {
  table_name: string;
  column_name: string;
  data_type: string;
  udt_name: string;
  is_nullable: string;
  column_default: string | null;
  ordinal_position: number;
}

interface PkRow {
  table_name: string;
  column_name: string;
  ordinal_position: number;
}

interface FkRow {
  from_table: string;
  from_column: string;
  to_table: string;
  to_column: string;
}

interface EnumRow {
  enum_name: string;
  enum_value: string;
  sort_order: number;
}

export interface IntrospectOptions {
  role?: string;
  sessionVars?: Record<string, string>;
}

/**
 * Run information_schema queries and return structured database metadata.
 *
 * When role or sessionVars are provided, applies them via SET ROLE / SET before querying
 * so the schema reflects the effective permissions during actual query execution.
 *
 * Columns, PKs, and FKs are filtered by SELECT privilege so the schema only contains
 * objects the user can actually query.
 */
export async function introspectDatabase(
  pool: pg.Pool,
  options?: IntrospectOptions
): Promise<DbMetadata> {
  const needsSession = Boolean(options?.role || options?.sessionVars);

  if (needsSession) {
    return introspectWithSession(pool, options!);
  }

  const [columnsResult, pksResult, fksResult, enumsResult] = await Promise.all([
    pool.query<ColumnRow>(COLUMNS_QUERY),
    pool.query<PkRow>(PRIMARY_KEYS_QUERY),
    pool.query<FkRow>(FOREIGN_KEYS_QUERY),
    pool.query<EnumRow>(ENUM_VALUES_QUERY),
  ]);

  return buildMetadata(columnsResult.rows, pksResult.rows, fksResult.rows, enumsResult.rows);
}

async function introspectWithSession(
  pool: pg.Pool,
  options: IntrospectOptions
): Promise<DbMetadata> {
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

    const [columnsResult, pksResult, fksResult, enumsResult] = await Promise.all([
      client.query<ColumnRow>(COLUMNS_QUERY),
      client.query<PkRow>(PRIMARY_KEYS_QUERY),
      client.query<FkRow>(FOREIGN_KEYS_QUERY),
      client.query<EnumRow>(ENUM_VALUES_QUERY),
    ]);

    return buildMetadata(columnsResult.rows, pksResult.rows, fksResult.rows, enumsResult.rows);
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
      logger.warn("Failed to reset introspection session state", {
        error: cleanupErr instanceof Error ? cleanupErr.message : String(cleanupErr),
        role: options.role ?? "",
        sessionVarKeys: options.sessionVars ? Object.keys(options.sessionVars).join(", ") : "",
      });
    }
    client.release();
  }
}

function buildMetadata(
  columnRows: ColumnRow[],
  pkRows: PkRow[],
  fkRows: FkRow[],
  enumRows: EnumRow[]
): DbMetadata {
  const columns: DbColumn[] = columnRows.map((r) => ({
    tableName: r.table_name,
    columnName: r.column_name,
    dataType: r.data_type,
    udtName: r.udt_name,
    isNullable: r.is_nullable === "YES",
    columnDefault: r.column_default,
    ordinalPosition: r.ordinal_position,
  }));

  const primaryKeys: DbPrimaryKey[] = pkRows.map((r) => ({
    tableName: r.table_name,
    columnName: r.column_name,
    ordinalPosition: r.ordinal_position,
  }));

  const foreignKeys: DbForeignKey[] = fkRows.map((r) => ({
    fromTable: r.from_table,
    fromColumn: r.from_column,
    toTable: r.to_table,
    toColumn: r.to_column,
  }));

  const enumValues: DbEnumValue[] = enumRows.map((r) => ({
    enumName: r.enum_name,
    enumValue: r.enum_value,
    sortOrder: r.sort_order,
  }));

  return { columns, primaryKeys, foreignKeys, enumValues };
}
