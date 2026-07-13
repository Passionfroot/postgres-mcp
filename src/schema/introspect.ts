import pg from "pg";

import type { DbColumn, DbEnumValue, DbForeignKey, DbMetadata, DbPrimaryKey } from "./types.js";

import { assertSafeGucName, escapeIdentifier, escapeLiteral } from "../sql-helpers.js";
import { logger } from "../logger.js";

/**
 * Only return columns where the connected user (or effective role) has SELECT privilege.
 * This filters out columns hidden by column-level security. (RLS does not affect these
 * checks — it filters rows at query time, not privileges.)
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

/**
 * Key queries check the key column itself with has_column_privilege so PK/FK metadata stays
 * consistent with the filtered columns query. has_table_privilege would be wrong here: it only
 * considers table-level ACLs and returns false for users with column-level grants only.
 */
const PRIMARY_KEYS_QUERY = `
SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
  AND has_column_privilege(format('%I.%I', tc.table_schema, tc.table_name), kcu.column_name, 'SELECT')
ORDER BY tc.table_name, kcu.ordinal_position
`;

/**
 * Use pg_constraint instead of information_schema for FK discovery. The information_schema
 * views (constraint_column_usage) require ownership or REFERENCES privilege on the referenced
 * table, so roles with only column-level SELECT grants (like zest_mcp_reader) see zero FKs.
 * pg_constraint is visible to all roles and filtered by has_column_privilege on the FK column.
 */
const FOREIGN_KEYS_QUERY = `
SELECT
  rel.relname AS from_table,
  a.attname AS from_column,
  frel.relname AS to_table,
  af.attname AS to_column
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_class frel ON frel.oid = con.confrelid
JOIN pg_namespace n ON n.oid = con.connamespace
JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS cols(conkey, confkey, ord) ON true
JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = cols.conkey
JOIN pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = cols.confkey
WHERE con.contype = 'f' AND n.nspname = 'public'
  AND has_column_privilege(con.conrelid, a.attnum, 'SELECT')
ORDER BY from_table, from_column, cols.ord
`;

/**
 * Single-column uniqueness. Used to determine FK cardinality: if the FK source column is
 * unique the relationship is 1:1, otherwise 1:many.
 *
 * Sourced from pg_index, not pg_constraint: Prisma emits @unique as a UNIQUE INDEX rather
 * than a UNIQUE constraint, so constraint-only discovery misses every Prisma 1:1 relation.
 * pg_index covers both (PKs and UNIQUE constraints are backed by unique indexes too).
 * Excludes multi-column indexes (indnatts = 1), partial indexes (indpred), and
 * expression indexes (indkey[0] = 0), none of which guarantee single-column uniqueness.
 */
const UNIQUE_COLUMNS_QUERY = `
SELECT
  rel.relname AS table_name,
  a.attname AS column_name
FROM pg_index idx
JOIN pg_class rel ON rel.oid = idx.indrelid
JOIN pg_namespace n ON n.oid = rel.relnamespace
JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = idx.indkey[0]
WHERE idx.indisunique AND idx.indnatts = 1 AND idx.indpred IS NULL
  AND idx.indkey[0] <> 0 AND n.nspname = 'public'
ORDER BY table_name, column_name
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

interface UniqueColumnRow {
  table_name: string;
  column_name: string;
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

  const [columnsResult, pksResult, fksResult, enumsResult, uniqueResult] = await Promise.all([
    pool.query<ColumnRow>(COLUMNS_QUERY),
    pool.query<PkRow>(PRIMARY_KEYS_QUERY),
    pool.query<FkRow>(FOREIGN_KEYS_QUERY),
    pool.query<EnumRow>(ENUM_VALUES_QUERY),
    pool.query<UniqueColumnRow>(UNIQUE_COLUMNS_QUERY),
  ]);

  return buildMetadata(columnsResult.rows, pksResult.rows, fksResult.rows, enumsResult.rows, uniqueResult.rows);
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

    const [columnsResult, pksResult, fksResult, enumsResult, uniqueResult] = await Promise.all([
      client.query<ColumnRow>(COLUMNS_QUERY),
      client.query<PkRow>(PRIMARY_KEYS_QUERY),
      client.query<FkRow>(FOREIGN_KEYS_QUERY),
      client.query<EnumRow>(ENUM_VALUES_QUERY),
      client.query<UniqueColumnRow>(UNIQUE_COLUMNS_QUERY),
    ]);

    return buildMetadata(columnsResult.rows, pksResult.rows, fksResult.rows, enumsResult.rows, uniqueResult.rows);
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
  enumRows: EnumRow[],
  uniqueRows: UniqueColumnRow[]
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

  const uniqueColumns = new Set(uniqueRows.map((r) => `${r.table_name}.${r.column_name}`));

  return { columns, primaryKeys, foreignKeys, enumValues, uniqueColumns };
}
