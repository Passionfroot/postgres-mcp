import pg from "pg";

import type {
  DbColumn,
  DbEnumValue,
  DbForeignKey,
  DbMetadata,
  DbPrimaryKey,
  DbUniqueColumnSet,
} from "./types.js";

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
 *
 * Both sides are constrained to schema 'public'. Only relnames are returned, so a FK pointing
 * at another schema would otherwise be reported against the same-named public table.
 *
 * conname is returned so composite FKs stay grouped: a 2-column FK is two rows here, and
 * without the constraint identity they are indistinguishable from two separate single-column
 * FKs between the same pair of tables.
 */
const FOREIGN_KEYS_QUERY = `
SELECT
  con.conname AS constraint_name,
  rel.relname AS from_table,
  a.attname AS from_column,
  frel.relname AS to_table,
  af.attname AS to_column
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_class frel ON frel.oid = con.confrelid
JOIN pg_namespace n ON n.oid = con.connamespace
JOIN pg_namespace fn ON fn.oid = frel.relnamespace
JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS cols(conkey, confkey, ord) ON true
JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = cols.conkey
JOIN pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = cols.confkey
WHERE con.contype = 'f' AND n.nspname = 'public' AND fn.nspname = 'public'
  AND has_column_privilege(con.conrelid, a.attnum, 'SELECT')
ORDER BY from_table, from_column, cols.ord
`;

/**
 * Enforced uniqueness, one row per unique index with its ordered key columns. Used to
 * determine FK cardinality: if the FK's column set is unique the relationship is 1:1,
 * otherwise 1:many. Composite FKs need the whole set, so this returns sets rather than
 * single columns.
 *
 * Sourced from pg_index, not pg_constraint: Prisma emits @unique as a UNIQUE INDEX rather
 * than a UNIQUE constraint, so constraint-only discovery misses every Prisma 1:1 relation.
 * pg_index covers both (PKs and UNIQUE constraints are backed by unique indexes too).
 *
 * Only indexes that actually enforce uniqueness over a plain column set qualify:
 * - indisvalid AND indislive: a failed CREATE UNIQUE INDEX CONCURRENTLY leaves indisunique
 *   set on an index that enforces nothing, so duplicates can exist despite the flag.
 * - indpred IS NULL: a partial index only constrains the rows matching its predicate.
 * - all key attnums <> 0: an expression index constrains the expression, not the column.
 *
 * Key columns are indkey[0 .. indnkeyatts-1]. indnatts also counts INCLUDE columns (PG11+),
 * which are payload only and do not widen the uniqueness guarantee.
 *
 * attname is cast to text before aggregating: node-postgres has no parser for name[], and
 * returns it as the raw literal string "{a,b}" instead of an array.
 */
const UNIQUE_COLUMN_SETS_QUERY = `
SELECT
  rel.relname AS table_name,
  keys.column_names
FROM pg_index idx
JOIN pg_class rel ON rel.oid = idx.indrelid
JOIN pg_namespace n ON n.oid = rel.relnamespace
JOIN LATERAL (
  SELECT array_agg(a.attname::text ORDER BY k.ord) AS column_names,
         bool_and(k.attnum <> 0) AS all_plain_columns
  FROM unnest(idx.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
  LEFT JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = k.attnum
  WHERE k.ord <= idx.indnkeyatts
) keys ON true
WHERE idx.indisunique AND idx.indisvalid AND idx.indislive
  AND idx.indpred IS NULL
  AND keys.all_plain_columns
  AND n.nspname = 'public'
ORDER BY table_name, keys.column_names
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
  constraint_name: string;
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

interface UniqueColumnSetRow {
  table_name: string;
  column_names: string[];
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
    pool.query<UniqueColumnSetRow>(UNIQUE_COLUMN_SETS_QUERY),
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
      client.query<UniqueColumnSetRow>(UNIQUE_COLUMN_SETS_QUERY),
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
  uniqueRows: UniqueColumnSetRow[]
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
    constraintName: r.constraint_name ?? null,
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

  const uniqueColumnSets: DbUniqueColumnSet[] = uniqueRows.map((r) => ({
    tableName: r.table_name,
    columnNames: r.column_names,
  }));

  // Single-column sets are the 1:1 signal for single-column FKs. Kept as a flat
  // "table.column" set for consumers that only care about that case.
  const uniqueColumns = new Set(
    uniqueColumnSets
      .filter((s) => s.columnNames.length === 1)
      .map((s) => `${s.tableName}.${s.columnNames[0]}`)
  );

  return { columns, primaryKeys, foreignKeys, enumValues, uniqueColumns, uniqueColumnSets };
}
