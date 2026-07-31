import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { introspectDatabase } from "../../src/schema/introspect.js";
import { mergeSchemas } from "../../src/schema/merge.js";
import { formatSearchResults, searchTables } from "../../src/schema/search.js";
import { createTestPool, resolveTestDb } from "./test-db.js";

/**
 * These tests exercise FK cardinality introspection against a real Postgres, because the
 * failure modes are SQL-semantics bugs (identifier quoting, cross-joined array columns,
 * index validity, INCLUDE columns, cross-schema references) that a mocked pg.Pool cannot
 * reproduce. The fixtures use synthetic tables so the assertions target the *category* of
 * bug, not any specific model.
 */

const { isAvailable } = await resolveTestDb();

// All fixtures share this prefix so setup/teardown can target them without touching the
// rest of the public schema. introspect only reads schema 'public', so they must live there.
const P = "pgmcp_card_";

// Mixed-case identifiers must be double-quoted at creation to preserve their casing.
const MIXED_PARENT = `${P}MixedParent`;
const MIXED_CHILD = `${P}MixedChild`;
const COMP_PARENT = `${P}comp_parent`;
const COMP_CHILD = `${P}comp_child`;
const COMP_CHILD_MANY = `${P}comp_child_many`;
const UNIQ_TABLE = `${P}uniq_index`;
const INVALID_PARENT = `${P}invalid_parent`;
const INVALID_CHILD = `${P}invalid_child`;
const COVERING_PARENT = `${P}covering_parent`;
const COVERING_CHILD = `${P}covering_child`;
const EXCLUDED = `${P}excluded`;
const EXCLUDED_PK = `${P}excluded_pk`;
const XSCHEMA_USERS = `${P}xschema_users`;
const XSCHEMA_REF = `${P}xschema_ref`;
const XSCHEMA_LOCAL_REF = `${P}xschema_local_ref`;

const OTHER_SCHEMA = `${P}other`;

const ALL_TABLES = [
  MIXED_CHILD,
  MIXED_PARENT,
  COMP_CHILD,
  COMP_CHILD_MANY,
  COMP_PARENT,
  UNIQ_TABLE,
  INVALID_CHILD,
  INVALID_PARENT,
  COVERING_CHILD,
  COVERING_PARENT,
  EXCLUDED,
  EXCLUDED_PK,
  XSCHEMA_REF,
  XSCHEMA_LOCAL_REF,
  XSCHEMA_USERS,
];

let pool: pg.Pool;

async function dropFixtures() {
  for (const t of ALL_TABLES) {
    await pool.query(`DROP TABLE IF EXISTS public."${t}" CASCADE`);
  }
  await pool.query(`DROP SCHEMA IF EXISTS "${OTHER_SCHEMA}" CASCADE`);
}

beforeAll(async () => {
  if (!isAvailable) return;
  pool = createTestPool();
  await dropFixtures();

  // Mixed-case parent/child with a single-column FK.
  await pool.query(`CREATE TABLE "${MIXED_PARENT}" (id text PRIMARY KEY)`);
  await pool.query(
    `CREATE TABLE "${MIXED_CHILD}" (id text PRIMARY KEY, "parentId" text REFERENCES "${MIXED_PARENT}"(id))`
  );

  // Two-column composite FK. comp_child is unique over (pa, pb), comp_child_many is not.
  await pool.query(`CREATE TABLE "${COMP_PARENT}" (a text, b text, PRIMARY KEY (a, b))`);
  await pool.query(
    `CREATE TABLE "${COMP_CHILD}" (pa text, pb text, FOREIGN KEY (pa, pb) REFERENCES "${COMP_PARENT}"(a, b))`
  );
  await pool.query(`CREATE UNIQUE INDEX "${COMP_CHILD}_pa_pb_key" ON "${COMP_CHILD}" (pa, pb)`);
  await pool.query(
    `CREATE TABLE "${COMP_CHILD_MANY}" (pa text, pb text, FOREIGN KEY (pa, pb) REFERENCES "${COMP_PARENT}"(a, b))`
  );

  // Single-column uniqueness expressed as a UNIQUE INDEX (how Prisma emits @unique),
  // not a UNIQUE constraint.
  await pool.query(`CREATE TABLE "${UNIQ_TABLE}" (id text PRIMARY KEY, handle text)`);
  await pool.query(`CREATE UNIQUE INDEX "${UNIQ_TABLE}_handle_key" ON "${UNIQ_TABLE}" (handle)`);

  // An invalid unique index: a failed CREATE UNIQUE INDEX CONCURRENTLY leaves indisunique
  // set on an index that enforces nothing. The table really does hold duplicate fk values.
  await pool.query(`CREATE TABLE "${INVALID_PARENT}" (id int PRIMARY KEY)`);
  await pool.query(
    `CREATE TABLE "${INVALID_CHILD}" (id int PRIMARY KEY, fk int REFERENCES "${INVALID_PARENT}"(id))`
  );
  await pool.query(`INSERT INTO "${INVALID_PARENT}" VALUES (1)`);
  await pool.query(`INSERT INTO "${INVALID_CHILD}" VALUES (1, 1), (2, 1)`);
  await expect(
    pool.query(
      `CREATE UNIQUE INDEX CONCURRENTLY "${INVALID_CHILD}_fk_key" ON "${INVALID_CHILD}" (fk)`
    )
  ).rejects.toThrow();

  // A covering unique index: indnatts = 2 (fk plus the INCLUDE column) but indnkeyatts = 1,
  // so it does guarantee single-column uniqueness.
  await pool.query(`CREATE TABLE "${COVERING_PARENT}" (id int PRIMARY KEY)`);
  await pool.query(
    `CREATE TABLE "${COVERING_CHILD}" (id int PRIMARY KEY, fk int REFERENCES "${COVERING_PARENT}"(id), extra text)`
  );
  await pool.query(
    `CREATE UNIQUE INDEX "${COVERING_CHILD}_fk_key" ON "${COVERING_CHILD}" (fk) INCLUDE (extra)`
  );

  // Index shapes that must never count as single-column uniqueness.
  await pool.query(
    `CREATE TABLE "${EXCLUDED}" (id int PRIMARY KEY, partial_col int, multi_a int, multi_b int, expr_col text, plain_col int, deleted_at timestamptz)`
  );
  await pool.query(
    `CREATE UNIQUE INDEX "${EXCLUDED}_partial" ON "${EXCLUDED}" (partial_col) WHERE deleted_at IS NULL`
  );
  await pool.query(`CREATE UNIQUE INDEX "${EXCLUDED}_multi" ON "${EXCLUDED}" (multi_a, multi_b)`);
  await pool.query(`CREATE UNIQUE INDEX "${EXCLUDED}_expr" ON "${EXCLUDED}" (lower(expr_col))`);
  await pool.query(`CREATE INDEX "${EXCLUDED}_plain" ON "${EXCLUDED}" (plain_col)`);
  await pool.query(`CREATE TABLE "${EXCLUDED_PK}" (a int, b int, PRIMARY KEY (a, b))`);

  // A same-named table in another schema, referenced from public.
  await pool.query(`CREATE SCHEMA "${OTHER_SCHEMA}"`);
  await pool.query(`CREATE TABLE "${OTHER_SCHEMA}"."${XSCHEMA_USERS}" (id int PRIMARY KEY)`);
  await pool.query(`CREATE TABLE public."${XSCHEMA_USERS}" (id int PRIMARY KEY)`);
  await pool.query(
    `CREATE TABLE "${XSCHEMA_REF}" (id int PRIMARY KEY, uid int REFERENCES "${OTHER_SCHEMA}"."${XSCHEMA_USERS}"(id))`
  );
  await pool.query(
    `CREATE TABLE "${XSCHEMA_LOCAL_REF}" (id int PRIMARY KEY, uid int REFERENCES public."${XSCHEMA_USERS}"(id))`
  );
});

afterAll(async () => {
  if (!isAvailable || !pool) return;
  await dropFixtures();
  await pool.end();
});

/** Renders the fixture table exactly as search_objects would. */
async function renderTable(tableName: string) {
  const metadata = await introspectDatabase(pool);
  const schema = mergeSchemas(null, metadata);
  const table = schema.tables.find((t) => t.sqlName === tableName);
  expect(table, `fixture table ${tableName} not found`).toBeDefined();
  return formatSearchResults([table!]);
}

describe.skipIf(!isAvailable)("FK cardinality introspection", () => {
  it("returns FK table names that match the columns query (no leftover identifier quoting)", async () => {
    const metadata = await introspectDatabase(pool);
    const columnTableNames = new Set(metadata.columns.map((c) => c.tableName));

    const fk = metadata.foreignKeys.find(
      (f) => f.fromColumn === "parentId" && f.toColumn === "id"
    );

    // The FK must be discovered at all, and its table names must be join-able with the
    // column list. regclass::text quotes mixed-case names ("MixedChild"), which would make
    // the FK silently un-attachable to its table downstream.
    expect(fk).toBeDefined();
    expect(fk!.fromTable).not.toContain('"');
    expect(fk!.toTable).not.toContain('"');
    expect(columnTableNames.has(fk!.fromTable)).toBe(true);
    expect(columnTableNames.has(fk!.toTable)).toBe(true);
  });

  it("pairs composite FK columns positionally instead of cross-joining them", async () => {
    const metadata = await introspectDatabase(pool);

    const compFks = metadata.foreignKeys.filter(
      (f) => f.fromTable === COMP_CHILD && f.toTable === COMP_PARENT
    );

    // A 2-column FK is two positional pairs (pa->a, pb->b), not the 4-row cartesian product
    // that `attnum = ANY(conkey)` x `attnum = ANY(confkey)` produces.
    expect(compFks).toHaveLength(2);
    const pairs = new Set(compFks.map((f) => `${f.fromColumn}->${f.toColumn}`));
    expect(pairs).toEqual(new Set(["pa->a", "pb->b"]));
  });

  it("treats a single-column UNIQUE INDEX as a uniqueness source for cardinality", async () => {
    const metadata = await introspectDatabase(pool);

    // Prisma @unique creates a unique index, not a pg_constraint. Sourcing uniqueness only
    // from pg_constraint misses these, so every Prisma 1:1 renders as 1:many.
    expect(new Set(metadata.uniqueColumns)).toContain(`${UNIQ_TABLE}.handle`);
  });

  it("ignores an invalid unique index, which enforces nothing", async () => {
    // The index has indisunique = true but indisvalid = false, and the table really does
    // hold two rows with fk = 1. Trusting indisunique alone renders this FK as [1:1].
    const duplicates = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${INVALID_CHILD}" WHERE fk = 1`
    );
    expect(duplicates.rows[0].count).toBe("2");

    const metadata = await introspectDatabase(pool);
    expect(new Set(metadata.uniqueColumns)).not.toContain(`${INVALID_CHILD}.fk`);

    expect(await renderTable(INVALID_PARENT)).toContain(`<- ${INVALID_CHILD} via fk [1:many]`);
  });

  it("counts key columns only, so a covering unique index still means 1:1", async () => {
    const metadata = await introspectDatabase(pool);

    // indnatts = 2 here (fk plus the INCLUDE column) but indnkeyatts = 1. Filtering on
    // indnatts drops a genuine single-column uniqueness guarantee.
    expect(new Set(metadata.uniqueColumns)).toContain(`${COVERING_CHILD}.fk`);

    expect(await renderTable(COVERING_PARENT)).toContain(`<- ${COVERING_CHILD} via fk [1:1]`);
  });

  it("excludes index shapes that do not guarantee single-column uniqueness", async () => {
    const metadata = await introspectDatabase(pool);
    const unique = new Set(metadata.uniqueColumns);

    // Partial: only unique among the rows matching the predicate.
    expect(unique).not.toContain(`${EXCLUDED}.partial_col`);
    // Multi-column: neither column is unique on its own.
    expect(unique).not.toContain(`${EXCLUDED}.multi_a`);
    expect(unique).not.toContain(`${EXCLUDED}.multi_b`);
    // Expression: lower(expr_col) is unique, expr_col is not.
    expect(unique).not.toContain(`${EXCLUDED}.expr_col`);
    // Non-unique index.
    expect(unique).not.toContain(`${EXCLUDED}.plain_col`);
    // First column of a composite PK.
    expect(unique).not.toContain(`${EXCLUDED_PK}.a`);

    // The PK of the same table is genuinely single-column unique, so the query is not
    // simply returning nothing.
    expect(unique).toContain(`${EXCLUDED}.id`);
  });

  it("does not attribute a cross-schema FK to the same-named public table", async () => {
    const metadata = await introspectDatabase(pool);

    const fromTables = metadata.foreignKeys
      .filter((f) => f.toTable === XSCHEMA_USERS)
      .map((f) => f.fromTable);

    // Only relnames are returned, so a FK into another schema's table of the same name is
    // otherwise indistinguishable from one into the public table.
    expect(fromTables).toContain(XSCHEMA_LOCAL_REF);
    expect(fromTables).not.toContain(XSCHEMA_REF);
  });

  it("renders a composite FK as one join path over all its columns", async () => {
    const output = await renderTable(COMP_PARENT);

    // One relationship joined on (pa, pb), not two independent single-column arrows. The
    // unique index over exactly (pa, pb) makes it 1:1; comp_child_many has no such index.
    expect(output).toContain(`<- ${COMP_CHILD} via (pa, pb) [1:1]`);
    expect(output).toContain(`<- ${COMP_CHILD_MANY} via (pa, pb) [1:many]`);
    expect(output).not.toContain(`<- ${COMP_CHILD} via pa`);
    expect(output).not.toContain(`<- ${COMP_CHILD} via pb`);
  });

  it("emits the fan-out warning once for a multi-table result", async () => {
    const metadata = await introspectDatabase(pool);
    const schema = mergeSchemas(null, metadata);
    const tables = searchTables(schema, P);
    expect(tables.length).toBeGreaterThan(2);

    const output = formatSearchResults(tables);
    const warningCount = output.split("will duplicate rows on JOIN").length - 1;
    expect(warningCount).toBe(1);
  });
});
