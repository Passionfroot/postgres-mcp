import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { introspectDatabase } from "../../src/schema/introspect.js";

/**
 * These tests exercise FK cardinality introspection against a real Postgres, because the
 * failure modes are SQL-semantics bugs (identifier quoting, cross-joined array columns,
 * constraint-vs-index uniqueness) that a mocked pg.Pool cannot reproduce. The fixtures use
 * synthetic tables so the assertions target the *category* of bug, not any specific model.
 */

const TEST_DSN = process.env.POSTGRES_MCP_TEST_DSN ?? "postgresql://localhost/postgres";

async function checkDbAvailable() {
  try {
    const testPool = new pg.Pool({ connectionString: TEST_DSN, max: 1 });
    await testPool.query("SELECT 1");
    await testPool.end();
    return true;
  } catch {
    return false;
  }
}

const isDbAvailable = await checkDbAvailable();

// All fixtures share this prefix so setup/teardown can target them without touching the
// rest of the public schema. introspect only reads schema 'public', so they must live there.
const P = "pgmcp_card_";

// Mixed-case identifiers must be double-quoted at creation to preserve their casing.
const MIXED_PARENT = `${P}MixedParent`;
const MIXED_CHILD = `${P}MixedChild`;
const COMP_PARENT = `${P}comp_parent`;
const COMP_CHILD = `${P}comp_child`;
const UNIQ_TABLE = `${P}uniq_index`;

const ALL_TABLES = [MIXED_CHILD, MIXED_PARENT, COMP_CHILD, COMP_PARENT, UNIQ_TABLE];

let pool: pg.Pool;

async function dropFixtures() {
  for (const t of ALL_TABLES) {
    await pool.query(`DROP TABLE IF EXISTS "${t}" CASCADE`);
  }
}

beforeAll(async () => {
  if (!isDbAvailable) return;
  pool = new pg.Pool({ connectionString: TEST_DSN, max: 1 });
  await dropFixtures();

  // Bug 1: mixed-case parent/child with a single-column FK.
  await pool.query(`CREATE TABLE "${MIXED_PARENT}" (id text PRIMARY KEY)`);
  await pool.query(
    `CREATE TABLE "${MIXED_CHILD}" (id text PRIMARY KEY, "parentId" text REFERENCES "${MIXED_PARENT}"(id))`
  );

  // Bug 2: two-column composite FK.
  await pool.query(`CREATE TABLE "${COMP_PARENT}" (a text, b text, PRIMARY KEY (a, b))`);
  await pool.query(
    `CREATE TABLE "${COMP_CHILD}" (a text, b text, FOREIGN KEY (a, b) REFERENCES "${COMP_PARENT}"(a, b))`
  );

  // Bug 3: single-column uniqueness expressed as a UNIQUE INDEX (how Prisma emits @unique),
  // not a UNIQUE constraint.
  await pool.query(`CREATE TABLE "${UNIQ_TABLE}" (id text PRIMARY KEY, handle text)`);
  await pool.query(`CREATE UNIQUE INDEX "${UNIQ_TABLE}_handle_key" ON "${UNIQ_TABLE}" (handle)`);
});

afterAll(async () => {
  if (!isDbAvailable || !pool) return;
  await dropFixtures();
  await pool.end();
});

describe.skipIf(!isDbAvailable)("FK cardinality introspection", () => {
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

    // A 2-column FK is two positional pairs (a->a, b->b), not the 4-row cartesian product
    // that `attnum = ANY(conkey)` × `attnum = ANY(confkey)` produces.
    expect(compFks).toHaveLength(2);
    const pairs = new Set(compFks.map((f) => `${f.fromColumn}->${f.toColumn}`));
    expect(pairs).toEqual(new Set(["a->a", "b->b"]));
  });

  it("treats a single-column UNIQUE INDEX as a uniqueness source for cardinality", async () => {
    const metadata = await introspectDatabase(pool);

    // Prisma @unique creates a unique index, not a pg_constraint. Sourcing uniqueness only
    // from pg_constraint misses these, so every Prisma 1:1 renders as 1:many.
    expect(metadata.uniqueColumns?.has(`${UNIQ_TABLE}.handle`)).toBe(true);
  });
});
