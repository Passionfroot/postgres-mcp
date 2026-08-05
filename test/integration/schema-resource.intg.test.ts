import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { Config, SourceConfig } from "../../src/types.js";

import { ConnectionManager } from "../../src/connections.js";
import { createSchemaCache } from "../../src/schema/cache.js";
import { formatRelationshipMap } from "../../src/schema/format.js";
import { introspectDatabase } from "../../src/schema/introspect.js";
import { mergeSchemas } from "../../src/schema/merge.js";
import { parsePrismaFiles } from "../../src/schema/prisma-parser.js";
import { searchTables } from "../../src/schema/search.js";

const TEST_DSN =
  process.env.POSTGRES_MCP_TEST_DSN ?? "postgresql://localhost/postgres";

const localSource: SourceConfig = {
  id: "local",
  dsn: TEST_DSN,
  readonly: false,
  maxRows: 10,
  timeout: 5,
  poolMax: 1,
  poolMaxExplicit: false,
  maxResponseBytes: 1_000_000,
  allowMultiStatements: false,
};

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

let connectionManager: ConnectionManager;

beforeAll(() => {
  if (isDbAvailable) {
    connectionManager = new ConnectionManager([localSource]);
  }
});

afterAll(async () => {
  if (connectionManager) {
    await connectionManager.shutdown();
  }
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const samplePrismaPath = path.resolve(__dirname, "../fixtures/sample.prisma");

describe.skipIf(!isDbAvailable)("schema introspection integration", () => {
  it("returns columns for public schema", async () => {
    const pool = await connectionManager.getPool("local");
    const metadata = await introspectDatabase(pool);

    expect(metadata.columns.length).toBeGreaterThan(0);
    expect(metadata.columns[0]).toHaveProperty("tableName");
    expect(metadata.columns[0]).toHaveProperty("columnName");
    expect(metadata.columns[0]).toHaveProperty("dataType");
  });

  it("returns primary keys", async () => {
    const pool = await connectionManager.getPool("local");
    const metadata = await introspectDatabase(pool);

    expect(metadata.primaryKeys.length).toBeGreaterThan(0);
    expect(metadata.primaryKeys[0]).toHaveProperty("tableName");
    expect(metadata.primaryKeys[0]).toHaveProperty("columnName");
  });
});

describe.skipIf(!isDbAvailable)("full schema pipeline integration", () => {
  it("introspect + merge + format produces valid relationship map", async () => {
    const pool = await connectionManager.getPool("local");

    const prismaMapping = parsePrismaFiles([samplePrismaPath]);
    const dbMetadata = await introspectDatabase(pool);
    const merged = mergeSchemas(prismaMapping, dbMetadata);
    const output = formatRelationshipMap(merged, "local");

    expect(output).toContain("# Schema: local");
  });

  it("MergedSchema preserves full column detail", async () => {
    const pool = await connectionManager.getPool("local");

    const dbMetadata = await introspectDatabase(pool);
    // An empty mapping is what createSchemaCache builds when no prisma_schema_path is
    // configured, which is every production source. mergeSchemas takes a PrismaMapping,
    // never null, so passing null here never matched the code under test.
    const merged = mergeSchemas({ models: [], enums: [] }, dbMetadata);

    const tableWithColumns = merged.tables.find((t) => t.columns.length > 1);
    expect(tableWithColumns).toBeDefined();

    const col = tableWithColumns!.columns[0];
    expect(col).toHaveProperty("dataType");
    expect(col).toHaveProperty("isNullable");
    expect(col).toHaveProperty("columnDefault");
    expect(col.dataType).toBeTruthy();
  });
});

describe.skipIf(!isDbAvailable)("createSchemaCache integration", () => {
  it("produces Prisma-annotated schema when prismaSchemaPath is configured", async () => {
    const config: Config = {
      sources: [localSource],
      prismaSchemaPath: samplePrismaPath,
    };

    const cache = await createSchemaCache(config);
    const pool = await connectionManager.getPool("local");
    const schema = await cache.get("local", pool);

    // The sample schema models won't match the test DB tables,
    // so Prisma models will show as unmapped — that's expected.
    // The important thing is the pipeline doesn't crash.
    expect(schema.tables.length).toBeGreaterThan(0);
  });

  it("returns schema when prismaSchemaPath is omitted", async () => {
    const config: Config = {
      sources: [localSource],
    };

    const cache = await createSchemaCache(config);
    const pool = await connectionManager.getPool("local");
    const schema = await cache.get("local", pool);

    expect(schema.tables.length).toBeGreaterThan(0);
    expect(schema.tables.every((t) => t.prismaModelName === null)).toBe(true);

    // Without a mapping the map lists every database table. Filtering to Prisma-mapped tables
    // here would leave the resource empty, which is what it used to serve.
    expect(cache.hasPrismaMapping).toBe(false);
    const output = formatRelationshipMap(schema, "local", {
      hasPrismaMapping: false,
    });
    expect(output).toContain(`${schema.tables.length} tables`);
    for (const table of schema.tables) {
      expect(output).toContain(table.sqlName);
    }
  });

  it("search_objects still finds tables when prismaSchemaPath is omitted", async () => {
    const config: Config = {
      sources: [localSource],
    };

    const cache = await createSchemaCache(config);
    const pool = await connectionManager.getPool("local");
    const schema = await cache.get("local", pool);

    // Search for any table that exists in the DB (information_schema guarantees pg_catalog tables)
    const allTables = schema.tables.map((t) => t.sqlName);
    expect(allTables.length).toBeGreaterThan(0);

    const results = searchTables(schema, allTables[0]);
    expect(results.length).toBeGreaterThan(0);
  });
});
