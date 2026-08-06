import path from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { Config, SourceConfig } from "../../src/types.js";

import { createAuditLog } from "../../src/audit-log.js";
import { ConnectionManager } from "../../src/connections.js";
import { createSchemaCache } from "../../src/schema/cache.js";
import { createServer } from "../../src/server.js";

const TEST_DSN = process.env.POSTGRES_MCP_TEST_DSN ?? "postgresql://localhost/postgres";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const prismaFixturePath = path.resolve(__dirname, "../fixtures/integration.prisma");

const localSource: SourceConfig = {
  id: "local",
  dsn: TEST_DSN,
  readonly: false,
  maxRows: 100,
  timeout: 10,
  poolMax: 1,
  poolMaxExplicit: false,
  maxResponseBytes: 1_000_000,
  allowMultiStatements: false,
  readOnlyQueries: false,
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

// Throwaway tables so the assertions do not depend on whatever else the test database holds.
const SETUP_SQL = `
  CREATE TABLE IF NOT EXISTS pgmcp_users (id text PRIMARY KEY, email text NOT NULL);
  CREATE TABLE IF NOT EXISTS pgmcp_posts (
    id text PRIMARY KEY,
    title text NOT NULL,
    author_id text NOT NULL REFERENCES pgmcp_users(id)
  );
`;
const TEARDOWN_SQL = `DROP TABLE IF EXISTS pgmcp_posts; DROP TABLE IF EXISTS pgmcp_users;`;

/** Everything about the server that a language model ever reads. */
interface Surfaces {
  tools: string;
  resourceTemplates: string;
  schemaBody: string;
  searchResults: string[];
  tableNames: string[];
}

const connectionManagers: ConnectionManager[] = [];

async function collectSurfaces(config: Config): Promise<Surfaces> {
  const connectionManager = new ConnectionManager([localSource]);
  connectionManagers.push(connectionManager);

  const schemaCache = await createSchemaCache(config);
  const server = createServer(config, connectionManager, schemaCache, createAuditLog(undefined));

  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  const client = new Client({ name: "surface-test", version: "1.0.0" });
  await Promise.all([client.connect(clientTransport), server.connect(serverTransport)]);

  const tools = await client.listTools();
  const resourceTemplates = await client.listResourceTemplates();
  const schemaRes = await client.readResource({ uri: "schema://local" });
  const content = schemaRes.contents[0];
  if (!("text" in content)) throw new Error("schema:// returned a non-text resource");
  const schemaBody = String(content.text);

  const pool = await connectionManager.getPool("local");
  const schema = await schemaCache.get("local", pool);
  const tableNames = schema.tables.map((t) => t.sqlName);

  const searchResults: string[] = [];
  for (const pattern of [...tableNames, "pgmcp", "user", "post"]) {
    const res = await client.callTool({
      name: "search_objects",
      arguments: { database: "local", pattern },
    });
    searchResults.push(JSON.stringify(res.content));
  }

  await client.close();
  await server.close();

  return {
    tools: JSON.stringify(tools),
    resourceTemplates: JSON.stringify(resourceTemplates),
    schemaBody,
    searchResults,
    tableNames,
  };
}

let setupPool: pg.Pool;

beforeAll(async () => {
  if (!isDbAvailable) return;
  setupPool = new pg.Pool({ connectionString: TEST_DSN, max: 1 });
  await setupPool.query(SETUP_SQL);
});

afterAll(async () => {
  await Promise.all(connectionManagers.map((cm) => cm.shutdown()));
  if (setupPool) {
    await setupPool.query(TEARDOWN_SQL);
    await setupPool.end();
  }
});

describe.skipIf(!isDbAvailable)("MCP surface with no Prisma mapping loaded", () => {
  let surfaces: Surfaces;

  beforeAll(async () => {
    surfaces = await collectSurfaces({ sources: [localSource] });
  });

  it("mentions Prisma nowhere a language model can read it", () => {
    // A bare /prisma/i sweep false-fails on a database's own vocabulary (a table or column that
    // happens to contain "prisma", e.g. `_prisma_migrations`). Check for the actual annotation
    // markers the server emits when a mapping is loaded (search-tool.ts, resource.ts, search.ts,
    // format.ts) instead.
    const prismaMarkers = [
      "Prisma model name",
      "Prisma model names",
      "(Prisma:",
      "no Prisma model",
    ];

    const everything = [
      surfaces.tools,
      surfaces.resourceTemplates,
      surfaces.schemaBody,
      ...surfaces.searchResults,
    ].join("\n");

    for (const marker of prismaMarkers) {
      expect(everything).not.toContain(marker);
    }
  });

  it("serves a schema:// body that lists the database's tables", () => {
    expect(surfaces.tableNames).toContain("pgmcp_users");
    expect(surfaces.tableNames).toContain("pgmcp_posts");
    for (const name of surfaces.tableNames) {
      expect(surfaces.schemaBody).toContain(name);
    }
    expect(surfaces.schemaBody).toContain("  -> pgmcp_users.id");
  });

  it("serves a schema:// header whose table count matches the body", () => {
    const header = surfaces.schemaBody.split("\n")[0];
    expect(header).toContain(`(${surfaces.tableNames.length} tables,`);
  });
});

describe.skipIf(!isDbAvailable)("MCP surface with a Prisma mapping loaded", () => {
  let surfaces: Surfaces;

  beforeAll(async () => {
    surfaces = await collectSurfaces({
      sources: [localSource],
      prismaSchemaPath: prismaFixturePath,
    });
  });

  it("keeps the Prisma wording in the tool and resource descriptions", () => {
    expect(surfaces.tools).toContain("Prisma model name");
    expect(surfaces.resourceTemplates).toContain("Prisma model names");
  });

  it("keeps the (Prisma: Model) annotations in search_objects output", () => {
    const searched = surfaces.searchResults.join("\n");
    expect(searched).toContain("pgmcp_users (Prisma: PgmcpUser)");
    expect(searched).toContain("(Prisma: authorId)");
  });

  it("keeps the schema:// map filtered to Prisma-mapped tables", () => {
    expect(surfaces.schemaBody).toContain("pgmcp_users (Prisma: PgmcpUser)");
    expect(surfaces.schemaBody).toContain("pgmcp_posts (Prisma: PgmcpPost)");
    expect(surfaces.schemaBody).toContain("# Schema: local (2 tables, 1 FK relationships)");
  });
});
