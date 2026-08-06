import pg from "pg";

/**
 * Integration tests only ever talk to the DSN in POSTGRES_MCP_TEST_DSN. There is deliberately
 * no default: these tests create and drop fixture tables, and a default like
 * "postgresql://localhost/postgres" silently targets a developer's local database.
 */
export const TEST_DSN = process.env.POSTGRES_MCP_TEST_DSN;

/**
 * A missing DSN is only a valid reason to skip locally. If a DSN was configured, or we are on
 * CI, an unreachable database is a failure -- otherwise a broken service container turns into
 * a green run with zero assertions.
 */
const isDbRequired = Boolean(TEST_DSN) || process.env.CI === "true";

export async function resolveTestDb(): Promise<{ isAvailable: boolean }> {
  if (!TEST_DSN) {
    if (isDbRequired) {
      throw new Error(
        "POSTGRES_MCP_TEST_DSN is not set. Integration tests require an explicit test database DSN on CI."
      );
    }
    return { isAvailable: false };
  }

  try {
    const pool = new pg.Pool({ connectionString: TEST_DSN, max: 1 });
    await pool.query("SELECT 1");
    await pool.end();
    return { isAvailable: true };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(
      `POSTGRES_MCP_TEST_DSN is set but the database is not reachable: ${message}`
    );
  }
}

/** Opens a pool against the test DSN. Only call this when resolveTestDb reported available. */
export function createTestPool() {
  if (!TEST_DSN) throw new Error("POSTGRES_MCP_TEST_DSN is not set");
  return new pg.Pool({ connectionString: TEST_DSN, max: 1 });
}
