import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { executeQuery, ReadOnlyQueryError } from "../../src/query.js";

// No default: an unset DSN skips the suite rather than pointing at whatever Postgres
// happens to be listening on localhost.
const TEST_DSN = process.env.POSTGRES_MCP_TEST_DSN;

// Two tenants behind FORCE ROW LEVEL SECURITY. `t` uses a scalar policy, `messages`
// uses an EXISTS-subquery policy (the shape the real messages table uses), because the
// two plan differently and a leak can show up in one and not the other.
const FIXTURE = `
DROP TABLE IF EXISTS rls_messages, rls_threads, rls_t CASCADE;
DO $fixture$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_tenant_reader') THEN
    EXECUTE 'DROP OWNED BY rls_tenant_reader';
    EXECUTE 'DROP ROLE rls_tenant_reader';
  END IF;
END $fixture$;
CREATE ROLE rls_tenant_reader NOLOGIN;

CREATE TABLE rls_t (id serial primary key, tenant_id text not null, secret text not null);
INSERT INTO rls_t (tenant_id, secret) VALUES ('tenantA','A-secret'), ('tenantB','B-secret');
ALTER TABLE rls_t ENABLE ROW LEVEL SECURITY;
ALTER TABLE rls_t FORCE ROW LEVEL SECURITY;
CREATE POLICY rls_t_scope ON rls_t USING (tenant_id = current_setting('app.partner_id', true));

CREATE TABLE rls_threads (id serial primary key, tenant_id text not null);
INSERT INTO rls_threads (tenant_id) VALUES ('tenantA'), ('tenantB');
CREATE TABLE rls_messages (id serial primary key, thread_id int not null references rls_threads(id), body text not null);
INSERT INTO rls_messages (thread_id, body) VALUES (1,'A-message'), (2,'B-message');
ALTER TABLE rls_threads ENABLE ROW LEVEL SECURITY;
ALTER TABLE rls_threads FORCE ROW LEVEL SECURITY;
CREATE POLICY rls_threads_scope ON rls_threads USING (tenant_id = current_setting('app.partner_id', true));
ALTER TABLE rls_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE rls_messages FORCE ROW LEVEL SECURITY;
CREATE POLICY rls_messages_scope ON rls_messages USING (
  EXISTS (SELECT 1 FROM rls_threads th WHERE th.id = rls_messages.thread_id
          AND th.tenant_id = current_setting('app.partner_id', true))
);

GRANT USAGE ON SCHEMA public TO rls_tenant_reader;
GRANT SELECT ON rls_t, rls_threads, rls_messages TO rls_tenant_reader;

-- A column whose name literally ends in a backslash, so the identifier half of a
-- smuggling payload resolves instead of aborting the batch at 42703.
DROP TABLE IF EXISTS backstop_src;
CREATE TABLE backstop_src ("x\\" int);
INSERT INTO backstop_src VALUES (7);
GRANT SELECT ON backstop_src TO rls_tenant_reader;
`;

async function checkDbAvailable() {
  if (!TEST_DSN) return false;
  try {
    const probe = new pg.Pool({ connectionString: TEST_DSN, max: 1 });
    await probe.query("SELECT 1");
    await probe.end();
    return true;
  } catch {
    return false;
  }
}

const isDbAvailable = await checkDbAvailable();

let pool: pg.Pool;

const tenantAOptions = {
  readonly: false,
  allowMultiStatements: false,
  readOnlyQueries: true,
  role: "rls_tenant_reader",
  sessionVars: { "app.partner_id": "tenantA" },
};

beforeAll(async () => {
  if (!isDbAvailable) return;
  pool = new pg.Pool({ connectionString: TEST_DSN, max: 2 });
  await pool.query(FIXTURE);
});

afterAll(async () => {
  if (pool) await pool.end();
});

describe.skipIf(!isDbAvailable)("read-only guard against a two-tenant RLS fixture", () => {
  it("scopes a plain SELECT to tenant A (scalar policy)", async () => {
    const result = await executeQuery(pool, "SELECT * FROM rls_t", 10, tenantAOptions);

    expect(result.rows).toEqual([{ id: 1, tenant_id: "tenantA", secret: "A-secret" }]);
  });

  it("scopes a plain SELECT to tenant A (EXISTS-subquery policy)", async () => {
    const result = await executeQuery(pool, "SELECT * FROM rls_messages", 10, tenantAOptions);

    expect(result.rows).toEqual([{ id: 1, thread_id: 1, body: "A-message" }]);
  });

  it("allows EXPLAIN without ANALYZE through to the server", async () => {
    const result = await executeQuery(
      pool,
      "EXPLAIN SELECT * FROM rls_t",
      10,
      tenantAOptions
    );

    expect(result.rows.length).toBeGreaterThan(0);
    expect(JSON.stringify(result.rows)).toContain("rls_t");
  });

  // Every payload proven to re-point tenant scope on this fixture shape. The guard has
  // to reject each one before it reaches the connection, and no tenant B value may
  // appear in anything the caller gets back.
  const payloads: Array<[string, string]> = [
    [
      "set_config in WHERE",
      "SELECT * FROM rls_t WHERE set_config('app.partner_id','tenantB',false) IS NOT NULL",
    ],
    [
      "set_config in a materialized CTE",
      "WITH x AS MATERIALIZED (SELECT set_config('app.partner_id','tenantB',false)) SELECT * FROM rls_t CROSS JOIN x",
    ],
    [
      "set_config in the select list",
      "SELECT set_config('app.partner_id','tenantB',false), rls_t.* FROM rls_t",
    ],
    [
      "SET ROLE smuggled past a backslash-terminated literal",
      String.raw`SELECT 'x\'; SET ROLE postgres; SELECT * FROM rls_t; --'`,
    ],
    [
      "SET of the tenant GUC smuggled past a backslash-terminated literal",
      String.raw`SELECT 'x\'; SET app.partner_id TO "tenantB"; SELECT * FROM rls_t; --'`,
    ],
    [
      "SET of the tenant GUC against the EXISTS policy",
      String.raw`SELECT 'x\'; SET app.partner_id TO "tenantB"; SELECT * FROM rls_messages; --'`,
    ],
    [
      "SET of the tenant GUC smuggled past a backslash-terminated identifier",
      String.raw`SELECT "x\"; SET app.partner_id TO 'tenantB'; SELECT * FROM rls_t; --"`,
    ],
    [
      "SET ROLE smuggled past a backslash-terminated identifier",
      String.raw`SELECT 1 AS "x\"; SET ROLE postgres; SELECT * FROM rls_t; --"`,
    ],
    [
      "identifier bypass with an odd backslash run of three",
      String.raw`SELECT "x\\\"; SET app.partner_id TO 'tenantB'; SELECT * FROM rls_t; --"`,
    ],
  ];

  it.each(payloads)("rejects %s", async (_label, sql) => {
    await expect(executeQuery(pool, sql, 10, tenantAOptions)).rejects.toThrow(
      ReadOnlyQueryError
    );
  });

  it("leaves tenant scope intact after a rejected attempt", async () => {
    for (const [, sql] of payloads) {
      await executeQuery(pool, sql, 10, tenantAOptions).catch(() => undefined);
    }

    const after = await executeQuery(pool, "SELECT * FROM rls_t", 10, tenantAOptions);
    expect(JSON.stringify(after.rows)).not.toContain("B-secret");
  });

  // Guards the premise. If Postgres ever stopped executing the smuggled statements,
  // the tests above would pass for the wrong reason.
  it("confirms Postgres really does execute the smuggled statements", async () => {
    const client = await pool.connect();
    try {
      await client.query("SET ROLE rls_tenant_reader");
      await client.query("SET app.partner_id = 'tenantA'");

      const scoped = await client.query("SELECT * FROM rls_t");
      expect(scoped.rows).toEqual([{ id: 1, tenant_id: "tenantA", secret: "A-secret" }]);

      const smuggled = await client.query(
        String.raw`SELECT 'x\'; SET app.partner_id TO "tenantB"; SELECT * FROM rls_t; --'`
      );
      const results = Array.isArray(smuggled) ? smuggled : [smuggled];

      expect(results).toHaveLength(3);
      expect(JSON.stringify(results.map((r) => r.rows))).toContain("B-secret");
    } finally {
      await client.query("RESET ALL").catch(() => undefined);
      await client.query("RESET ROLE").catch(() => undefined);
      client.release();
    }
  });

  // Guards the premise for the identifier half. Same check as above, one lexer branch over.
  it("confirms Postgres executes statements smuggled past a quoted identifier", async () => {
    const client = await pool.connect();
    try {
      await client.query("SET ROLE rls_tenant_reader");
      await client.query("SET app.partner_id = 'tenantA'");

      // Reads backstop_src so the first statement resolves against a real `x\` column;
      // without it the batch aborts at 42703 before reaching the smuggled SET.
      const smuggled = await client.query(
        String.raw`SELECT "x\" FROM backstop_src; SET app.partner_id TO 'tenantB'; SELECT * FROM rls_t; --"`
      );
      const results = Array.isArray(smuggled) ? smuggled : [smuggled];

      expect(results).toHaveLength(3);
      expect(JSON.stringify(results.map((r) => r.rows))).toContain("B-secret");
    } finally {
      await client.query("RESET ALL").catch(() => undefined);
      await client.query("RESET ROLE").catch(() => undefined);
      client.release();
    }
  });
});
