import { describe, expect, it, vi } from "vitest";

import { introspectDatabase } from "../../src/schema/introspect.js";

function createMockPool(queryFn: ReturnType<typeof vi.fn>) {
  const client = {
    query: queryFn,
    release: vi.fn(),
  };
  return {
    query: queryFn,
    connect: vi.fn().mockResolvedValue(client),
    _client: client,
  } as unknown as import("pg").Pool & {
    connect: ReturnType<typeof vi.fn>;
    _client: {
      query: ReturnType<typeof vi.fn>;
      release: ReturnType<typeof vi.fn>;
    };
  };
}

const emptyResult = { rows: [] };

describe("introspectDatabase", () => {
  describe("privilege-filtered queries", () => {
    it("uses has_column_privilege in the columns query", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      const columnsSql = queryFn.mock.calls[0][0] as string;
      expect(columnsSql).toContain("has_column_privilege");
      expect(columnsSql).toContain("'SELECT'");
    });

    it("uses column-level has_column_privilege in the primary keys query", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      const pkSql = queryFn.mock.calls[1][0] as string;
      expect(pkSql).toContain("has_column_privilege");
      // has_table_privilege ignores column-level grants and must not be used here
      expect(pkSql).not.toContain("has_table_privilege");
      expect(pkSql).toContain("'SELECT'");
    });

    it("uses column-level has_column_privilege in the foreign keys query", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      const fkSql = queryFn.mock.calls[2][0] as string;
      expect(fkSql).toContain("has_column_privilege");
      expect(fkSql).not.toContain("has_table_privilege");
      expect(fkSql).toContain("'SELECT'");
    });

    it("checks privilege on both the referencing and the referenced column", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      const fkSql = queryFn.mock.calls[2][0] as string;
      // A role with SELECT only on the referencing side would otherwise report an FK into a
      // table it cannot see at all, instead of omitting it.
      expect(fkSql).toContain(
        "has_column_privilege(con.conrelid, a.attnum, 'SELECT')"
      );
      expect(fkSql).toContain(
        "has_column_privilege(con.confrelid, af.attnum, 'SELECT')"
      );
    });

    it("orders composite FK rows by constraint and declared position, not by column name", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      const fkSql = queryFn.mock.calls[2][0] as string;
      // Sorting by from_column instead of cols.ord scrambles a composite FK's column order
      // relative to the parent table's own column order.
      expect(fkSql).toContain("ORDER BY from_table, con.conname, cols.ord");
      expect(fkSql).not.toContain("ORDER BY from_table, from_column, cols.ord");
    });

    it("constrains both sides of a foreign key to the public schema", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      const fkSql = queryFn.mock.calls[2][0] as string;
      // Only relnames are returned, so leaving the referenced side unfiltered attributes a
      // cross-schema FK to the same-named public table.
      expect(fkSql).toContain(
        "JOIN pg_namespace fn ON fn.oid = frel.relnamespace"
      );
      expect(fkSql).toContain("fn.nspname = 'public'");
      // Constraint identity keeps composite FK columns grouped as one relationship.
      expect(fkSql).toContain("con.conname AS constraint_name");
    });

    it("only counts unique indexes that actually enforce uniqueness", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      const uniqueSql = queryFn.mock.calls[4][0] as string;
      // A failed CREATE UNIQUE INDEX CONCURRENTLY leaves indisunique set on an index that
      // enforces nothing.
      expect(uniqueSql).toContain("idx.indisvalid");
      expect(uniqueSql).toContain("idx.indislive");
      // indnatts also counts INCLUDE columns, which do not widen the guarantee.
      expect(uniqueSql).toContain("idx.indnkeyatts");
      expect(uniqueSql).not.toContain("indnatts");
      expect(uniqueSql).toContain("idx.indpred IS NULL");
    });

    it("does not use privilege filters in the enum values query", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      const enumSql = queryFn.mock.calls[3][0] as string;
      expect(enumSql).not.toContain("has_column_privilege");
      expect(enumSql).not.toContain("has_table_privilege");
      expect(enumSql).toContain("pg_enum");
    });
  });

  describe("basic introspection (no role/sessionVars)", () => {
    it("runs the 5 queries on a single acquired client, not pool.query", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      // Acquiring one client instead of calling pool.query() 5 times avoids queuing 4 of the 5
      // queries behind a fresh connection-acquire wait at the default pool_max of 1.
      expect(pool.connect).toHaveBeenCalledTimes(1);
      expect(queryFn).toHaveBeenCalledTimes(5);
      expect(pool._client.release).toHaveBeenCalledTimes(1);
    });

    it("maps column rows to DbColumn objects", async () => {
      const queryFn = vi
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              table_name: "users",
              column_name: "id",
              data_type: "text",
              udt_name: "text",
              is_nullable: "NO",
              column_default: null,
              ordinal_position: 1,
            },
            {
              table_name: "users",
              column_name: "email",
              data_type: "text",
              udt_name: "text",
              is_nullable: "YES",
              column_default: null,
              ordinal_position: 2,
            },
          ],
        })
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce(emptyResult);

      const pool = createMockPool(queryFn);
      const result = await introspectDatabase(pool);

      expect(result.columns).toHaveLength(2);
      expect(result.columns[0]).toEqual({
        tableName: "users",
        columnName: "id",
        dataType: "text",
        udtName: "text",
        isNullable: false,
        columnDefault: null,
        ordinalPosition: 1,
      });
      expect(result.columns[1].isNullable).toBe(true);
    });

    it("maps primary key rows", async () => {
      const queryFn = vi
        .fn()
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce({
          rows: [
            { table_name: "users", column_name: "id", ordinal_position: 1 },
          ],
        })
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce(emptyResult);

      const pool = createMockPool(queryFn);
      const result = await introspectDatabase(pool);

      expect(result.primaryKeys).toHaveLength(1);
      expect(result.primaryKeys[0]).toEqual({
        tableName: "users",
        columnName: "id",
        ordinalPosition: 1,
      });
    });

    it("maps foreign key rows", async () => {
      const queryFn = vi
        .fn()
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce({
          rows: [
            {
              constraint_name: "orders_userId_fkey",
              from_table: "orders",
              from_column: "userId",
              to_table: "users",
              to_column: "id",
            },
          ],
        })
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce(emptyResult);

      const pool = createMockPool(queryFn);
      const result = await introspectDatabase(pool);

      expect(result.foreignKeys).toHaveLength(1);
      expect(result.foreignKeys[0]).toEqual({
        constraintName: "orders_userId_fkey",
        fromTable: "orders",
        fromColumn: "userId",
        toTable: "users",
        toColumn: "id",
      });
    });

    it("maps enum value rows", async () => {
      const queryFn = vi
        .fn()
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce(emptyResult)
        .mockResolvedValueOnce({
          rows: [
            { enum_name: "Status", enum_value: "ACTIVE", sort_order: 1 },
            { enum_name: "Status", enum_value: "INACTIVE", sort_order: 2 },
          ],
        })
        .mockResolvedValueOnce(emptyResult);

      const pool = createMockPool(queryFn);
      const result = await introspectDatabase(pool);

      expect(result.enumValues).toHaveLength(2);
      expect(result.enumValues[0]).toEqual({
        enumName: "Status",
        enumValue: "ACTIVE",
        sortOrder: 1,
      });
    });
  });

  describe("with role option", () => {
    it("uses a client connection and sets role before introspecting", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool, { role: "app_readonly" });

      // Should use pool.connect for session-scoped SET ROLE
      expect(pool.connect).toHaveBeenCalledTimes(1);

      // First call: SET ROLE, then 5 introspection queries, then RESET ROLE
      expect(queryFn.mock.calls[0][0]).toBe('SET ROLE "app_readonly"');

      // Last cleanup call
      const lastCall = queryFn.mock.calls[
        queryFn.mock.calls.length - 1
      ][0] as string;
      expect(lastCall).toBe("RESET ROLE");
    });

    it("releases the client after successful introspection", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool, { role: "app_readonly" });

      expect(pool._client.release).toHaveBeenCalledTimes(1);
      // Falsy first argument, so pg-pool keeps the connection for reuse.
      expect(pool._client.release.mock.calls[0][0]).toBeFalsy();
    });

    it("releases the client even when introspection fails", async () => {
      const queryFn = vi
        .fn()
        .mockResolvedValueOnce(emptyResult) // SET ROLE succeeds
        .mockRejectedValueOnce(new Error("query failed")); // first introspection query fails

      const pool = createMockPool(queryFn);

      await expect(
        introspectDatabase(pool, { role: "app_readonly" })
      ).rejects.toThrow("query failed");
      expect(pool._client.release).toHaveBeenCalledTimes(1);
    });
  });

  describe("connection discard on failed cleanup (RLS safety)", () => {
    // introspectWithSession runs under the same SET ROLE / SET var / RESET pattern executeQuery
    // uses (see ../query.test.ts). A connection that failed to unwind its role or session vars
    // must not go back into the pool clean -- at the default pool_max of 1 the very next
    // introspection or query call would acquire it and inherit this call's role / app.partner_id.

    it("discards the connection when resetting session state fails", async () => {
      const queryFn = vi.fn().mockImplementation((sql: string) => {
        if (sql.startsWith("RESET"))
          return Promise.reject(new Error("connection lost"));
        return Promise.resolve(emptyResult);
      });
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool, {
        role: "zest_mcp_reader",
        sessionVars: { "app.partner_id": "p1" },
      });

      expect(pool._client.release).toHaveBeenCalledTimes(1);
      expect(pool._client.release.mock.calls[0][0]).toBeInstanceOf(Error);
    });

    it("discards the connection when the client-side query timeout fires", async () => {
      const queryFn = vi.fn().mockImplementation((sql: string) => {
        if (/information_schema|pg_type/.test(sql)) {
          return Promise.reject(new Error("Query read timeout"));
        }
        return Promise.resolve(emptyResult);
      });
      const pool = createMockPool(queryFn);

      await expect(
        introspectDatabase(pool, {
          role: "zest_mcp_reader",
          sessionVars: { "app.partner_id": "p1" },
        })
      ).rejects.toThrow("Query read timeout");

      expect(pool._client.release).toHaveBeenCalledTimes(1);
      expect(pool._client.release.mock.calls[0][0]).toBeInstanceOf(Error);
    });

    it("still releases clean when a plain query error is not a reset failure or timeout", async () => {
      const queryFn = vi
        .fn()
        .mockResolvedValueOnce(emptyResult) // SET ROLE
        .mockRejectedValueOnce(new Error("relation does not exist")); // first introspection query

      const pool = createMockPool(queryFn);

      await expect(
        introspectDatabase(pool, { role: "zest_mcp_reader" })
      ).rejects.toThrow("relation does not exist");

      expect(pool._client.release).toHaveBeenCalledTimes(1);
      expect(pool._client.release.mock.calls[0][0]).toBeFalsy();
    });
  });

  describe("with sessionVars option", () => {
    it("sets session variables before introspecting and resets after", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool, {
        sessionVars: { "app.tenant_id": "t_123" },
      });

      expect(pool.connect).toHaveBeenCalledTimes(1);

      // First call: SET session var
      expect(queryFn.mock.calls[0][0]).toBe("SET app.tenant_id = 't_123'");

      // Last cleanup call: RESET session var
      const lastCall = queryFn.mock.calls[
        queryFn.mock.calls.length - 1
      ][0] as string;
      expect(lastCall).toBe("RESET app.tenant_id");
    });

    it("rejects unsafe GUC names", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await expect(
        introspectDatabase(pool, {
          sessionVars: { "'; DROP TABLE users; --": "oops" },
        })
      ).rejects.toThrow("Invalid session variable name");
    });
  });

  describe("with role AND sessionVars", () => {
    it("sets role first, then session vars, and resets in reverse order", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool, {
        role: "rls_user",
        sessionVars: { "app.partner_id": "p_456" },
      });

      // Setup: SET ROLE, SET session var
      expect(queryFn.mock.calls[0][0]).toBe('SET ROLE "rls_user"');
      expect(queryFn.mock.calls[1][0]).toBe("SET app.partner_id = 'p_456'");

      // Teardown: RESET session var, RESET ROLE (reverse order)
      const calls = queryFn.mock.calls.map((c) => c[0] as string);
      const resetVarIdx = calls.lastIndexOf("RESET app.partner_id");
      const resetRoleIdx = calls.lastIndexOf("RESET ROLE");
      expect(resetVarIdx).toBeLessThan(resetRoleIdx);
    });
  });

  describe("without options (no role, no sessionVars)", () => {
    it("acquires a client but sends no SET ROLE / RESET traffic", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool);

      expect(pool.connect).toHaveBeenCalledTimes(1);

      const calls = queryFn.mock.calls.map((c) => c[0] as string);
      expect(calls.every((sql) => !sql.startsWith("SET ROLE"))).toBe(true);
      expect(calls.every((sql) => !sql.startsWith("RESET"))).toBe(true);
    });

    it("also acquires a client with empty options", async () => {
      const queryFn = vi.fn().mockResolvedValue(emptyResult);
      const pool = createMockPool(queryFn);

      await introspectDatabase(pool, {});

      expect(pool.connect).toHaveBeenCalledTimes(1);
    });
  });
});
