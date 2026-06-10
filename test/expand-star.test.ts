import { describe, expect, it } from "vitest";

import { expandStarColumns } from "../src/expand-star.js";
import type { MergedSchema, MergedTable } from "../src/schema/types.js";

function makeTable(
  sqlName: string,
  columns: string[],
  prismaModelName: string | null = null
): MergedTable {
  return {
    sqlName,
    prismaModelName,
    columns: columns.map((c) => ({
      sqlName: c,
      prismaFieldName: null,
      dataType: "text",
      udtName: "text",
      isNullable: false,
      columnDefault: null,
      isPrimaryKey: c === "id",
    })),
    primaryKeys: ["id"],
    incomingFks: [],
    outgoingFks: [],
    driftWarnings: [],
  };
}

function makeSchema(tables: MergedTable[]): MergedSchema {
  return {
    tables,
    unmappedTables: [],
    driftWarnings: [],
  };
}

const creatorsColumns = ["id", "displayName", "currency", "country"];
const conversationsColumns = ["id", "creatorId", "partnerId", "lastActivity"];

const schema = makeSchema([
  makeTable("creators", creatorsColumns, "Creator"),
  makeTable("conversations", conversationsColumns, "Conversation"),
]);

describe("expandStarColumns", () => {
  describe("single table SELECT *", () => {
    it("expands SELECT * FROM creators to explicit columns", () => {
      const result = expandStarColumns("SELECT * FROM creators", schema);
      for (const col of creatorsColumns) {
        expect(result).toContain(`"${col}"`);
      }
      expect(result).not.toContain("*");
    });

    it("preserves WHERE clause when expanding", () => {
      const result = expandStarColumns(
        "SELECT * FROM creators WHERE id = 'abc'",
        schema
      );
      expect(result).toContain("WHERE");
      expect(result).toContain("'abc'");
      expect(result).not.toContain("*");
    });

    it("preserves ORDER BY and LIMIT", () => {
      const result = expandStarColumns(
        'SELECT * FROM creators ORDER BY "displayName" LIMIT 10',
        schema
      );
      expect(result).toMatch(/ORDER BY/i);
      expect(result).toMatch(/LIMIT 10/i);
      expect(result).not.toContain("*");
    });
  });

  describe("aliased table SELECT t.*", () => {
    it("expands SELECT c.* FROM creators c", () => {
      const result = expandStarColumns(
        "SELECT c.* FROM creators c",
        schema
      );
      for (const col of creatorsColumns) {
        expect(result).toContain(`"${col}"`);
      }
      expect(result).not.toContain("*");
    });
  });

  describe("multi-table SELECT *", () => {
    it("expands SELECT * with multiple tables", () => {
      const result = expandStarColumns(
        "SELECT * FROM creators, conversations",
        schema
      );
      for (const col of creatorsColumns) {
        expect(result).toContain(`"${col}"`);
      }
      for (const col of conversationsColumns) {
        expect(result).toContain(`"${col}"`);
      }
      expect(result).not.toContain("*");
    });
  });

  describe("mixed star and explicit columns", () => {
    it("expands only the star, preserves explicit columns", () => {
      const result = expandStarColumns(
        "SELECT c.*, 1 as one FROM creators c",
        schema
      );
      for (const col of creatorsColumns) {
        expect(result).toContain(`"${col}"`);
      }
      expect(result).not.toContain("c.*");
      expect(result).toContain("one");
    });
  });

  describe("no expansion needed", () => {
    it("returns original SQL when no star columns", () => {
      const sql = 'SELECT id, "displayName" FROM creators';
      expect(expandStarColumns(sql, schema)).toBe(sql);
    });

    it("returns original SQL for non-SELECT statements", () => {
      const sql = "INSERT INTO creators (id) VALUES ('abc')";
      expect(expandStarColumns(sql, schema)).toBe(sql);
    });

    it("returns original SQL for multi-statement queries", () => {
      const sql = "SELECT * FROM creators; SELECT * FROM conversations";
      expect(expandStarColumns(sql, schema)).toBe(sql);
    });

    it("returns original SQL when table is not in schema", () => {
      const sql = "SELECT * FROM unknown_table";
      expect(expandStarColumns(sql, schema)).toBe(sql);
    });

    it("returns original SQL when SQL can't be parsed", () => {
      const sql = "THIS IS NOT VALID SQL %%%";
      expect(expandStarColumns(sql, schema)).toBe(sql);
    });

    it("returns original SQL for a subquery in FROM", () => {
      const sql = "SELECT * FROM (SELECT 1 AS x) sub";
      expect(expandStarColumns(sql, schema)).toBe(sql);
    });

    it("returns original SQL for a table function in FROM", () => {
      const sql = "SELECT * FROM generate_series(1,5)";
      expect(expandStarColumns(sql, schema)).toBe(sql);
    });
  });

  describe("edge cases", () => {
    it("handles quoted table names", () => {
      const result = expandStarColumns(
        'SELECT * FROM "creators"',
        schema
      );
      for (const col of creatorsColumns) {
        expect(result).toContain(`"${col}"`);
      }
    });

    it("handles JOIN with aliased t.*", () => {
      const result = expandStarColumns(
        'SELECT c.* FROM creators c JOIN conversations conv ON conv."creatorId" = c.id',
        schema
      );
      for (const col of creatorsColumns) {
        expect(result).toContain(`"${col}"`);
      }
      // Should not include conversations columns
      expect(result).not.toContain('"lastActivity"');
    });

    it("falls back when multi-table SELECT * has unknown table", () => {
      const sql = "SELECT * FROM creators, unknown_table";
      expect(expandStarColumns(sql, schema)).toBe(sql);
    });
  });
});
