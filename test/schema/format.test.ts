import { describe, expect, it } from "vitest";

import type { MergedSchema, MergedTable } from "../../src/schema/types.js";

import { formatRelationshipMap } from "../../src/schema/format.js";

function makeTable(overrides: Partial<MergedTable> & Pick<MergedTable, "sqlName">): MergedTable {
  return {
    prismaModelName: null,
    columns: [],
    primaryKeys: [],
    incomingFks: [],
    outgoingFks: [],
    driftWarnings: [],
    ...overrides,
  };
}

function makeSchema(tables: MergedTable[], overrides?: Partial<MergedSchema>): MergedSchema {
  return {
    tables,
    unmappedTables: [],
    driftWarnings: [],
    ...overrides,
  };
}

describe("formatRelationshipMap", () => {
  it("formats a single table with Prisma model name and outgoing FKs", () => {
    const schema = makeSchema([
      makeTable({
        sqlName: "collaborations",
        prismaModelName: "Collaboration",
        outgoingFks: [
          { toTable: "creators", toColumn: "id", viaColumn: "creatorId" },
          { toTable: "campaigns", toColumn: "id", viaColumn: "campaignId" },
        ],
      }),
    ]);

    const output = formatRelationshipMap(schema, "local");

    expect(output).toContain("collaborations (Prisma: Collaboration)");
    expect(output).toContain("  -> creators.id, campaigns.id");
  });

  it("formats incoming FKs as <- with unique source table names", () => {
    const schema = makeSchema([
      makeTable({
        sqlName: "creators",
        prismaModelName: "Creator",
        incomingFks: [
          { fromTable: "collaborations", fromColumn: "creatorId" },
          { fromTable: "collaborations", fromColumn: "ownerId" },
          { fromTable: "invoices", fromColumn: "creatorId" },
        ],
      }),
    ]);

    const output = formatRelationshipMap(schema, "local");

    expect(output).toContain("  <- collaborations, invoices");
  });

  it("excludes tables where prismaModelName is null", () => {
    const schema = makeSchema([
      makeTable({
        sqlName: "_prisma_migrations",
        prismaModelName: null,
      }),
      makeTable({
        sqlName: "creators",
        prismaModelName: "Creator",
      }),
    ]);

    const output = formatRelationshipMap(schema, "local");

    expect(output).toContain("creators (Prisma: Creator)");
    expect(output).not.toContain("_prisma_migrations");
  });

  it("renders header with correct table count (only mapped) and FK count", () => {
    const schema = makeSchema([
      makeTable({
        sqlName: "creators",
        prismaModelName: "Creator",
        outgoingFks: [{ toTable: "users", toColumn: "id", viaColumn: "userId" }],
      }),
      makeTable({
        sqlName: "campaigns",
        prismaModelName: "Campaign",
        outgoingFks: [
          { toTable: "creators", toColumn: "id", viaColumn: "creatorId" },
          { toTable: "partners", toColumn: "id", viaColumn: "partnerId" },
        ],
      }),
      makeTable({
        sqlName: "_prisma_migrations",
        prismaModelName: null,
      }),
    ]);

    const output = formatRelationshipMap(schema, "prod");

    expect(output).toContain("# Schema: prod (2 tables, 3 FK relationships)");
  });

  it("renders missing_table drift warning inline", () => {
    const schema = makeSchema(
      [
        makeTable({
          sqlName: "creators",
          prismaModelName: "Creator",
        }),
      ],
      {
        driftWarnings: [
          {
            type: "missing_table",
            tableName: "ghost_table",
            detail:
              'Prisma model "GhostModel" maps to table "ghost_table" which does not exist in the database',
          },
        ],
      }
    );

    const output = formatRelationshipMap(schema, "local");

    expect(output).toContain("ghost_table (Prisma: GhostModel) -- TABLE MISSING IN DATABASE");
  });

  it("renders missing_column and type_mismatch drift warnings indented under table", () => {
    const schema = makeSchema([
      makeTable({
        sqlName: "creators",
        prismaModelName: "Creator",
        driftWarnings: [
          {
            type: "missing_column",
            tableName: "creators",
            detail:
              'Prisma field "legacyName" maps to column "legacy_name" which does not exist in table "creators"',
          },
          {
            type: "type_mismatch",
            tableName: "creators",
            detail: 'Column "age": Prisma type "Int" expects integer but DB has "text"',
          },
        ],
      }),
    ]);

    const output = formatRelationshipMap(schema, "local");

    expect(output).toContain(
      '  ⚠ Prisma field "legacyName" maps to column "legacy_name" which does not exist in table "creators"'
    );
    expect(output).toContain(
      '  ⚠ Column "age": Prisma type "Int" expects integer but DB has "text"'
    );
  });

  it("omits -> line when no outgoing FKs and <- when no incoming FKs", () => {
    const schema = makeSchema([
      makeTable({
        sqlName: "settings",
        prismaModelName: "Settings",
      }),
    ]);

    const output = formatRelationshipMap(schema, "local");

    expect(output).toContain("settings (Prisma: Settings)");
    expect(output).not.toContain("->");
    expect(output).not.toContain("<-");
  });

  it("sorts tables alphabetically by sqlName", () => {
    const schema = makeSchema([
      makeTable({ sqlName: "zebra", prismaModelName: "Zebra" }),
      makeTable({ sqlName: "alpha", prismaModelName: "Alpha" }),
      makeTable({ sqlName: "middle", prismaModelName: "Middle" }),
    ]);

    const output = formatRelationshipMap(schema, "local");
    const lines = output.split("\n");
    const tableLines = lines.filter((l) => l.match(/^\w.*\(Prisma:/));

    expect(tableLines[0]).toContain("alpha");
    expect(tableLines[1]).toContain("middle");
    expect(tableLines[2]).toContain("zebra");
  });

  it("includes the search_objects guidance line after the header", () => {
    const schema = makeSchema([makeTable({ sqlName: "creators", prismaModelName: "Creator" })]);

    const output = formatRelationshipMap(schema, "local");
    const lines = output.split("\n");

    const headerIdx = lines.findIndex((l) => l.startsWith("# Schema:"));
    expect(lines[headerIdx + 2]).toBe(
      "Use search_objects to look up column detail for specific tables."
    );
  });
});
