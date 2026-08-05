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

  it("counts missing-table entries in the header total", () => {
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

    // Body lists both `creators` and the trailing `ghost_table` entry, so the header must say 2.
    expect(output).toContain("# Schema: local (2 tables, 0 FK relationships)");
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

  it("counts only the FK edges the map actually shows", () => {
    const schema = makeSchema([
      makeTable({
        sqlName: "creators",
        prismaModelName: "Creator",
        incomingFks: [{ fromTable: "audit_events", fromColumn: "creatorId" }],
      }),
      // Unmapped, so not rendered. Its FK to another unmapped table appears nowhere in the body.
      makeTable({
        sqlName: "audit_events",
        prismaModelName: null,
        outgoingFks: [
          { toTable: "creators", toColumn: "id", viaColumn: "creatorId" },
          { toTable: "_prisma_migrations", toColumn: "id", viaColumn: "migrationId" },
        ],
      }),
      makeTable({ sqlName: "_prisma_migrations", prismaModelName: null }),
    ]);

    const output = formatRelationshipMap(schema, "local");

    // audit_events -> creators is visible as the `<- audit_events` line on creators.
    // audit_events -> _prisma_migrations is not visible anywhere, so it is not counted.
    expect(output).toContain("# Schema: local (1 tables, 1 FK relationships)");
  });

  describe("no Prisma mapping loaded", () => {
    it("renders every database table instead of an empty map", () => {
      const schema = makeSchema([
        makeTable({
          sqlName: "posts",
          prismaModelName: null,
          outgoingFks: [{ toTable: "users", toColumn: "id", viaColumn: "author_id" }],
        }),
        makeTable({
          sqlName: "users",
          prismaModelName: null,
          incomingFks: [{ fromTable: "posts", fromColumn: "author_id" }],
        }),
      ]);

      const output = formatRelationshipMap(schema, "local", { hasPrismaMapping: false });

      expect(output).toContain("\nposts\n");
      expect(output).toContain("\nusers\n");
      expect(output).toContain("  -> users.id");
      expect(output).toContain("  <- posts");
    });

    it("renders a header count that matches the rendered body", () => {
      const schema = makeSchema([
        makeTable({
          sqlName: "posts",
          prismaModelName: null,
          outgoingFks: [{ toTable: "users", toColumn: "id", viaColumn: "author_id" }],
        }),
        makeTable({ sqlName: "users", prismaModelName: null }),
        makeTable({ sqlName: "_prisma_migrations", prismaModelName: null }),
      ]);

      const output = formatRelationshipMap(schema, "local", { hasPrismaMapping: false });

      expect(output).toContain("# Schema: local (3 tables, 1 FK relationships)");
    });

    it("suppresses drift warnings, which are Prisma-vs-database by definition", () => {
      const schema = makeSchema(
        [
          makeTable({
            sqlName: "creators",
            prismaModelName: null,
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
        ],
        {
          driftWarnings: [
            {
              type: "missing_table",
              tableName: "ghostTable",
              detail: 'Prisma model "GhostModel" maps to table "ghostTable" which does not exist',
            },
          ],
        }
      );

      const output = formatRelationshipMap(schema, "local", { hasPrismaMapping: false });

      expect(output).not.toContain("⚠");
      expect(output).not.toContain("TABLE MISSING IN DATABASE");
      expect(output).not.toContain("ghostTable");
    });

    it("emits no Prisma text anywhere in the map", () => {
      const schema = makeSchema(
        [
          makeTable({ sqlName: "creators", prismaModelName: "Creator" }),
          makeTable({ sqlName: "migrations", prismaModelName: null }),
        ],
        {
          driftWarnings: [
            {
              type: "missing_table",
              tableName: "ghostTable",
              detail: 'Prisma model "GhostModel" maps to table "ghostTable" which does not exist',
            },
          ],
        }
      );

      const output = formatRelationshipMap(schema, "local", { hasPrismaMapping: false });

      expect(output).not.toMatch(/prisma/i);
    });
  });
});
