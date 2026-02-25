import { describe, expect, it } from "vitest";

import type { MergedColumn, MergedSchema, MergedTable } from "../../src/schema/types.js";

import { formatSearchResults, searchTables } from "../../src/schema/search.js";

function makeColumn(
  overrides: Partial<MergedColumn> & Pick<MergedColumn, "sqlName">
): MergedColumn {
  return {
    prismaFieldName: null,
    dataType: "text",
    udtName: "text",
    isNullable: false,
    columnDefault: null,
    isPrimaryKey: false,
    ...overrides,
  };
}

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

describe("searchTables", () => {
  it("matches by exact SQL name", () => {
    const schema = makeSchema([
      makeTable({ sqlName: "partnerUsers", prismaModelName: "User" }),
      makeTable({ sqlName: "creators" }),
    ]);

    const results = searchTables(schema, "partnerUsers");

    expect(results).toHaveLength(1);
    expect(results[0].sqlName).toBe("partnerUsers");
  });

  it("matches by exact Prisma model name", () => {
    const schema = makeSchema([
      makeTable({ sqlName: "partnerUsers", prismaModelName: "User" }),
      makeTable({ sqlName: "creators", prismaModelName: "Creator" }),
    ]);

    const results = searchTables(schema, "User");

    expect(results).toHaveLength(1);
    expect(results[0].sqlName).toBe("partnerUsers");
  });

  it("matches case-insensitively on exact match", () => {
    const schema = makeSchema([makeTable({ sqlName: "partnerUsers", prismaModelName: "User" })]);

    const results = searchTables(schema, "user");

    expect(results).toHaveLength(1);
    expect(results[0].prismaModelName).toBe("User");
  });

  it("returns partial/substring matches", () => {
    const schema = makeSchema([
      makeTable({ sqlName: "partnerUsers", prismaModelName: "User" }),
      makeTable({ sqlName: "partners", prismaModelName: "Partner" }),
      makeTable({ sqlName: "creators", prismaModelName: "Creator" }),
    ]);

    const results = searchTables(schema, "partner");

    expect(results).toHaveLength(2);
    expect(results.map((t) => t.sqlName)).toContain("partnerUsers");
    expect(results.map((t) => t.sqlName)).toContain("partners");
  });

  it("ranks exact matches before partial matches", () => {
    const schema = makeSchema([
      makeTable({ sqlName: "campaign_creators", prismaModelName: "CampaignCreator" }),
      makeTable({ sqlName: "creators", prismaModelName: "Creator" }),
    ]);

    const results = searchTables(schema, "creators");

    expect(results).toHaveLength(2);
    expect(results[0].sqlName).toBe("creators");
    expect(results[1].sqlName).toBe("campaign_creators");
  });

  it("returns empty array when no match", () => {
    const schema = makeSchema([makeTable({ sqlName: "creators", prismaModelName: "Creator" })]);

    const results = searchTables(schema, "nonexistent");

    expect(results).toHaveLength(0);
  });

  it("matches unmapped tables (prismaModelName is null)", () => {
    const schema = makeSchema([
      makeTable({ sqlName: "_prisma_migrations", prismaModelName: null }),
      makeTable({ sqlName: "creators", prismaModelName: "Creator" }),
    ]);

    const results = searchTables(schema, "prisma_migrations");

    expect(results).toHaveLength(1);
    expect(results[0].sqlName).toBe("_prisma_migrations");
    expect(results[0].prismaModelName).toBeNull();
  });

  it("matches on Prisma model name for tables with model mappings", () => {
    const schema = makeSchema([
      makeTable({ sqlName: "collaborations", prismaModelName: "Collaboration" }),
      makeTable({ sqlName: "creators", prismaModelName: "Creator" }),
    ]);

    const results = searchTables(schema, "Collaboration");

    expect(results).toHaveLength(1);
    expect(results[0].prismaModelName).toBe("Collaboration");
  });
});

describe("formatSearchResults", () => {
  it("formats a single table with columns, PKs, and FKs", () => {
    const tables = [
      makeTable({
        sqlName: "collaborations",
        prismaModelName: "Collaboration",
        primaryKeys: ["id"],
        columns: [
          makeColumn({
            sqlName: "id",
            dataType: "uuid",
            udtName: "uuid",
            isPrimaryKey: true,
            columnDefault: "gen_random_uuid()",
          }),
          makeColumn({
            sqlName: "creatorId",
            dataType: "uuid",
            udtName: "uuid",
            prismaFieldName: "creatorId",
          }),
          makeColumn({
            sqlName: "status",
            dataType: "USER-DEFINED",
            udtName: "CollaborationStatus",
            isNullable: true,
          }),
        ],
        outgoingFks: [{ toTable: "creators", toColumn: "id", viaColumn: "creatorId" }],
        incomingFks: [
          { fromTable: "invoices", fromColumn: "collaborationId" },
          { fromTable: "messages", fromColumn: "collaborationId" },
        ],
      }),
    ];

    const output = formatSearchResults(tables);

    expect(output).toContain("collaborations (Prisma: Collaboration)");
    expect(output).toContain("PK: id");
    expect(output).toContain("id");
    expect(output).toContain("uuid");
    expect(output).toContain("NOT NULL");
    expect(output).toContain("default: gen_random_uuid()");
    expect(output).toContain("creatorId");
    expect(output).toContain("USER-DEFINED");
    expect(output).toContain("FK out:");
    expect(output).toContain("-> creators.id via creatorId");
    expect(output).toContain("FK in:");
    expect(output).toContain("invoices");
    expect(output).toContain("messages");
  });

  it("shows 'no Prisma model' for unmapped tables", () => {
    const tables = [
      makeTable({
        sqlName: "_prisma_migrations",
        prismaModelName: null,
        columns: [makeColumn({ sqlName: "id", dataType: "integer", udtName: "int4" })],
      }),
    ];

    const output = formatSearchResults(tables);

    expect(output).toContain("_prisma_migrations (no Prisma model)");
  });

  it("resolves enum values for USER-DEFINED columns when resolver provided", () => {
    const tables = [
      makeTable({
        sqlName: "collaborations",
        prismaModelName: "Collaboration",
        columns: [
          makeColumn({
            sqlName: "status",
            dataType: "USER-DEFINED",
            udtName: "CollaborationStatus",
          }),
        ],
      }),
    ];

    const enumResolver = (udtName: string) => {
      if (udtName === "CollaborationStatus") {
        return [
          { label: "DRAFT", dbValue: "DRAFT" },
          { label: "ACTIVE", dbValue: "ACTIVE" },
          { label: "COMPLETED", dbValue: "completed" },
        ];
      }
      return null;
    };

    const output = formatSearchResults(tables, enumResolver);

    expect(output).toContain("enum CollaborationStatus:");
    expect(output).toContain("DRAFT, ACTIVE, completed");
  });

  it("returns 'No matching tables found.' when no tables", () => {
    const output = formatSearchResults([]);

    expect(output).toBe("No matching tables found.");
  });

  it("shows Prisma field name suffix when prismaFieldName differs from sqlName", () => {
    const tables = [
      makeTable({
        sqlName: "creators",
        prismaModelName: "Creator",
        columns: [
          makeColumn({
            sqlName: "legacy_id",
            dataType: "text",
            udtName: "text",
            prismaFieldName: "legacyId",
          }),
        ],
      }),
    ];

    const output = formatSearchResults(tables);

    expect(output).toContain("(Prisma: legacyId)");
  });
});
