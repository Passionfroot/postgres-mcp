import { describe, expect, it } from "vitest";

import type { DbMetadata, PrismaMapping } from "../../src/schema/types.js";

import { mergeSchemas } from "../../src/schema/merge.js";

function makeDbColumn(overrides: Partial<DbMetadata["columns"][0]> = {}): DbMetadata["columns"][0] {
  return {
    tableName: "users",
    columnName: "id",
    dataType: "text",
    udtName: "text",
    isNullable: false,
    columnDefault: null,
    ordinalPosition: 1,
    ...overrides,
  };
}

function makeDbMetadata(overrides: Partial<DbMetadata> = {}): DbMetadata {
  return {
    columns: [],
    primaryKeys: [],
    foreignKeys: [],
    enumValues: [],
    ...overrides,
  };
}

function makePrismaMapping(overrides: Partial<PrismaMapping> = {}): PrismaMapping {
  return {
    models: [],
    enums: [],
    ...overrides,
  };
}

describe("mergeSchemas", () => {
  it("merges a simple model with matching DB table", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "User",
          tableName: "partnerUsers",
          fields: [
            { fieldName: "id", columnName: "id", prismaType: "String", isId: true },
            { fieldName: "email", columnName: "email", prismaType: "String", isId: false },
          ],
        },
      ],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({ tableName: "partnerUsers", columnName: "id", ordinalPosition: 1 }),
        makeDbColumn({ tableName: "partnerUsers", columnName: "email", ordinalPosition: 2 }),
      ],
      primaryKeys: [{ tableName: "partnerUsers", columnName: "id", ordinalPosition: 1 }],
    });

    const result = mergeSchemas(prisma, db);

    expect(result.tables).toHaveLength(1);
    expect(result.tables[0].sqlName).toBe("partnerUsers");
    expect(result.tables[0].prismaModelName).toBe("User");
    expect(result.tables[0].columns).toHaveLength(2);
    expect(result.tables[0].primaryKeys).toEqual(["id"]);
    expect(result.driftWarnings).toHaveLength(0);
  });

  it("sets prismaFieldName only when it differs from SQL column name", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "Item",
          tableName: "items",
          fields: [
            { fieldName: "id", columnName: "id", prismaType: "String", isId: true },
            {
              fieldName: "displayName",
              columnName: "display_name",
              prismaType: "String",
              isId: false,
            },
            { fieldName: "email", columnName: "email", prismaType: "String", isId: false },
          ],
        },
      ],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({ tableName: "items", columnName: "id", ordinalPosition: 1 }),
        makeDbColumn({ tableName: "items", columnName: "display_name", ordinalPosition: 2 }),
        makeDbColumn({ tableName: "items", columnName: "email", ordinalPosition: 3 }),
      ],
    });

    const result = mergeSchemas(prisma, db);
    const columns = result.tables[0].columns;

    expect(columns[0].prismaFieldName).toBeNull(); // id == id
    expect(columns[1].prismaFieldName).toBe("displayName"); // displayName != display_name
    expect(columns[2].prismaFieldName).toBeNull(); // email == email
  });

  it("detects missing table drift", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "Ghost",
          tableName: "ghost_table",
          fields: [{ fieldName: "id", columnName: "id", prismaType: "String", isId: true }],
        },
      ],
    });

    const db = makeDbMetadata({ columns: [] });

    const result = mergeSchemas(prisma, db);

    expect(result.tables).toHaveLength(0);
    expect(result.driftWarnings).toHaveLength(1);
    expect(result.driftWarnings[0].type).toBe("missing_table");
    expect(result.driftWarnings[0].tableName).toBe("ghost_table");
    expect(result.driftWarnings[0].detail).toContain("Ghost");
  });

  it("detects missing column drift", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "Item",
          tableName: "items",
          fields: [
            { fieldName: "id", columnName: "id", prismaType: "String", isId: true },
            { fieldName: "phantom", columnName: "phantom", prismaType: "String", isId: false },
          ],
        },
      ],
    });

    const db = makeDbMetadata({
      columns: [makeDbColumn({ tableName: "items", columnName: "id", ordinalPosition: 1 })],
    });

    const result = mergeSchemas(prisma, db);
    const tableWarnings = result.tables[0].driftWarnings;

    expect(tableWarnings).toHaveLength(1);
    expect(tableWarnings[0].type).toBe("missing_column");
    expect(tableWarnings[0].detail).toContain("phantom");
  });

  it("detects type mismatch drift", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "Item",
          tableName: "items",
          fields: [{ fieldName: "name", columnName: "name", prismaType: "String", isId: false }],
        },
      ],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({
          tableName: "items",
          columnName: "name",
          dataType: "integer",
          udtName: "int4",
          ordinalPosition: 1,
        }),
      ],
    });

    const result = mergeSchemas(prisma, db);
    const tableWarnings = result.tables[0].driftWarnings;

    expect(tableWarnings).toHaveLength(1);
    expect(tableWarnings[0].type).toBe("type_mismatch");
    expect(tableWarnings[0].detail).toContain("String");
    expect(tableWarnings[0].detail).toContain("integer");
  });

  it("produces no false-positive type mismatch for standard Prisma-to-SQL mappings", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "Everything",
          tableName: "everything",
          fields: [
            { fieldName: "s", columnName: "s", prismaType: "String", isId: false },
            { fieldName: "i", columnName: "i", prismaType: "Int", isId: false },
            { fieldName: "bi", columnName: "bi", prismaType: "BigInt", isId: false },
            { fieldName: "f", columnName: "f", prismaType: "Float", isId: false },
            { fieldName: "b", columnName: "b", prismaType: "Boolean", isId: false },
            { fieldName: "dt", columnName: "dt", prismaType: "DateTime", isId: false },
            { fieldName: "j", columnName: "j", prismaType: "Json", isId: false },
            { fieldName: "d", columnName: "d", prismaType: "Decimal", isId: false },
            { fieldName: "by", columnName: "by", prismaType: "Bytes", isId: false },
          ],
        },
      ],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({
          tableName: "everything",
          columnName: "s",
          dataType: "text",
          udtName: "text",
          ordinalPosition: 1,
        }),
        makeDbColumn({
          tableName: "everything",
          columnName: "i",
          dataType: "integer",
          udtName: "int4",
          ordinalPosition: 2,
        }),
        makeDbColumn({
          tableName: "everything",
          columnName: "bi",
          dataType: "bigint",
          udtName: "int8",
          ordinalPosition: 3,
        }),
        makeDbColumn({
          tableName: "everything",
          columnName: "f",
          dataType: "double precision",
          udtName: "float8",
          ordinalPosition: 4,
        }),
        makeDbColumn({
          tableName: "everything",
          columnName: "b",
          dataType: "boolean",
          udtName: "bool",
          ordinalPosition: 5,
        }),
        makeDbColumn({
          tableName: "everything",
          columnName: "dt",
          dataType: "timestamp without time zone",
          udtName: "timestamp",
          ordinalPosition: 6,
        }),
        makeDbColumn({
          tableName: "everything",
          columnName: "j",
          dataType: "jsonb",
          udtName: "jsonb",
          ordinalPosition: 7,
        }),
        makeDbColumn({
          tableName: "everything",
          columnName: "d",
          dataType: "numeric",
          udtName: "numeric",
          ordinalPosition: 8,
        }),
        makeDbColumn({
          tableName: "everything",
          columnName: "by",
          dataType: "bytea",
          udtName: "bytea",
          ordinalPosition: 9,
        }),
      ],
    });

    const result = mergeSchemas(prisma, db);

    expect(result.tables[0].driftWarnings).toHaveLength(0);
    expect(result.driftWarnings).toHaveLength(0);
  });

  it("handles enum type matching (USER-DEFINED + matching udt_name = no warning)", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "Collab",
          tableName: "collabs",
          fields: [
            { fieldName: "status", columnName: "status", prismaType: "CollabStatus", isId: false },
          ],
        },
      ],
      enums: [{ enumName: "CollabStatus", values: [{ label: "ACTIVE", dbValue: "ACTIVE" }] }],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({
          tableName: "collabs",
          columnName: "status",
          dataType: "USER-DEFINED",
          udtName: "CollabStatus",
          ordinalPosition: 1,
        }),
      ],
    });

    const result = mergeSchemas(prisma, db);

    expect(result.tables[0].driftWarnings).toHaveLength(0);
  });

  it("detects enum type mismatch when udt_name differs", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "Collab",
          tableName: "collabs",
          fields: [
            { fieldName: "status", columnName: "status", prismaType: "CollabStatus", isId: false },
          ],
        },
      ],
      enums: [{ enumName: "CollabStatus", values: [{ label: "ACTIVE", dbValue: "ACTIVE" }] }],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({
          tableName: "collabs",
          columnName: "status",
          dataType: "USER-DEFINED",
          udtName: "OtherEnum",
          ordinalPosition: 1,
        }),
      ],
    });

    const result = mergeSchemas(prisma, db);

    expect(result.tables[0].driftWarnings).toHaveLength(1);
    expect(result.tables[0].driftWarnings[0].type).toBe("type_mismatch");
  });

  it("collects unmapped tables (DB tables not in any Prisma model)", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "User",
          tableName: "users",
          fields: [{ fieldName: "id", columnName: "id", prismaType: "String", isId: true }],
        },
      ],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({ tableName: "users", columnName: "id", ordinalPosition: 1 }),
        makeDbColumn({ tableName: "_prisma_migrations", columnName: "id", ordinalPosition: 1 }),
        makeDbColumn({ tableName: "orphan_table", columnName: "id", ordinalPosition: 1 }),
      ],
    });

    const result = mergeSchemas(prisma, db);

    expect(result.unmappedTables).toEqual(["_prisma_migrations", "orphan_table"]);
    expect(result.tables).toHaveLength(3);
  });

  it("correctly builds outgoingFks and incomingFks from FK data", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "Invoice",
          tableName: "invoices",
          fields: [
            { fieldName: "id", columnName: "id", prismaType: "String", isId: true },
            { fieldName: "collabId", columnName: "collabId", prismaType: "String", isId: false },
          ],
        },
        {
          modelName: "Collab",
          tableName: "collabs",
          fields: [{ fieldName: "id", columnName: "id", prismaType: "String", isId: true }],
        },
      ],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({ tableName: "invoices", columnName: "id", ordinalPosition: 1 }),
        makeDbColumn({ tableName: "invoices", columnName: "collabId", ordinalPosition: 2 }),
        makeDbColumn({ tableName: "collabs", columnName: "id", ordinalPosition: 1 }),
      ],
      foreignKeys: [
        { fromTable: "invoices", fromColumn: "collabId", toTable: "collabs", toColumn: "id" },
      ],
    });

    const result = mergeSchemas(prisma, db);
    const invoicesTable = result.tables.find((t) => t.sqlName === "invoices")!;
    const collabsTable = result.tables.find((t) => t.sqlName === "collabs")!;

    expect(invoicesTable.outgoingFks).toEqual([
      { toTable: "collabs", toColumn: "id", viaColumn: "collabId" },
    ]);
    expect(collabsTable.incomingFks).toEqual([{ fromTable: "invoices", fromColumn: "collabId" }]);
  });

  it("marks primary key columns correctly (including composite PKs)", () => {
    const prisma = makePrismaMapping({
      models: [
        {
          modelName: "CampaignToCreator",
          tableName: "CampaignToCreator",
          fields: [
            {
              fieldName: "campaignId",
              columnName: "campaignId",
              prismaType: "String",
              isId: false,
            },
            { fieldName: "creatorId", columnName: "creatorId", prismaType: "String", isId: false },
          ],
          compositePk: ["campaignId", "creatorId"],
        },
      ],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({
          tableName: "CampaignToCreator",
          columnName: "campaignId",
          ordinalPosition: 1,
        }),
        makeDbColumn({
          tableName: "CampaignToCreator",
          columnName: "creatorId",
          ordinalPosition: 2,
        }),
      ],
      primaryKeys: [
        { tableName: "CampaignToCreator", columnName: "campaignId", ordinalPosition: 1 },
        { tableName: "CampaignToCreator", columnName: "creatorId", ordinalPosition: 2 },
      ],
    });

    const result = mergeSchemas(prisma, db);
    const table = result.tables[0];

    expect(table.primaryKeys).toEqual(["campaignId", "creatorId"]);
    expect(table.columns[0].isPrimaryKey).toBe(true);
    expect(table.columns[1].isPrimaryKey).toBe(true);
  });

  describe("Prisma-derived FK relationships", () => {
    it("derives FKs from Prisma @relation when DB has no FK constraints", () => {
      const prisma = makePrismaMapping({
        models: [
          {
            modelName: "Invoice",
            tableName: "invoices",
            fields: [
              { fieldName: "id", columnName: "id", prismaType: "String", isId: true },
              { fieldName: "collabId", columnName: "collabId", prismaType: "String", isId: false },
            ],
            relations: [
              {
                fieldName: "collab",
                targetModel: "Collab",
                fromFields: ["collabId"],
                toReferences: ["id"],
              },
            ],
          },
          {
            modelName: "Collab",
            tableName: "collabs",
            fields: [{ fieldName: "id", columnName: "id", prismaType: "String", isId: true }],
          },
        ],
      });

      const db = makeDbMetadata({
        columns: [
          makeDbColumn({ tableName: "invoices", columnName: "id", ordinalPosition: 1 }),
          makeDbColumn({ tableName: "invoices", columnName: "collabId", ordinalPosition: 2 }),
          makeDbColumn({ tableName: "collabs", columnName: "id", ordinalPosition: 1 }),
        ],
        foreignKeys: [],
      });

      const result = mergeSchemas(prisma, db);
      const invoicesTable = result.tables.find((t) => t.sqlName === "invoices")!;
      const collabsTable = result.tables.find((t) => t.sqlName === "collabs")!;

      expect(invoicesTable.outgoingFks).toEqual([
        { toTable: "collabs", toColumn: "id", viaColumn: "collabId" },
      ]);
      expect(collabsTable.incomingFks).toEqual([{ fromTable: "invoices", fromColumn: "collabId" }]);
    });

    it("deduplicates when both DB and Prisma provide the same FK", () => {
      const prisma = makePrismaMapping({
        models: [
          {
            modelName: "Invoice",
            tableName: "invoices",
            fields: [
              { fieldName: "id", columnName: "id", prismaType: "String", isId: true },
              { fieldName: "collabId", columnName: "collabId", prismaType: "String", isId: false },
            ],
            relations: [
              {
                fieldName: "collab",
                targetModel: "Collab",
                fromFields: ["collabId"],
                toReferences: ["id"],
              },
            ],
          },
          {
            modelName: "Collab",
            tableName: "collabs",
            fields: [{ fieldName: "id", columnName: "id", prismaType: "String", isId: true }],
          },
        ],
      });

      const db = makeDbMetadata({
        columns: [
          makeDbColumn({ tableName: "invoices", columnName: "id", ordinalPosition: 1 }),
          makeDbColumn({ tableName: "invoices", columnName: "collabId", ordinalPosition: 2 }),
          makeDbColumn({ tableName: "collabs", columnName: "id", ordinalPosition: 1 }),
        ],
        foreignKeys: [
          { fromTable: "invoices", fromColumn: "collabId", toTable: "collabs", toColumn: "id" },
        ],
      });

      const result = mergeSchemas(prisma, db);
      const invoicesTable = result.tables.find((t) => t.sqlName === "invoices")!;

      expect(invoicesTable.outgoingFks).toHaveLength(1);
      expect(invoicesTable.outgoingFks).toEqual([
        { toTable: "collabs", toColumn: "id", viaColumn: "collabId" },
      ]);
    });

    it("derives composite (multi-column) FKs from Prisma relation", () => {
      const prisma = makePrismaMapping({
        models: [
          {
            modelName: "LineItem",
            tableName: "line_items",
            fields: [
              { fieldName: "id", columnName: "id", prismaType: "String", isId: true },
              { fieldName: "orderId", columnName: "orderId", prismaType: "String", isId: false },
              {
                fieldName: "productId",
                columnName: "productId",
                prismaType: "String",
                isId: false,
              },
            ],
            relations: [
              {
                fieldName: "orderProduct",
                targetModel: "OrderProduct",
                fromFields: ["orderId", "productId"],
                toReferences: ["orderId", "productId"],
              },
            ],
          },
          {
            modelName: "OrderProduct",
            tableName: "order_products",
            fields: [
              { fieldName: "orderId", columnName: "orderId", prismaType: "String", isId: false },
              {
                fieldName: "productId",
                columnName: "productId",
                prismaType: "String",
                isId: false,
              },
            ],
            compositePk: ["orderId", "productId"],
          },
        ],
      });

      const db = makeDbMetadata({
        columns: [
          makeDbColumn({ tableName: "line_items", columnName: "id", ordinalPosition: 1 }),
          makeDbColumn({ tableName: "line_items", columnName: "orderId", ordinalPosition: 2 }),
          makeDbColumn({ tableName: "line_items", columnName: "productId", ordinalPosition: 3 }),
          makeDbColumn({ tableName: "order_products", columnName: "orderId", ordinalPosition: 1 }),
          makeDbColumn({
            tableName: "order_products",
            columnName: "productId",
            ordinalPosition: 2,
          }),
        ],
      });

      const result = mergeSchemas(prisma, db);
      const lineItemsTable = result.tables.find((t) => t.sqlName === "line_items")!;

      expect(lineItemsTable.outgoingFks).toEqual([
        { toTable: "order_products", toColumn: "orderId", viaColumn: "orderId" },
        { toTable: "order_products", toColumn: "productId", viaColumn: "productId" },
      ]);
    });

    it("resolves @map column names when deriving FKs from Prisma relations", () => {
      const prisma = makePrismaMapping({
        models: [
          {
            modelName: "Invoice",
            tableName: "invoices",
            fields: [
              { fieldName: "id", columnName: "id", prismaType: "String", isId: true },
              {
                fieldName: "collaborationId",
                columnName: "collaboration_id",
                prismaType: "String",
                isId: false,
              },
            ],
            relations: [
              {
                fieldName: "collaboration",
                targetModel: "Collaboration",
                fromFields: ["collaborationId"],
                toReferences: ["id"],
              },
            ],
          },
          {
            modelName: "Collaboration",
            tableName: "collaborations",
            fields: [{ fieldName: "id", columnName: "id", prismaType: "String", isId: true }],
          },
        ],
      });

      const db = makeDbMetadata({
        columns: [
          makeDbColumn({ tableName: "invoices", columnName: "id", ordinalPosition: 1 }),
          makeDbColumn({
            tableName: "invoices",
            columnName: "collaboration_id",
            ordinalPosition: 2,
          }),
          makeDbColumn({ tableName: "collaborations", columnName: "id", ordinalPosition: 1 }),
        ],
      });

      const result = mergeSchemas(prisma, db);
      const invoicesTable = result.tables.find((t) => t.sqlName === "invoices")!;

      expect(invoicesTable.outgoingFks).toEqual([
        { toTable: "collaborations", toColumn: "id", viaColumn: "collaboration_id" },
      ]);
    });

    it("skips gracefully when target model is missing from Prisma mapping", () => {
      const prisma = makePrismaMapping({
        models: [
          {
            modelName: "Invoice",
            tableName: "invoices",
            fields: [
              { fieldName: "id", columnName: "id", prismaType: "String", isId: true },
              { fieldName: "ghostId", columnName: "ghostId", prismaType: "String", isId: false },
            ],
            relations: [
              {
                fieldName: "ghost",
                targetModel: "GhostModel",
                fromFields: ["ghostId"],
                toReferences: ["id"],
              },
            ],
          },
        ],
      });

      const db = makeDbMetadata({
        columns: [
          makeDbColumn({ tableName: "invoices", columnName: "id", ordinalPosition: 1 }),
          makeDbColumn({ tableName: "invoices", columnName: "ghostId", ordinalPosition: 2 }),
        ],
      });

      const result = mergeSchemas(prisma, db);
      const invoicesTable = result.tables.find((t) => t.sqlName === "invoices")!;

      expect(invoicesTable.outgoingFks).toEqual([]);
    });
  });

  it("sorts tables by sqlName and unmappedTables alphabetically", () => {
    const prisma = makePrismaMapping({
      models: [
        { modelName: "Zebra", tableName: "zebras", fields: [] },
        { modelName: "Alpha", tableName: "alphas", fields: [] },
      ],
    });

    const db = makeDbMetadata({
      columns: [
        makeDbColumn({ tableName: "zebras", columnName: "id", ordinalPosition: 1 }),
        makeDbColumn({ tableName: "alphas", columnName: "id", ordinalPosition: 1 }),
        makeDbColumn({ tableName: "zzz_unmapped", columnName: "id", ordinalPosition: 1 }),
        makeDbColumn({ tableName: "aaa_unmapped", columnName: "id", ordinalPosition: 1 }),
      ],
    });

    const result = mergeSchemas(prisma, db);

    expect(result.tables.map((t) => t.sqlName)).toEqual([
      "aaa_unmapped",
      "alphas",
      "zebras",
      "zzz_unmapped",
    ]);
    expect(result.unmappedTables).toEqual(["aaa_unmapped", "zzz_unmapped"]);
  });
});
