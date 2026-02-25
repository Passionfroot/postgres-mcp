import { describe, expect, it } from "vitest";

import { parsePrismaSchema } from "../../src/schema/prisma-parser.js";

describe("parsePrismaSchema", () => {
  it("parses a model with @@map and extracts correct tableName", () => {
    const content = `
model User {
  id    String @id @default(uuid())
  email String
  @@map("partnerUsers")
}
`;
    const result = parsePrismaSchema(content);

    expect(result.models).toHaveLength(1);
    expect(result.models[0].modelName).toBe("User");
    expect(result.models[0].tableName).toBe("partnerUsers");
  });

  it("defaults tableName to modelName when @@map is absent", () => {
    const content = `
model Plan {
  id   String @id @default(uuid())
  name String
}
`;
    const result = parsePrismaSchema(content);

    expect(result.models[0].modelName).toBe("Plan");
    expect(result.models[0].tableName).toBe("Plan");
  });

  it("extracts scalar fields with correct types", () => {
    const content = `
model Creator {
  id        String   @id @default(uuid())
  name      String
  verified  Boolean
  score     Float
  createdAt DateTime @default(now())
  @@map("creators")
}
`;
    const result = parsePrismaSchema(content);
    const fields = result.models[0].fields;

    expect(fields).toHaveLength(5);
    expect(fields[0]).toMatchObject({ fieldName: "id", prismaType: "String", isId: true });
    expect(fields[1]).toMatchObject({ fieldName: "name", prismaType: "String", isId: false });
    expect(fields[2]).toMatchObject({ fieldName: "verified", prismaType: "Boolean", isId: false });
    expect(fields[3]).toMatchObject({ fieldName: "score", prismaType: "Float", isId: false });
    expect(fields[4]).toMatchObject({
      fieldName: "createdAt",
      prismaType: "DateTime",
      isId: false,
    });
  });

  it("excludes relation fields with @relation() decorator", () => {
    const content = `
model Collaboration {
  id        String  @id @default(uuid())
  creatorId String
  creator   Creator @relation(fields: [creatorId], references: [id])
  @@map("collaborations")
}

model Creator {
  id             String          @id @default(uuid())
  collaborations Collaboration[]
  @@map("creators")
}
`;
    const result = parsePrismaSchema(content);
    const collabFields = result.models.find((m) => m.modelName === "Collaboration")!.fields;

    expect(collabFields).toHaveLength(2);
    expect(collabFields.map((f) => f.fieldName)).toEqual(["id", "creatorId"]);
  });

  it("excludes implicit relation fields (model type without @relation())", () => {
    const content = `
model Collaboration {
  id        String   @id @default(uuid())
  creatorId String
  creator   Creator?
  @@map("collaborations")
}

model Creator {
  id String @id @default(uuid())
  @@map("creators")
}
`;
    const result = parsePrismaSchema(content);
    const collabFields = result.models.find((m) => m.modelName === "Collaboration")!.fields;

    expect(collabFields).toHaveLength(2);
    expect(collabFields.map((f) => f.fieldName)).toEqual(["id", "creatorId"]);
  });

  it("excludes array relation fields ([] modifier)", () => {
    const content = `
model Creator {
  id             String          @id @default(uuid())
  collaborations Collaboration[]
  @@map("creators")
}

model Collaboration {
  id String @id @default(uuid())
  @@map("collaborations")
}
`;
    const result = parsePrismaSchema(content);
    const creatorFields = result.models.find((m) => m.modelName === "Creator")!.fields;

    expect(creatorFields).toHaveLength(1);
    expect(creatorFields[0].fieldName).toBe("id");
  });

  it("extracts @map on field -> correct columnName", () => {
    const content = `
model Item {
  id       String @id @default(uuid())
  legacyId String @map("legacy_id")
}
`;
    const result = parsePrismaSchema(content);
    const fields = result.models[0].fields;

    expect(fields[1].fieldName).toBe("legacyId");
    expect(fields[1].columnName).toBe("legacy_id");
  });

  it("defaults columnName to fieldName when @map is absent", () => {
    const content = `
model Item {
  id   String @id @default(uuid())
  name String
}
`;
    const result = parsePrismaSchema(content);

    expect(result.models[0].fields[1].columnName).toBe("name");
  });

  it("extracts @id flag on fields", () => {
    const content = `
model Thing {
  id   String  @id @default(uuid())
  name String
}
`;
    const result = parsePrismaSchema(content);

    expect(result.models[0].fields[0].isId).toBe(true);
    expect(result.models[0].fields[1].isId).toBe(false);
  });

  it("extracts @@id([field1, field2]) composite PKs", () => {
    const content = `
model CampaignToCreator {
  campaignId String
  creatorId  String
  @@id([campaignId, creatorId])
}
`;
    const result = parsePrismaSchema(content);

    expect(result.models[0].compositePk).toEqual(["campaignId", "creatorId"]);
  });

  it("does not set compositePk when @@id is absent", () => {
    const content = `
model Item {
  id String @id @default(uuid())
}
`;
    const result = parsePrismaSchema(content);

    expect(result.models[0].compositePk).toBeUndefined();
  });

  it("parses enum with plain values", () => {
    const content = `
enum Status {
  ACTIVE
  INACTIVE
  DELETED
}
`;
    const result = parsePrismaSchema(content);

    expect(result.enums).toHaveLength(1);
    expect(result.enums[0].enumName).toBe("Status");
    expect(result.enums[0].values).toEqual([
      { label: "ACTIVE", dbValue: "ACTIVE" },
      { label: "INACTIVE", dbValue: "INACTIVE" },
      { label: "DELETED", dbValue: "DELETED" },
    ]);
  });

  it("parses enum with @map on values (ClerkRole pattern)", () => {
    const content = `
enum ClerkRole {
  ADMIN  @map("org:admin")
  MEMBER @map("org:member")
}
`;
    const result = parsePrismaSchema(content);

    expect(result.enums[0].values).toEqual([
      { label: "ADMIN", dbValue: "org:admin" },
      { label: "MEMBER", dbValue: "org:member" },
    ]);
  });

  it("handles multiple models and enums in one content string", () => {
    const content = `
model Alpha {
  id String @id
  @@map("alphas")
}

enum Color {
  RED
  GREEN
}

model Beta {
  id    String @id
  color Color
}

enum Size {
  SMALL
  LARGE
}
`;
    const result = parsePrismaSchema(content);

    expect(result.models).toHaveLength(2);
    expect(result.models.map((m) => m.modelName)).toEqual(["Alpha", "Beta"]);
    expect(result.enums).toHaveLength(2);
    expect(result.enums.map((e) => e.enumName)).toEqual(["Color", "Size"]);
  });

  it("skips comment lines inside model blocks", () => {
    const content = `
model Thing {
  id   String @id @default(uuid())
  // This is a comment
  name String
  // Another comment
}
`;
    const result = parsePrismaSchema(content);

    expect(result.models[0].fields).toHaveLength(2);
    expect(result.models[0].fields.map((f) => f.fieldName)).toEqual(["id", "name"]);
  });

  describe("relation extraction", () => {
    it("extracts a simple @relation with fields and references", () => {
      const content = `
model Collaboration {
  id        String  @id @default(uuid())
  creatorId String
  creator   Creator @relation(fields: [creatorId], references: [id])
  @@map("collaborations")
}

model Creator {
  id             String          @id @default(uuid())
  collaborations Collaboration[]
  @@map("creators")
}
`;
      const result = parsePrismaSchema(content);
      const collab = result.models.find((m) => m.modelName === "Collaboration")!;

      expect(collab.relations).toEqual([
        {
          fieldName: "creator",
          targetModel: "Creator",
          fromFields: ["creatorId"],
          toReferences: ["id"],
        },
      ]);
    });

    it("extracts a named @relation with fields and references", () => {
      const content = `
model Message {
  id         String @id @default(uuid())
  senderId   String
  sender     User   @relation("SentMessages", fields: [senderId], references: [id])
}

model User {
  id           String    @id @default(uuid())
  sentMessages Message[] @relation("SentMessages")
}
`;
      const result = parsePrismaSchema(content);
      const message = result.models.find((m) => m.modelName === "Message")!;

      expect(message.relations).toEqual([
        {
          fieldName: "sender",
          targetModel: "User",
          fromFields: ["senderId"],
          toReferences: ["id"],
        },
      ]);
    });

    it("extracts composite (multi-field) relations", () => {
      const content = `
model LineItem {
  id         String  @id @default(uuid())
  orderId    String
  productId  String
  orderProduct OrderProduct @relation(fields: [orderId, productId], references: [orderId, productId])
}

model OrderProduct {
  orderId   String
  productId String
  items     LineItem[]
  @@id([orderId, productId])
}
`;
      const result = parsePrismaSchema(content);
      const lineItem = result.models.find((m) => m.modelName === "LineItem")!;

      expect(lineItem.relations).toEqual([
        {
          fieldName: "orderProduct",
          targetModel: "OrderProduct",
          fromFields: ["orderId", "productId"],
          toReferences: ["orderId", "productId"],
        },
      ]);
    });

    it("does not extract relations from inverse-only side (no fields/references)", () => {
      const content = `
model Creator {
  id             String          @id @default(uuid())
  collaborations Collaboration[]
  @@map("creators")
}

model Collaboration {
  id String @id @default(uuid())
  @@map("collaborations")
}
`;
      const result = parsePrismaSchema(content);
      const creator = result.models.find((m) => m.modelName === "Creator")!;

      expect(creator.relations).toBeUndefined();
    });

    it("does not set relations when model has no relation fields", () => {
      const content = `
model Simple {
  id   String @id @default(uuid())
  name String
}
`;
      const result = parsePrismaSchema(content);

      expect(result.models[0].relations).toBeUndefined();
    });

    it("extracts multiple relations from a single model", () => {
      const content = `
model Collaboration {
  id         String   @id @default(uuid())
  creatorId  String
  campaignId String
  creator    Creator  @relation(fields: [creatorId], references: [id])
  campaign   Campaign @relation(fields: [campaignId], references: [id])
  @@map("collaborations")
}

model Creator {
  id String @id @default(uuid())
}

model Campaign {
  id String @id @default(uuid())
}
`;
      const result = parsePrismaSchema(content);
      const collab = result.models.find((m) => m.modelName === "Collaboration")!;

      expect(collab.relations).toHaveLength(2);
      expect(collab.relations).toEqual([
        {
          fieldName: "creator",
          targetModel: "Creator",
          fromFields: ["creatorId"],
          toReferences: ["id"],
        },
        {
          fieldName: "campaign",
          targetModel: "Campaign",
          fromFields: ["campaignId"],
          toReferences: ["id"],
        },
      ]);
    });
  });

  it("handles optional fields (?) correctly -- still scalar, still included", () => {
    const content = `
model Profile {
  id    String  @id @default(uuid())
  bio   String?
  age   Int?
  email String
}
`;
    const result = parsePrismaSchema(content);
    const fields = result.models[0].fields;

    expect(fields).toHaveLength(4);
    expect(fields[1]).toMatchObject({ fieldName: "bio", prismaType: "String" });
    expect(fields[2]).toMatchObject({ fieldName: "age", prismaType: "Int" });
  });
});
