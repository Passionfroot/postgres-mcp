export interface PrismaFieldMapping {
  fieldName: string;
  columnName: string;
  prismaType: string;
  isId: boolean;
}

export interface PrismaRelationMapping {
  fieldName: string;
  targetModel: string;
  fromFields: string[];
  toReferences: string[];
}

export interface PrismaModelMapping {
  modelName: string;
  tableName: string;
  fields: PrismaFieldMapping[];
  compositePk?: string[];
  relations?: PrismaRelationMapping[];
}

export interface PrismaEnumMapping {
  enumName: string;
  values: { label: string; dbValue: string }[];
}

export interface PrismaMapping {
  models: PrismaModelMapping[];
  enums: PrismaEnumMapping[];
}

export interface DbColumn {
  tableName: string;
  columnName: string;
  dataType: string;
  udtName: string;
  isNullable: boolean;
  columnDefault: string | null;
  ordinalPosition: number;
}

export interface DbPrimaryKey {
  tableName: string;
  columnName: string;
  ordinalPosition: number;
}

export interface DbForeignKey {
  /**
   * Constraint identity, so the columns of a composite FK stay grouped as one relationship.
   * null for FKs derived from a Prisma schema without a matching database constraint.
   */
  constraintName: string | null;
  fromTable: string;
  fromColumn: string;
  toTable: string;
  toColumn: string;
}

/** One unique index, as its ordered list of key columns. */
export interface DbUniqueColumnSet {
  tableName: string;
  columnNames: string[];
}

export interface DbEnumValue {
  enumName: string;
  enumValue: string;
  sortOrder: number;
}

export interface DbMetadata {
  columns: DbColumn[];
  primaryKeys: DbPrimaryKey[];
  foreignKeys: DbForeignKey[];
  enumValues: DbEnumValue[];
  /**
   * "table.column" entries for columns that are unique on their own. Accepts an array as
   * well as a Set: this interface is exported, and a Set does not survive a JSON round-trip.
   * Undefined means uniqueness is unknown, which is not the same as "nothing is unique" —
   * consumers must not treat it as an empty set.
   */
  uniqueColumns?: Set<string> | string[];
  /** Full unique indexes, needed to decide cardinality for composite FKs. */
  uniqueColumnSets?: DbUniqueColumnSet[];
}

export interface DriftWarning {
  type: "missing_table" | "missing_column" | "type_mismatch";
  tableName: string;
  detail: string;
}

export interface MergedColumn {
  sqlName: string;
  prismaFieldName: string | null;
  dataType: string;
  udtName: string;
  isNullable: boolean;
  columnDefault: string | null;
  isPrimaryKey: boolean;
}

export interface MergedIncomingFk {
  fromTable: string;
  fromColumn: string;
  /** Groups the columns of a composite FK. null when the FK has no constraint identity. */
  constraintName: string | null;
  /**
   * Whether the referencing side is unique, so the join is 1:1. Same value on every column
   * of a composite FK, since uniqueness is a property of the column set.
   * null means unknown: render no cardinality rather than asserting one.
   */
  isUnique: boolean | null;
}

export interface MergedTable {
  sqlName: string;
  prismaModelName: string | null;
  columns: MergedColumn[];
  primaryKeys: string[];
  incomingFks: MergedIncomingFk[];
  outgoingFks: { toTable: string; toColumn: string; viaColumn: string }[];
  driftWarnings: DriftWarning[];
}

export interface MergedSchema {
  tables: MergedTable[];
  unmappedTables: string[];
  driftWarnings: DriftWarning[];
  /** DB-introspected enum labels by udt_name, in enumsortorder. Available without a Prisma schema. */
  dbEnums: Record<string, string[]>;
}
