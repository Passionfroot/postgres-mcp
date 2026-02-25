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
  fromTable: string;
  fromColumn: string;
  toTable: string;
  toColumn: string;
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

export interface MergedTable {
  sqlName: string;
  prismaModelName: string | null;
  columns: MergedColumn[];
  primaryKeys: string[];
  incomingFks: { fromTable: string; fromColumn: string }[];
  outgoingFks: { toTable: string; toColumn: string; viaColumn: string }[];
  driftWarnings: DriftWarning[];
}

export interface MergedSchema {
  tables: MergedTable[];
  unmappedTables: string[];
  driftWarnings: DriftWarning[];
}
