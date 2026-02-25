import type {
  DbColumn,
  DbForeignKey,
  DbMetadata,
  DbPrimaryKey,
  DriftWarning,
  MergedColumn,
  MergedSchema,
  MergedTable,
  PrismaMapping,
  PrismaModelMapping,
} from "./types.js";

const PRISMA_TO_SQL_TYPES: Record<string, string[]> = {
  String: ["text", "character varying"],
  Int: ["integer"],
  BigInt: ["bigint"],
  Float: ["double precision"],
  Boolean: ["boolean"],
  DateTime: ["timestamp without time zone", "timestamp with time zone"],
  Json: ["jsonb", "json"],
  Decimal: ["numeric", "decimal"],
  Bytes: ["bytea"],
};

function groupBy<T>(items: T[], keyFn: (item: T) => string) {
  const map = new Map<string, T[]>();
  for (const item of items) {
    const key = keyFn(item);
    const existing = map.get(key);
    if (existing) {
      existing.push(item);
    } else {
      map.set(key, [item]);
    }
  }
  return map;
}

function resolveFieldToColumn(model: PrismaModelMapping, prismaFieldName: string) {
  const field = model.fields.find((f) => f.fieldName === prismaFieldName);
  return field?.columnName;
}

function deriveFksFromPrisma(prisma: PrismaMapping) {
  const modelByName = new Map(prisma.models.map((m) => [m.modelName, m]));
  const fks: DbForeignKey[] = [];

  for (const model of prisma.models) {
    if (!model.relations) continue;

    for (const rel of model.relations) {
      const targetModel = modelByName.get(rel.targetModel);
      if (!targetModel) continue;

      for (let i = 0; i < rel.fromFields.length; i++) {
        const fromColumn = resolveFieldToColumn(model, rel.fromFields[i]);
        const toColumn = resolveFieldToColumn(targetModel, rel.toReferences[i]);
        if (!fromColumn || !toColumn) continue;

        fks.push({
          fromTable: model.tableName,
          fromColumn,
          toTable: targetModel.tableName,
          toColumn,
        });
      }
    }
  }

  return fks;
}

function fkKey(fk: DbForeignKey) {
  return `${fk.fromTable}|${fk.fromColumn}|${fk.toTable}|${fk.toColumn}`;
}

function deduplicateFks(dbFks: DbForeignKey[], prismaFks: DbForeignKey[]) {
  const seen = new Set(dbFks.map(fkKey));
  const merged = [...dbFks];

  for (const fk of prismaFks) {
    if (!seen.has(fkKey(fk))) {
      seen.add(fkKey(fk));
      merged.push(fk);
    }
  }

  return merged;
}

interface TableLookups {
  columnsByTable: Map<string, DbColumn[]>;
  pksByTable: Map<string, DbPrimaryKey[]>;
  fksByFromTable: Map<string, DbForeignKey[]>;
  fksByToTable: Map<string, DbForeignKey[]>;
  prismaEnumNames: Set<string>;
}

function dbColToMergedColumn(dbCol: DbColumn, pkColNames: Set<string>): MergedColumn {
  return {
    sqlName: dbCol.columnName,
    prismaFieldName: null,
    dataType: dbCol.dataType,
    udtName: dbCol.udtName,
    isNullable: dbCol.isNullable,
    columnDefault: dbCol.columnDefault,
    isPrimaryKey: pkColNames.has(dbCol.columnName),
  };
}

function buildMergedTable(
  tableName: string,
  model: PrismaModelMapping | null,
  lookups: TableLookups
): { table: MergedTable; warnings: DriftWarning[] } {
  const dbCols = lookups.columnsByTable.get(tableName) ?? [];
  const pks = lookups.pksByTable.get(tableName) ?? [];
  const pkColNames = new Set(pks.map((pk) => pk.columnName));
  const outgoingFks = lookups.fksByFromTable.get(tableName) ?? [];
  const incomingFks = lookups.fksByToTable.get(tableName) ?? [];

  const tableWarnings: DriftWarning[] = [];
  let mergedColumns: MergedColumn[];

  if (model) {
    const dbColMap = new Map(dbCols.map((c) => [c.columnName, c]));
    const coveredDbCols = new Set<string>();
    mergedColumns = [];

    for (const field of model.fields) {
      const dbCol = dbColMap.get(field.columnName);

      if (!dbCol) {
        tableWarnings.push({
          type: "missing_column",
          tableName,
          detail: `Prisma field "${field.fieldName}" maps to column "${field.columnName}" which does not exist in table "${tableName}"`,
        });
        continue;
      }

      coveredDbCols.add(field.columnName);
      checkTypeMismatch(field.prismaType, dbCol, lookups.prismaEnumNames, tableWarnings, tableName);

      mergedColumns.push({
        ...dbColToMergedColumn(dbCol, pkColNames),
        prismaFieldName: field.fieldName !== dbCol.columnName ? field.fieldName : null,
      });
    }

    for (const dbCol of dbCols) {
      if (!coveredDbCols.has(dbCol.columnName)) {
        mergedColumns.push(dbColToMergedColumn(dbCol, pkColNames));
      }
    }
  } else {
    mergedColumns = dbCols.map((c) => dbColToMergedColumn(c, pkColNames));
  }

  return {
    table: {
      sqlName: tableName,
      prismaModelName: model?.modelName ?? null,
      columns: mergedColumns,
      primaryKeys: pks.map((pk) => pk.columnName),
      incomingFks: incomingFks.map((fk) => ({
        fromTable: fk.fromTable,
        fromColumn: fk.fromColumn,
      })),
      outgoingFks: outgoingFks.map((fk) => ({
        toTable: fk.toTable,
        toColumn: fk.toColumn,
        viaColumn: fk.fromColumn,
      })),
      driftWarnings: tableWarnings,
    },
    warnings: tableWarnings,
  };
}

/** Merge Prisma schema mappings with live database metadata and detect drift. */
export function mergeSchemas(prisma: PrismaMapping, db: DbMetadata): MergedSchema {
  const prismaFks = deriveFksFromPrisma(prisma);
  const allFks = deduplicateFks(db.foreignKeys, prismaFks);

  const dbTableNames = new Set(db.columns.map((c) => c.tableName));
  const lookups: TableLookups = {
    columnsByTable: groupBy(db.columns, (c) => c.tableName),
    pksByTable: groupBy(db.primaryKeys, (pk) => pk.tableName),
    fksByFromTable: groupBy(allFks, (fk) => fk.fromTable),
    fksByToTable: groupBy(allFks, (fk) => fk.toTable),
    prismaEnumNames: new Set(prisma.enums.map((e) => e.enumName)),
  };

  const tables: MergedTable[] = [];
  const topLevelWarnings: DriftWarning[] = [];
  const mappedTableNames = new Set<string>();

  for (const model of prisma.models) {
    mappedTableNames.add(model.tableName);

    if (!dbTableNames.has(model.tableName)) {
      topLevelWarnings.push({
        type: "missing_table",
        tableName: model.tableName,
        detail: `Prisma model "${model.modelName}" maps to table "${model.tableName}" which does not exist in the database`,
      });
      continue;
    }

    const { table } = buildMergedTable(model.tableName, model, lookups);
    tables.push(table);
  }

  const unmappedTables: string[] = [];
  for (const tableName of dbTableNames) {
    if (mappedTableNames.has(tableName)) continue;
    unmappedTables.push(tableName);

    const { table } = buildMergedTable(tableName, null, lookups);
    tables.push(table);
  }

  tables.sort((a, b) => a.sqlName.localeCompare(b.sqlName));
  unmappedTables.sort();

  return {
    tables,
    unmappedTables,
    driftWarnings: topLevelWarnings,
  };
}

function checkTypeMismatch(
  prismaType: string,
  dbCol: DbColumn,
  prismaEnumNames: Set<string>,
  warnings: DriftWarning[],
  tableName: string
) {
  // If the Prisma type is an enum, check that DB reports USER-DEFINED with matching udt_name
  if (prismaEnumNames.has(prismaType)) {
    if (dbCol.dataType === "USER-DEFINED") {
      if (dbCol.udtName.toLowerCase() !== prismaType.toLowerCase()) {
        warnings.push({
          type: "type_mismatch",
          tableName,
          detail: `Column "${dbCol.columnName}": DB enum type "${dbCol.udtName}" does not match Prisma enum "${prismaType}"`,
        });
      }
    } else {
      warnings.push({
        type: "type_mismatch",
        tableName,
        detail: `Column "${dbCol.columnName}": expected USER-DEFINED for Prisma enum "${prismaType}" but got "${dbCol.dataType}"`,
      });
    }
    return;
  }

  // Check standard type mappings
  const expectedSqlTypes = PRISMA_TO_SQL_TYPES[prismaType];
  if (!expectedSqlTypes) return; // Unknown Prisma type -- skip

  if (!expectedSqlTypes.includes(dbCol.dataType)) {
    warnings.push({
      type: "type_mismatch",
      tableName,
      detail: `Column "${dbCol.columnName}": Prisma type "${prismaType}" expects ${expectedSqlTypes.join(" or ")} but DB has "${dbCol.dataType}"`,
    });
  }
}
