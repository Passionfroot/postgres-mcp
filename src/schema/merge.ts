import type {
  DbColumn,
  DbForeignKey,
  DbMetadata,
  DbPrimaryKey,
  DriftWarning,
  MergedColumn,
  MergedIncomingFk,
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

      // Synthetic identity so the columns of a composite Prisma relation group together the
      // same way a database constraint does. Namespaced to avoid colliding with a conname.
      const constraintName = `prisma:${model.modelName}.${rel.fieldName}`;

      for (let i = 0; i < rel.fromFields.length; i++) {
        const fromColumn = resolveFieldToColumn(model, rel.fromFields[i]);
        const toColumn = resolveFieldToColumn(targetModel, rel.toReferences[i]);
        if (!fromColumn || !toColumn) continue;

        fks.push({
          constraintName,
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

/**
 * Uniqueness knowledge for the whole database. `columns`/`columnSetsByTable` being undefined
 * means "not introspected", which must stay distinguishable from "introspected, nothing
 * unique": defaulting the former to empty would relabel every genuine 1:1 as 1:many.
 */
interface UniquenessIndex {
  columns: Set<string> | undefined;
  columnSetsByTable: Map<string, string[][]> | undefined;
}

function buildUniquenessIndex(db: DbMetadata): UniquenessIndex {
  // Normalizes both the Set and the array form (a Set does not survive a JSON round-trip).
  const columns = db.uniqueColumns ? new Set(db.uniqueColumns) : undefined;

  const columnSetsByTable = db.uniqueColumnSets
    ? groupBy(db.uniqueColumnSets, (s) => s.tableName)
    : undefined;

  return {
    columns,
    columnSetsByTable: columnSetsByTable
      ? new Map([...columnSetsByTable].map(([table, sets]) => [table, sets.map((s) => s.columnNames)]))
      : undefined,
  };
}

/**
 * Whether an FK's referencing column set is unique, so the join yields at most one row.
 * Returns null when uniqueness cannot be decided from the metadata available.
 *
 * A unique index over a subset of the FK's columns proves the whole column set is unique too
 * (a candidate key stays a key once you add more columns to it), so this checks for any known
 * unique set contained in fkColumns, not just an exact match against it.
 */
function isFkUnique(
  tableName: string,
  fkColumns: string[],
  uniqueness: UniquenessIndex
): boolean | null {
  if (fkColumns.length === 1) {
    if (!uniqueness.columns) return null;
    return uniqueness.columns.has(`${tableName}.${fkColumns[0]}`);
  }

  // Single-column uniqueness can never decide a composite FK, so without the column sets
  // there is nothing to answer with.
  if (!uniqueness.columnSetsByTable) return null;

  const fkColumnSet = new Set(fkColumns);
  const knownSets = uniqueness.columnSetsByTable.get(tableName) ?? [];
  return knownSets.some((set) => set.every((col) => fkColumnSet.has(col)));
}

/**
 * Split incoming FKs into constraints. Rows without a constraint name cannot be grouped,
 * so each becomes its own single-column relationship.
 */
function groupIncomingFksByConstraint(fks: DbForeignKey[]) {
  const groups: DbForeignKey[][] = [];
  const byKey = new Map<string, DbForeignKey[]>();

  for (const fk of fks) {
    if (fk.constraintName === null) {
      groups.push([fk]);
      continue;
    }

    const key = `${fk.fromTable}|${fk.constraintName}`;
    const existing = byKey.get(key);
    if (existing) {
      existing.push(fk);
    } else {
      const group = [fk];
      byKey.set(key, group);
      groups.push(group);
    }
  }

  return groups;
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

/**
 * One entry per FK column, carrying the constraint identity and a cardinality decided over
 * the constraint's whole column set. Consumers group by constraintName to render a composite
 * FK as one relationship.
 */
function buildIncomingFks(
  incomingFks: DbForeignKey[],
  uniqueness: UniquenessIndex
): MergedIncomingFk[] {
  const result: MergedIncomingFk[] = [];

  for (const group of groupIncomingFksByConstraint(incomingFks)) {
    const fromTable = group[0].fromTable;
    const isUnique = isFkUnique(
      fromTable,
      group.map((fk) => fk.fromColumn),
      uniqueness
    );

    for (const fk of group) {
      result.push({
        fromTable,
        fromColumn: fk.fromColumn,
        constraintName: fk.constraintName,
        isUnique,
      });
    }
  }

  return result;
}

function buildMergedTable(
  tableName: string,
  model: PrismaModelMapping | null,
  lookups: TableLookups,
  uniqueness: UniquenessIndex
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
      incomingFks: buildIncomingFks(incomingFks, uniqueness),
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
export function mergeSchemas(prisma: PrismaMapping | null, db: DbMetadata): MergedSchema {
  const mapping = prisma ?? { models: [], enums: [] };
  const prismaFks = deriveFksFromPrisma(mapping);
  const allFks = deduplicateFks(db.foreignKeys, prismaFks);
  const uniqueness = buildUniquenessIndex(db);

  const dbTableNames = new Set(db.columns.map((c) => c.tableName));
  const lookups: TableLookups = {
    columnsByTable: groupBy(db.columns, (c) => c.tableName),
    pksByTable: groupBy(db.primaryKeys, (pk) => pk.tableName),
    fksByFromTable: groupBy(allFks, (fk) => fk.fromTable),
    fksByToTable: groupBy(allFks, (fk) => fk.toTable),
    prismaEnumNames: new Set(mapping.enums.map((e) => e.enumName)),
  };

  const tables: MergedTable[] = [];
  const topLevelWarnings: DriftWarning[] = [];
  const mappedTableNames = new Set<string>();

  for (const model of mapping.models) {
    mappedTableNames.add(model.tableName);

    if (!dbTableNames.has(model.tableName)) {
      topLevelWarnings.push({
        type: "missing_table",
        tableName: model.tableName,
        detail: `Prisma model "${model.modelName}" maps to table "${model.tableName}" which does not exist in the database`,
      });
      continue;
    }

    const { table } = buildMergedTable(model.tableName, model, lookups, uniqueness);
    tables.push(table);
  }

  const unmappedTables: string[] = [];
  for (const tableName of dbTableNames) {
    if (mappedTableNames.has(tableName)) continue;
    unmappedTables.push(tableName);

    const { table } = buildMergedTable(tableName, null, lookups, uniqueness);
    tables.push(table);
  }

  tables.sort((a, b) => a.sqlName.localeCompare(b.sqlName));
  unmappedTables.sort();

  const dbEnums: Record<string, string[]> = {};
  for (const ev of [...db.enumValues].sort((a, b) => a.sortOrder - b.sortOrder)) {
    (dbEnums[ev.enumName] ??= []).push(ev.enumValue);
  }

  return {
    tables,
    unmappedTables,
    driftWarnings: topLevelWarnings,
    dbEnums,
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
