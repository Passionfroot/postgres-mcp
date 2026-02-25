import type { MergedColumn, MergedSchema, MergedTable } from "./types.js";

export function searchTables(schema: MergedSchema, pattern: string): MergedTable[] {
  const lowerPattern = pattern.toLowerCase();

  const exact: MergedTable[] = [];
  const partial: MergedTable[] = [];

  for (const table of schema.tables) {
    const sqlLower = table.sqlName.toLowerCase();
    const prismaLower = table.prismaModelName?.toLowerCase() ?? null;

    const isExactSql = sqlLower === lowerPattern;
    const isExactPrisma = prismaLower === lowerPattern;

    if (isExactSql || isExactPrisma) {
      exact.push(table);
      continue;
    }

    const isPartialSql = sqlLower.includes(lowerPattern);
    const isPartialPrisma = prismaLower !== null && prismaLower.includes(lowerPattern);

    if (isPartialSql || isPartialPrisma) {
      partial.push(table);
    }
  }

  return [...exact, ...partial];
}

function formatColumn(col: MergedColumn) {
  const parts = [`    ${col.sqlName}`, col.dataType, col.isNullable ? "NULL" : "NOT NULL"];

  if (col.isPrimaryKey) parts.push("[PK]");
  if (col.columnDefault !== null) parts.push(`default: ${col.columnDefault}`);
  if (col.prismaFieldName !== null) {
    parts.push(`(Prisma: ${col.prismaFieldName})`);
  }

  return parts.join("  ");
}

export function formatSearchResults(
  tables: MergedTable[],
  enumResolver?: (udtName: string) => { label: string; dbValue: string }[] | null
) {
  if (tables.length === 0) return "No matching tables found.";

  const sections: string[] = [];

  for (const table of tables) {
    const lines: string[] = [];

    const header = table.prismaModelName
      ? `${table.sqlName} (Prisma: ${table.prismaModelName})`
      : `${table.sqlName} (no Prisma model)`;
    lines.push(header);

    if (table.primaryKeys.length > 0) {
      lines.push(`  PK: ${table.primaryKeys.join(", ")}`);
    }

    if (table.columns.length > 0) {
      lines.push("  Columns:");
      for (const col of table.columns) {
        lines.push(formatColumn(col));

        if (col.dataType === "USER-DEFINED" && enumResolver) {
          const values = enumResolver(col.udtName);
          if (values && values.length > 0) {
            lines.push(`      enum ${col.udtName}: ${values.map((v) => v.dbValue).join(", ")}`);
          }
        }
      }
    }

    if (table.outgoingFks.length > 0) {
      const fkParts = table.outgoingFks.map(
        (fk) => `-> ${fk.toTable}.${fk.toColumn} via ${fk.viaColumn}`
      );
      lines.push(`  FK out: ${fkParts.join(", ")}`);
    }

    if (table.incomingFks.length > 0) {
      const sources = [...new Set(table.incomingFks.map((fk) => fk.fromTable))].sort();
      lines.push(`  FK in:  <- ${sources.join(", ")}`);
    }

    sections.push(lines.join("\n"));
  }

  return sections.join("\n\n");
}
