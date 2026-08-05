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

// Full label list for typical enums. Huge ones (e.g. Country, 245 labels) get a sample + count,
// but only when resolved via the DB-introspection fallback (no Prisma schema available) -
// Prisma-mapped enums keep rendering in full, as they did before that fallback existed.
const ENUM_FULL_RENDER_MAX = 24;
const ENUM_SAMPLE_SIZE = 8;

export interface EnumResolution {
  values: { label: string; dbValue: string }[];
  isDbFallback: boolean;
}

function formatEnumValues(resolution: EnumResolution) {
  const labels = resolution.values.map((v) => v.dbValue);
  if (!resolution.isDbFallback || labels.length <= ENUM_FULL_RENDER_MAX) return labels.join(", ");
  return `${labels.slice(0, ENUM_SAMPLE_SIZE).join(", ")}, … (${labels.length} values total)`;
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
  enumResolver?: (udtName: string) => EnumResolution | null
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
          const resolution = enumResolver(col.udtName);
          if (resolution && resolution.values.length > 0) {
            lines.push(`      enum ${col.udtName}: ${formatEnumValues(resolution)}`);
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
