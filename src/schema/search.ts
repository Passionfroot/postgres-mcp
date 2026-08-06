import type {
  MergedColumn,
  MergedIncomingFk,
  MergedSchema,
  MergedTable,
} from "./types.js";

export function searchTables(
  schema: MergedSchema,
  pattern: string
): MergedTable[] {
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
    const isPartialPrisma =
      prismaLower !== null && prismaLower.includes(lowerPattern);

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
  if (!resolution.isDbFallback || labels.length <= ENUM_FULL_RENDER_MAX)
    return labels.join(", ");
  return `${labels.slice(0, ENUM_SAMPLE_SIZE).join(", ")}, … (${
    labels.length
  } values total)`;
}

function formatColumn(col: MergedColumn, hasPrismaMapping: boolean) {
  const parts = [
    `    ${col.sqlName}`,
    col.dataType,
    col.isNullable ? "NULL" : "NOT NULL",
  ];

  if (col.isPrimaryKey) parts.push("[PK]");
  if (col.columnDefault !== null) parts.push(`default: ${col.columnDefault}`);
  if (hasPrismaMapping && col.prismaFieldName !== null) {
    parts.push(`(Prisma: ${col.prismaFieldName})`);
  }

  return parts.join("  ");
}

const FAN_OUT_WARNING =
  "⚠ 1:many FKs above will duplicate rows on JOIN. Use a subquery, LATERAL JOIN, or DISTINCT ON to avoid fan-out.";

/**
 * Render incoming FKs, one entry per constraint. A composite FK is a single relationship
 * joined on all its columns, so it renders as `via (a, b)` — splitting it into one arrow per
 * column invents join paths that do not exist.
 *
 * isUnique === null means uniqueness could not be determined; the cardinality tag is left off
 * rather than asserting a value.
 */
function renderIncomingFks(table: MergedTable) {
  if (table.incomingFks.length === 0) return null;

  const parts: string[] = [];
  const seen = new Set<string>();

  for (const group of groupIncomingFks(table)) {
    const columns = group.map((fk) => fk.fromColumn);
    const via = columns.length === 1 ? columns[0] : `(${columns.join(", ")})`;

    const key = `${group[0].fromTable}.${via}`;
    if (seen.has(key)) continue;
    seen.add(key);

    const isUnique = group[0].isUnique;
    const cardinality =
      isUnique === null ? "" : isUnique ? " [1:1]" : " [1:many]";
    parts.push(`<- ${group[0].fromTable} via ${via}${cardinality}`);
  }

  return parts.join(", ");
}

function groupIncomingFks(table: MergedTable) {
  const groups: MergedIncomingFk[][] = [];
  const byKey = new Map<string, MergedIncomingFk[]>();

  for (const fk of table.incomingFks) {
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

function hasFanOut(table: MergedTable) {
  return table.incomingFks.some((fk) => fk.isUnique === false);
}

export interface FormatSearchResultsOptions {
  enumResolver?: (udtName: string) => EnumResolution | null;
  /** When false, no Prisma annotation is rendered at all. Defaults to true. */
  hasPrismaMapping?: boolean;
}

export function formatSearchResults(
  tables: MergedTable[],
  options: FormatSearchResultsOptions = {}
) {
  if (tables.length === 0) return "No matching tables found.";

  const { enumResolver, hasPrismaMapping = true } = options;

  const sections: string[] = [];
  let hasAnyFanOut = false;

  for (const table of tables) {
    const lines: string[] = [];

    let header: string;
    if (!hasPrismaMapping) {
      header = table.sqlName;
    } else if (table.prismaModelName) {
      header = `${table.sqlName} (Prisma: ${table.prismaModelName})`;
    } else {
      header = `${table.sqlName} (no Prisma model)`;
    }
    lines.push(header);

    if (table.primaryKeys.length > 0) {
      lines.push(`  PK: ${table.primaryKeys.join(", ")}`);
    }

    if (table.columns.length > 0) {
      lines.push("  Columns:");
      for (const col of table.columns) {
        lines.push(formatColumn(col, hasPrismaMapping));

        if (col.dataType === "USER-DEFINED" && enumResolver) {
          const resolution = enumResolver(col.udtName);
          if (resolution && resolution.values.length > 0) {
            lines.push(
              `      enum ${col.udtName}: ${formatEnumValues(resolution)}`
            );
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

    const incoming = renderIncomingFks(table);
    if (incoming) {
      lines.push(`  FK in:  ${incoming}`);
      if (hasFanOut(table)) hasAnyFanOut = true;
    }

    sections.push(lines.join("\n"));
  }

  // One warning per response, not per table: the text is identical every time and repeating
  // it across dozens of tables is pure token cost.
  if (hasAnyFanOut) {
    sections.push(FAN_OUT_WARNING);
  }

  return sections.join("\n\n");
}
