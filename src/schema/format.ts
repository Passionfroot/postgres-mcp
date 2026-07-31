import type { DriftWarning, MergedSchema, MergedTable } from "./types.js";

function renderDriftWarning(warning: DriftWarning) {
  if (warning.type === "missing_table") {
    return ` -- TABLE MISSING IN DATABASE`;
  }
  return `  ⚠ ${warning.detail}`;
}

function renderOutgoingFks(table: MergedTable) {
  if (table.outgoingFks.length === 0) return null;
  const targets = table.outgoingFks.map((fk) => `${fk.toTable}.${fk.toColumn}`);
  return `  -> ${targets.join(", ")}`;
}

function renderIncomingFks(table: MergedTable) {
  if (table.incomingFks.length === 0) return null;
  const sources = [...new Set(table.incomingFks.map((fk) => fk.fromTable))].sort();
  return `  <- ${sources.join(", ")}`;
}

/**
 * Number of distinct FK edges the rendered map actually shows. An edge is visible if either end is
 * a rendered table: as a `->` line on the source, or a `<-` line on the target. Counting over
 * `schema.tables` instead would report edges between two tables the map never lists.
 */
function countVisibleFks(renderedTables: MergedTable[]) {
  const edges = new Set<string>();

  for (const table of renderedTables) {
    for (const fk of table.outgoingFks) {
      edges.add(`${table.sqlName}|${fk.viaColumn}|${fk.toTable}`);
    }
    for (const fk of table.incomingFks) {
      edges.add(`${fk.fromTable}|${fk.fromColumn}|${table.sqlName}`);
    }
  }

  return edges.size;
}

export interface FormatRelationshipMapOptions {
  /**
   * When false, no Prisma annotation is rendered and every database table is listed rather than
   * only the Prisma-mapped ones. Defaults to true.
   */
  hasPrismaMapping?: boolean;
}

export function formatRelationshipMap(
  schema: MergedSchema,
  databaseId: string,
  options: FormatRelationshipMapOptions = {}
) {
  const { hasPrismaMapping = true } = options;

  // With a mapping loaded the map is deliberately an application-model overview, so unmapped
  // tables are dropped. Without one that filter would empty the resource, so list everything.
  const visibleTables = hasPrismaMapping
    ? schema.tables.filter((t) => t.prismaModelName !== null)
    : schema.tables;
  const sortedTables = [...visibleTables].sort((a, b) => a.sqlName.localeCompare(b.sqlName));

  const totalFks = countVisibleFks(sortedTables);

  const lines: string[] = [];
  lines.push(
    `# Schema: ${databaseId} (${sortedTables.length} tables, ${totalFks} FK relationships)`
  );
  lines.push("");
  lines.push("Use search_objects to look up column detail for specific tables.");

  // missing_table warnings from top-level driftWarnings
  const missingTableWarnings = hasPrismaMapping
    ? schema.driftWarnings.filter((w) => w.type === "missing_table")
    : [];
  const missingTableNames = new Set(missingTableWarnings.map((w) => w.tableName));

  for (const table of sortedTables) {
    lines.push("");

    const isMissingTable = missingTableNames.has(table.sqlName);
    if (isMissingTable) {
      lines.push(`${table.sqlName} (Prisma: ${table.prismaModelName}) -- TABLE MISSING IN DATABASE`);
      continue;
    }

    lines.push(
      hasPrismaMapping && table.prismaModelName
        ? `${table.sqlName} (Prisma: ${table.prismaModelName})`
        : table.sqlName
    );

    const outgoing = renderOutgoingFks(table);
    if (outgoing) lines.push(outgoing);

    const incoming = renderIncomingFks(table);
    if (incoming) lines.push(incoming);

    // Drift is Prisma-vs-database by definition, and the detail text names Prisma models, fields
    // and types. Without a mapping there is nothing to drift from.
    if (hasPrismaMapping) {
      for (const warning of table.driftWarnings) {
        lines.push(renderDriftWarning(warning));
      }
    }
  }

  // Render top-level missing_table warnings for tables not already in sortedTables
  for (const warning of missingTableWarnings) {
    const alreadyRendered = sortedTables.some((t) => t.sqlName === warning.tableName);
    if (!alreadyRendered) {
      // Find the Prisma model name from the warning detail
      const modelNameMatch = warning.detail.match(/Prisma model "(\w+)"/);
      const modelName = modelNameMatch ? modelNameMatch[1] : warning.tableName;
      lines.push("");
      lines.push(`${warning.tableName} (Prisma: ${modelName}) -- TABLE MISSING IN DATABASE`);
    }
  }

  lines.push("");
  return lines.join("\n");
}
