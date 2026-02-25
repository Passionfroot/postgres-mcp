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

export function formatRelationshipMap(schema: MergedSchema, databaseId: string) {
  const mappedTables = schema.tables.filter((t) => t.prismaModelName !== null);
  const sortedTables = [...mappedTables].sort((a, b) => a.sqlName.localeCompare(b.sqlName));

  const totalFks = schema.tables.reduce((sum, t) => sum + t.outgoingFks.length, 0);

  const lines: string[] = [];
  lines.push(
    `# Schema: ${databaseId} (${mappedTables.length} tables, ${totalFks} FK relationships)`
  );
  lines.push("");
  lines.push("Use search_objects to look up column detail for specific tables.");

  // missing_table warnings from top-level driftWarnings
  const missingTableWarnings = schema.driftWarnings.filter((w) => w.type === "missing_table");
  const missingTableNames = new Set(missingTableWarnings.map((w) => w.tableName));

  for (const table of sortedTables) {
    lines.push("");

    const isMissingTable = missingTableNames.has(table.sqlName);
    if (isMissingTable) {
      lines.push(
        `${table.sqlName} (Prisma: ${table.prismaModelName}) -- TABLE MISSING IN DATABASE`
      );
      continue;
    }

    lines.push(`${table.sqlName} (Prisma: ${table.prismaModelName})`);

    const outgoing = renderOutgoingFks(table);
    if (outgoing) lines.push(outgoing);

    const incoming = renderIncomingFks(table);
    if (incoming) lines.push(incoming);

    for (const warning of table.driftWarnings) {
      lines.push(renderDriftWarning(warning));
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
