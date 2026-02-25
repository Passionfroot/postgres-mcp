/**
 * Lightweight regex-based Prisma schema parser.
 *
 * We intentionally avoid `@prisma/internals` (getDMMF) here. That package pulls in
 * Prisma engine binaries (~40MB) which would bloat every MCP server instance. Since
 * this runs as a stdio process per Claude Code session, memory adds up quickly.
 *
 * The regex approach covers the schema patterns we actually use (models, fields,
 * @@map, @map, @relation, enums, @@id) and is covered by unit tests. Prisma's
 * schema syntax is stable, so the risk of breakage is low. If robustness becomes
 * a concern, consider a build-time script that runs getDMMF() once and writes JSON
 * for the server to read at startup.
 */
import fs from "node:fs";

import type {
  PrismaEnumMapping,
  PrismaFieldMapping,
  PrismaMapping,
  PrismaModelMapping,
  PrismaRelationMapping,
} from "./types.js";

const MODEL_BLOCK_RE = /model\s+(\w+)\s*\{([\s\S]*?)^\}/gm;
const ENUM_BLOCK_RE = /enum\s+(\w+)\s*\{([\s\S]*?)^\}/gm;
const TABLE_MAP_RE = /@@map\("([^"]+)"\)/;
const FIELD_MAP_RE = /@map\("([^"]+)"\)/;
const COMPOSITE_ID_RE = /@@id\(\[([^\]]+)\]\)/;
const FIELD_LINE_RE = /^\s+(\w+)\s+(\w+)(\?|\[\])?\s*(.*)/;
const RELATION_FIELDS_RE = /fields:\s*\[([^\]]+)\]/;
const RELATION_REFS_RE = /references:\s*\[([^\]]+)\]/;

function isRelationField(
  fieldType: string,
  modifier: string | undefined,
  knownModelNames: Set<string>
) {
  return modifier === "[]" || knownModelNames.has(fieldType);
}

function parseModelBlock(modelName: string, body: string, knownModelNames: Set<string>) {
  const tableMapMatch = body.match(TABLE_MAP_RE);
  const tableName = tableMapMatch ? tableMapMatch[1] : modelName;

  const compositeIdMatch = body.match(COMPOSITE_ID_RE);
  const compositePk = compositeIdMatch
    ? compositeIdMatch[1].split(",").map((s) => s.trim())
    : undefined;

  const fields: PrismaFieldMapping[] = [];
  const relations: PrismaRelationMapping[] = [];

  for (const line of body.split("\n")) {
    if (line.trim().startsWith("//")) continue;

    const fieldMatch = line.match(FIELD_LINE_RE);
    if (!fieldMatch) continue;

    const [, fieldName, fieldType, modifier, rest] = fieldMatch;

    if (isRelationField(fieldType, modifier, knownModelNames)) {
      if (modifier !== "[]") {
        const fieldsMatch = rest.match(RELATION_FIELDS_RE);
        const refsMatch = rest.match(RELATION_REFS_RE);
        if (fieldsMatch && refsMatch) {
          relations.push({
            fieldName,
            targetModel: fieldType,
            fromFields: fieldsMatch[1].split(",").map((s) => s.trim()),
            toReferences: refsMatch[1].split(",").map((s) => s.trim()),
          });
        }
      }
      continue;
    }

    const isId = rest.includes("@id");
    const columnMapMatch = rest.match(FIELD_MAP_RE);
    const columnName = columnMapMatch ? columnMapMatch[1] : fieldName;

    fields.push({ fieldName, columnName, prismaType: fieldType, isId });
  }

  const model: PrismaModelMapping = { modelName, tableName, fields };
  if (compositePk) {
    model.compositePk = compositePk;
  }
  if (relations.length > 0) {
    model.relations = relations;
  }
  return model;
}

function parseEnumBlock(enumName: string, body: string) {
  const values: PrismaEnumMapping["values"] = [];

  for (const line of body.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("//") || trimmed.startsWith("@@")) continue;

    const labelMatch = trimmed.match(/^(\w+)/);
    if (!labelMatch) continue;

    const label = labelMatch[1];
    const mapMatch = trimmed.match(FIELD_MAP_RE);
    const dbValue = mapMatch ? mapMatch[1] : label;

    values.push({ label, dbValue });
  }

  return { enumName, values } satisfies PrismaEnumMapping;
}

/** Extract all model names from content so relation field detection works. */
function collectModelNames(content: string) {
  const names = new Set<string>();
  for (const match of content.matchAll(/model\s+(\w+)\s*\{/g)) {
    names.add(match[1]);
  }
  return names;
}

function extractModels(content: string, knownModelNames: Set<string>) {
  return Array.from(content.matchAll(MODEL_BLOCK_RE)).map((m) =>
    parseModelBlock(m[1], m[2], knownModelNames)
  );
}

function extractEnums(content: string) {
  return Array.from(content.matchAll(ENUM_BLOCK_RE)).map((m) => parseEnumBlock(m[1], m[2]));
}

/**
 * Parse a single .prisma file content string into structured model and enum mappings. Pure function
 * -- no file I/O.
 */
export function parsePrismaSchema(content: string): PrismaMapping {
  const knownModelNames = collectModelNames(content);
  return {
    models: extractModels(content, knownModelNames),
    enums: extractEnums(content),
  };
}

/** Read multiple .prisma files and merge their parsed results. I/O wrapper around parsePrismaSchema. */
export function parsePrismaFiles(filePaths: string[]): PrismaMapping {
  // First pass: collect all model names across all files for relation detection
  const allModelNames = new Set<string>();
  const fileContents: string[] = [];
  for (const filePath of filePaths) {
    const content = fs.readFileSync(filePath, "utf-8");
    fileContents.push(content);
    for (const name of collectModelNames(content)) {
      allModelNames.add(name);
    }
  }

  // Second pass: parse each file with the full set of known model names
  const allModels: PrismaModelMapping[] = [];
  const allEnums: PrismaEnumMapping[] = [];

  for (const content of fileContents) {
    allModels.push(...extractModels(content, allModelNames));
    allEnums.push(...extractEnums(content));
  }

  return { models: allModels, enums: allEnums };
}
