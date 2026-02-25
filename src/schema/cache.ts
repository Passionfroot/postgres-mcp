import fs from "node:fs";
import path from "node:path";
import pg from "pg";

import type { Config } from "../types.js";
import type { MergedSchema, PrismaMapping } from "./types.js";

import { logger } from "../logger.js";
import { introspectDatabase } from "./introspect.js";
import { mergeSchemas } from "./merge.js";
import { parsePrismaFiles } from "./prisma-parser.js";

function isNodeError(err: unknown): err is NodeJS.ErrnoException {
  return err instanceof Error && "code" in err;
}

function isEnoent(err: unknown) {
  return isNodeError(err) && err.code === "ENOENT";
}

export class SchemaCache {
  private cache = new Map<string, MergedSchema>();

  constructor(private prismaMapping: PrismaMapping) {}

  async get(database: string, pool: pg.Pool): Promise<MergedSchema> {
    const cached = this.cache.get(database);
    if (cached) return cached;

    logger.debug(`Cache miss for "${database}", introspecting...`);
    const dbMetadata = await introspectDatabase(pool);
    const merged = mergeSchemas(this.prismaMapping, dbMetadata);
    this.cache.set(database, merged);
    logger.info(
      `Cached schema for "${database}": ${merged.tables.length} tables, ${merged.driftWarnings.length} drift warnings`
    );
    return merged;
  }

  getEnumValues(enumName: string): { label: string; dbValue: string }[] | null {
    const enumMapping = this.prismaMapping.enums.find((e) => e.enumName === enumName);
    return enumMapping ? enumMapping.values : null;
  }

  clear(database?: string) {
    if (database) {
      this.cache.delete(database);
    } else {
      this.cache.clear();
    }
  }
}

function discoverPrismaFiles(mainSchemaPath: string) {
  const resolvedMain = path.resolve(mainSchemaPath);
  const schemaDir = path.dirname(resolvedMain);
  const modelsDir = path.join(schemaDir, "models");

  const filePaths = [resolvedMain];

  try {
    const entries = fs.readdirSync(modelsDir);
    for (const entry of entries) {
      if (entry.endsWith(".prisma")) {
        filePaths.push(path.join(modelsDir, entry));
      }
    }
  } catch (err) {
    if (!isEnoent(err)) throw err;
  }

  return filePaths;
}

export async function createSchemaCache(config: Config): Promise<SchemaCache> {
  if (!config.prismaSchemaPath) {
    logger.info("No prisma_schema_path configured, schema cache will have no Prisma annotations");
    return new SchemaCache({ models: [], enums: [] });
  }

  const filePaths = discoverPrismaFiles(config.prismaSchemaPath);
  logger.info(`Discovered ${filePaths.length} Prisma schema file(s)`);

  const mapping = parsePrismaFiles(filePaths);
  logger.info(
    `Parsed ${mapping.models.length} model(s) and ${mapping.enums.length} enum(s) from Prisma schema`
  );

  return new SchemaCache(mapping);
}
