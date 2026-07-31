import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { parse as parseToml } from "smol-toml";
import { z } from "zod";

import type { AuditLogConfig, Config, SourceConfig } from "./types.js";

import { logger } from "./logger.js";

export const sourceConfigSchema = z.object({
  id: z.string().min(1, "Source id is required"),
  dsn: z.string().min(1, "Source dsn is required"),
  readonly: z.boolean().optional().default(false),
  max_rows: z.number().int().positive().optional().default(1000),
  timeout: z.number().positive().optional().default(10),
  pool_max: z.number().int().positive().optional(),
  max_response_bytes: z.number().int().positive().optional().default(1_000_000),
  allow_multi_statements: z.boolean().optional().default(false),
  role: z.string().min(1).optional(),
  session_vars: z.record(z.string().min(1), z.string()).optional(),
  ssh_host: z.string().optional(),
  ssh_user: z.string().optional(),
  ssh_key: z.string().optional(),
});

/** One process per client, so one connection per client is the whole budget. */
export const DEFAULT_POOL_MAX = 1;

/**
 * Under --http one process serves every client, so pool_max stops being a per-client budget and
 * becomes a per-server one. Left at 1 the clients queue behind each other on a single connection.
 * 10 matches what 10 stdio sessions already asked of the database, so the ceiling is unchanged
 * while concurrent clients stop serializing.
 */
export const HTTP_DEFAULT_POOL_MAX = 10;

const auditLogSchema = z.object({
  log_file: z.string().min(1, "Audit log file path is required"),
  max_size: z.number().int().positive().optional(),
});

const configSchema = z.object({
  sources: z.array(sourceConfigSchema).min(1, "At least one source is required"),
  prisma_schema_path: z.string().optional(),
  audit_log: auditLogSchema.optional(),
});

export function expandEnvVars(value: string): string {
  return value.replace(/\$\{([^}]+)\}|\$([A-Z_][A-Z0-9_]*)/gi, (match, braced, bare) => {
    const varName = braced ?? bare;
    const envValue = process.env[varName];
    if (envValue === undefined) {
      throw new Error(`Environment variable ${varName} is not set (referenced in config)`);
    }
    return envValue;
  });
}

export function expandTilde(filePath: string): string {
  if (filePath.startsWith("~")) {
    return path.join(os.homedir(), filePath.slice(1));
  }
  return filePath;
}

function toSourceConfig(raw: z.infer<typeof sourceConfigSchema>): SourceConfig {
  const dsn = expandEnvVars(raw.dsn);
  const sshKey = raw.ssh_key ? expandTilde(raw.ssh_key) : undefined;

  const sessionVars = raw.session_vars
    ? Object.fromEntries(
        Object.entries(raw.session_vars).map(([k, v]) => [k, expandEnvVars(v)])
      )
    : undefined;

  return {
    id: raw.id,
    dsn,
    readonly: raw.readonly,
    maxRows: raw.max_rows,
    timeout: raw.timeout,
    poolMax: raw.pool_max ?? DEFAULT_POOL_MAX,
    poolMaxExplicit: raw.pool_max !== undefined,
    maxResponseBytes: raw.max_response_bytes,
    allowMultiStatements: raw.allow_multi_statements,
    role: raw.role,
    sessionVars,
    sshHost: raw.ssh_host,
    sshUser: raw.ssh_user,
    sshKey,
  };
}

function toAuditLogConfig(raw: z.infer<typeof auditLogSchema>): AuditLogConfig {
  return {
    logFile: expandTilde(raw.log_file),
    maxSize: raw.max_size,
  };
}

export function loadConfig(filePath: string): Config {
  let content: string;
  try {
    content = fs.readFileSync(filePath, "utf-8");
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`Failed to read config file "${filePath}": ${message}`);
  }

  let parsed: unknown;
  try {
    parsed = parseToml(content);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`Failed to parse TOML in "${filePath}": ${message}`);
  }

  const result = configSchema.safeParse(parsed);
  if (!result.success) {
    const issues = result.error.issues
      .map((i) => `  - ${i.path.join(".")}: ${i.message}`)
      .join("\n");
    throw new Error(`Invalid config in "${filePath}":\n${issues}`);
  }

  const sources = result.data.sources.map(toSourceConfig);
  const prismaSchemaPath = result.data.prisma_schema_path
    ? expandTilde(result.data.prisma_schema_path)
    : undefined;
  const auditLog = result.data.audit_log ? toAuditLogConfig(result.data.audit_log) : undefined;

  return { sources, prismaSchemaPath, auditLog };
}

/**
 * Raise the pool ceiling for sources that never named one, because under --http a pool_max of 1
 * makes every client wait its turn on a single connection and past the MCP client's 60s request
 * timeout that surfaces as an unexplained failure rather than as slowness.
 */
export function applyHttpPoolDefaults(config: Config): Config {
  const sources = config.sources.map((source) => {
    if (source.poolMaxExplicit) {
      if (source.poolMax < 4) {
        logger.warn(
          `Source "${source.id}" sets pool_max = ${source.poolMax}. Under --http that is the ceiling for ` +
            "the whole server, so concurrent clients will serialize on it."
        );
      }
      return source;
    }
    logger.info(
      `Source "${source.id}": raising pool_max ${source.poolMax} -> ${HTTP_DEFAULT_POOL_MAX} for --http ` +
        "(shared across clients; set pool_max explicitly to override)"
    );
    return { ...source, poolMax: HTTP_DEFAULT_POOL_MAX };
  });

  return { ...config, sources };
}
