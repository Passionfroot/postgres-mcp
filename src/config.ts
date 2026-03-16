import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { parse as parseToml } from "smol-toml";
import { z } from "zod";

import type { AuditLogConfig, Config, SourceConfig } from "./types.js";

export const sourceConfigSchema = z.object({
  id: z.string().min(1, "Source id is required"),
  dsn: z.string().min(1, "Source dsn is required"),
  readonly: z.boolean().optional().default(false),
  max_rows: z.number().int().positive().optional().default(1000),
  timeout: z.number().positive().optional().default(10),
  pool_max: z.number().int().positive().optional().default(1),
  allow_multi_statements: z.boolean().optional().default(false),
  role: z.string().min(1).optional(),
  session_vars: z.record(z.string().min(1), z.string()).optional(),
  ssh_host: z.string().optional(),
  ssh_user: z.string().optional(),
  ssh_key: z.string().optional(),
});

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
    poolMax: raw.pool_max,
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
