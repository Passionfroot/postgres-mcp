import fs from "node:fs";
import path from "node:path";

import type { AuditLogConfig } from "./types.js";

import { logger } from "./logger.js";

interface AuditEntry {
  source: string;
  sql: string;
  durationMs: number;
  rowCount: number;
  truncated: boolean;
  error?: string;
}

function formatEntry(entry: AuditEntry) {
  const ts = new Date().toISOString();
  return JSON.stringify({ ts, ...entry }) + "\n";
}

function rotateIfNeeded(filePath: string, maxSize: number) {
  try {
    const stats = fs.statSync(filePath);
    if (stats.size < maxSize) return;

    const rotatedPath = `${filePath}.1`;
    // Simple single-file rotation: current -> .1, then start fresh
    fs.renameSync(filePath, rotatedPath);
  } catch (err) {
    // File doesn't exist yet or stat failed — nothing to rotate
    if (err instanceof Error && "code" in err && (err as NodeJS.ErrnoException).code === "ENOENT") {
      return;
    }
    logger.error(`Audit log rotation failed: ${err instanceof Error ? err.message : String(err)}`);
  }
}

export class AuditLog {
  private filePath: string;
  private maxSize?: number;
  private fd: number | null = null;

  constructor(config: AuditLogConfig) {
    this.filePath = config.logFile;
    this.maxSize = config.maxSize;

    // Ensure parent directory exists
    const dir = path.dirname(this.filePath);
    fs.mkdirSync(dir, { recursive: true });

    this.fd = fs.openSync(this.filePath, "a");
    logger.info(
      `Audit logging to "${this.filePath}"${this.maxSize ? ` (max ${this.maxSize} bytes)` : " (unlimited)"}`
    );
  }

  log(entry: AuditEntry) {
    if (this.fd === null) return;

    try {
      if (this.maxSize) {
        rotateIfNeeded(this.filePath, this.maxSize);
        // Reopen after potential rotation
        fs.closeSync(this.fd);
        this.fd = fs.openSync(this.filePath, "a");
      }

      fs.writeSync(this.fd, formatEntry(entry));
    } catch (err) {
      logger.error(`Audit log write failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  close() {
    if (this.fd !== null) {
      try {
        fs.closeSync(this.fd);
      } catch {
        // ignore close errors during shutdown
      }
      this.fd = null;
    }
  }
}

/** No-op audit log when audit logging is not configured. */
export class NullAuditLog {
  log(_entry: AuditEntry) {}
  close() {}
}

export type AuditLogger = AuditLog | NullAuditLog;

export function createAuditLog(config?: AuditLogConfig): AuditLogger {
  if (!config) return new NullAuditLog();
  return new AuditLog(config);
}
