/**
 * Validate that a PostgreSQL configuration parameter name (GUC) is safe.
 * Custom GUCs follow the pattern `extension.parameter` — only alphanumeric, underscores, and dots.
 */
const SAFE_GUC_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/;

export function assertSafeGucName(name: string): void {
  if (!SAFE_GUC_NAME_RE.test(name)) {
    throw new Error(`Invalid session variable name: ${JSON.stringify(name)}`);
  }
}

/** Double-quote a SQL identifier (role name). Escapes embedded double quotes. */
export function escapeIdentifier(str: string): string {
  return `"${str.replace(/"/g, '""')}"`;
}

/** Single-quote a SQL literal value. Escapes embedded single quotes. */
export function escapeLiteral(str: string): string {
  return `'${str.replace(/'/g, "''")}'`;
}
