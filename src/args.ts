import fs from "node:fs";

export const DEFAULT_PORT = 7803;
export const DEFAULT_HOST = "127.0.0.1";

/** A path, which is what a variable named after a config already means to anyone passing one. */
export const CONFIG_PATH_ENV_VAR = "POSTGRES_MCP_CONFIG";
/** One connection string, for a host that has nowhere to put a file. */
export const DSN_ENV_VAR = "POSTGRES_MCP_DSN";

export function printUsage() {
  console.error("Usage: postgres-mcp [config-file] [options]");
  console.error("  config-file        Path to TOML configuration file. Omit it and the config");
  console.error(`                     comes from ${DSN_ENV_VAR}, one read-only connection`);
  console.error(`                     string, or from ${CONFIG_PATH_ENV_VAR}, a path.`);
  console.error("                     An argument wins over both.");
  console.error("");
  console.error("Options:");
  console.error("  --stdio            Serve over stdio, one process per client (default)");
  console.error("  --http             Serve over Streamable HTTP, shared across clients");
  console.error(`  --port <n>         HTTP port (default ${DEFAULT_PORT}, env POSTGRES_MCP_PORT)`);
  console.error(`  --host <addr>      HTTP bind address (default ${DEFAULT_HOST}, env POSTGRES_MCP_HOST)`);
  console.error("  --token <secret>   Require 'Authorization: Bearer <secret>' (env POSTGRES_MCP_TOKEN)");
  console.error("  --token-file <p>   Read the token from a file, so it is not visible in ps");
  console.error("");
  console.error("Note: --http is refused when any source sets session_vars. Those pin a");
  console.error("per-tenant identity to the process, which a shared server cannot honour.");
  console.error("");
  console.error("Note: under --http, pool_max is a per-server budget shared by every client, not");
  console.error("a per-client one. Sources that do not set it get a larger default in HTTP mode.");
}

/** Where the config text comes from, resolved once so nothing downstream re-reads the environment. */
export type ConfigSource = { kind: "file"; path: string } | { kind: "dsn"; dsn: string };

export interface ParsedArgs {
  configSource: ConfigSource;
  useHttp: boolean;
  host: string;
  port: number;
  token?: string;
}

export function parseArgs(argv: string[]): ParsedArgs | undefined {
  const positional: string[] = [];
  // stdio stays the default: the per-tenant production path depends on it.
  let useHttp = false;
  let host = process.env.POSTGRES_MCP_HOST ?? DEFAULT_HOST;
  let port = Number(process.env.POSTGRES_MCP_PORT ?? DEFAULT_PORT);
  let token = process.env.POSTGRES_MCP_TOKEN;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--http") useHttp = true;
    else if (arg === "--stdio") useHttp = false;
    else if (arg === "--port") port = Number(argv[++i]);
    else if (arg === "--host") host = argv[++i];
    else if (arg === "--token") token = argv[++i];
    else if (arg === "--token-file") token = fs.readFileSync(argv[++i], "utf-8").trim();
    else if (arg === "--help" || arg === "-h") return undefined;
    else if (arg.startsWith("-")) {
      // Warn rather than exit: the previous release read only argv[2] and silently ignored
      // everything else, so a hard failure here would stop pinned consumers from starting.
      console.error(`Ignoring unknown option: ${arg}`);
    } else positional.push(arg);
  }

  const configSource = resolveConfigSource(positional[0]);
  if (!configSource) return undefined;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    console.error(`Invalid port: ${port}`);
    return undefined;
  }

  return { configSource, useHttp, host, port, token };
}

/**
 * An argument wins over both variables, being the most deliberate of the three, and a connection
 * string wins over the path variable, which a shell profile may have exported for every process.
 * An argument that is present but empty (a wrapper interpolating an unset shell variable) counts
 * as absent, the way it did when a missing path was the only thing that printed usage.
 */
function resolveConfigSource(positionalPath: string | undefined): ConfigSource | undefined {
  const path = positionalPath?.trim();
  if (path) return { kind: "file", path };

  const dsn = process.env[DSN_ENV_VAR]?.trim();
  if (dsn) return { kind: "dsn", dsn };

  const pathFromEnv = process.env[CONFIG_PATH_ENV_VAR]?.trim();
  if (pathFromEnv) return { kind: "file", path: pathFromEnv };

  return undefined;
}
