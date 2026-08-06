import fs from "node:fs";

export const DEFAULT_PORT = 7803;
export const DEFAULT_HOST = "127.0.0.1";

export function printUsage() {
  console.error("Usage: postgres-mcp <config-file> [options]");
  console.error("  config-file        Path to TOML configuration file");
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

export interface ParsedArgs {
  configPath: string;
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

  const configPath = positional[0];
  if (!configPath) return undefined;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    console.error(`Invalid port: ${port}`);
    return undefined;
  }

  return { configPath, useHttp, host, port, token };
}
