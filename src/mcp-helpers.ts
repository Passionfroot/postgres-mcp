import type { Config, SourceConfig } from "./types.js";

type ResolveSourceResult =
  | { ok: true; source: SourceConfig }
  | { ok: false; error: ReturnType<typeof mcpErrorResult> };

export function mcpTextResult(text: string) {
  return { content: [{ type: "text" as const, text }] };
}

export function mcpErrorResult(message: string) {
  return { content: [{ type: "text" as const, text: message }], isError: true as const };
}

export function resolveSource(database: string, config: Config): ResolveSourceResult {
  const source = config.sources.find((s) => s.id === database);
  if (!source) {
    const available = config.sources.map((s) => s.id).join(", ");
    return {
      ok: false,
      error: mcpErrorResult(`Unknown database '${database}'. Available: ${available}`),
    };
  }
  return { ok: true, source };
}
