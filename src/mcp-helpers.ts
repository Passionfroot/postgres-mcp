import type { QueryResult } from "./query.js";
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

/**
 * max_rows bounds the row count, not the byte count: 1000 rows of 100 kB text is still a ~95 MB
 * response. No MCP client can use one, and on a server shared across clients building it costs
 * every other client hundreds of megabytes of heap. Drop rows until it fits and say so.
 */
export function serializeQueryResult(result: QueryResult, maxResponseBytes: number) {
  // Size the rows one at a time and stop at the budget. Stringifying the whole result first, just
  // to measure it, is what allocated the 95 MB string this cap exists to prevent.
  const budget = Math.floor(maxResponseBytes * 0.9);
  let used = 0;
  let kept = 0;
  for (const row of result.rows) {
    used += Buffer.byteLength(JSON.stringify(row, null, 2)) + 6;
    if (used > budget) break;
    kept++;
  }

  if (kept === result.rows.length) return JSON.stringify(result, null, 2);

  return JSON.stringify(
    {
      rows: result.rows.slice(0, kept),
      rowCount: kept,
      truncated: true,
      truncatedReason:
        `Response exceeded max_response_bytes (${maxResponseBytes}), so ${result.rowCount - kept} of ` +
        `${result.rowCount} rows were dropped. Select fewer columns, or use left(col, n) on wide text ` +
        "columns, to get the whole result.",
    },
    null,
    2
  );
}

/** Byte-cap a formatted text response, with a marker so the caller knows it is incomplete. */
export function truncateText(text: string, maxResponseBytes: number) {
  if (Buffer.byteLength(text) <= maxResponseBytes) return text;
  const marker = `\n\n[truncated: output exceeded max_response_bytes (${maxResponseBytes})]`;
  const keep = maxResponseBytes - Buffer.byteLength(marker);
  return Buffer.from(text, "utf8").subarray(0, keep).toString("utf8") + marker;
}
