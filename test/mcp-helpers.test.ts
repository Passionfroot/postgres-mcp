import { describe, expect, it } from "vitest";

import type { QueryResult } from "../src/query.js";

import { serializeQueryResult, truncateText } from "../src/mcp-helpers.js";

function wideResult(rows: number, width: number): QueryResult {
  return {
    rows: Array.from({ length: rows }, (_, i) => ({ id: i, v: "x".repeat(width) })),
    rowCount: rows,
    truncated: false,
  };
}

describe("serializeQueryResult", () => {
  it("returns the whole result when it fits", () => {
    const parsed = JSON.parse(serializeQueryResult(wideResult(3, 10), 1_000_000));

    expect(parsed.rowCount).toBe(3);
    expect(parsed.truncated).toBe(false);
    expect(parsed.truncatedReason).toBeUndefined();
  });

  it("caps the response in bytes, which max_rows alone does not", () => {
    // 1000 rows of 100 kB is the ~95 MB response that max_rows = 1000 happily allowed.
    const serialized = serializeQueryResult(wideResult(1000, 100_000), 1_000_000);

    expect(Buffer.byteLength(serialized)).toBeLessThanOrEqual(1_000_000);
    const parsed = JSON.parse(serialized);
    expect(parsed.truncated).toBe(true);
    expect(parsed.rows).toHaveLength(parsed.rowCount);
    expect(parsed.rowCount).toBeLessThan(1000);
    expect(parsed.truncatedReason).toContain("max_response_bytes");
  });

  it("honours a per-source limit", () => {
    const serialized = serializeQueryResult(wideResult(1000, 1000), 50_000);

    expect(Buffer.byteLength(serialized)).toBeLessThanOrEqual(50_000);
  });

  it("still returns valid JSON when even one row is over the limit", () => {
    const parsed = JSON.parse(serializeQueryResult(wideResult(5, 100_000), 1000));

    expect(parsed.rowCount).toBe(0);
    expect(parsed.truncated).toBe(true);
  });
});

describe("truncateText", () => {
  it("leaves text under the limit alone", () => {
    expect(truncateText("hello", 1000)).toBe("hello");
  });

  it("caps longer text and marks it", () => {
    const capped = truncateText("x".repeat(10_000), 500);

    expect(Buffer.byteLength(capped)).toBeLessThanOrEqual(500);
    expect(capped).toContain("[truncated:");
  });
});
