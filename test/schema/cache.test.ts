import type pg from "pg";

import { beforeEach, describe, expect, it, vi } from "vitest";

const introspectDatabase = vi.hoisted(() => vi.fn());

vi.mock("../../src/schema/introspect.js", () => ({ introspectDatabase }));

const { SchemaCache } = await import("../../src/schema/cache.js");

const emptyMapping = { models: [], enums: [] };
const fakePool = {} as pg.Pool;

beforeEach(() => {
  introspectDatabase.mockReset();
  introspectDatabase.mockImplementation(async () => {
    // Introspection is a real round trip. That await is the race window.
    await new Promise((resolve) => setTimeout(resolve, 50));
    return { columns: [], primaryKeys: [], foreignKeys: [], enumValues: [] };
  });
});

describe("SchemaCache.get", () => {
  it("introspects once for concurrent first calls on the same database", async () => {
    const cache = new SchemaCache(emptyMapping);

    const results = await Promise.all(Array.from({ length: 10 }, () => cache.get("main", fakePool)));

    expect(introspectDatabase).toHaveBeenCalledTimes(1);
    expect(new Set(results).size).toBe(1);
  });

  it("keys the in-flight introspection per database", async () => {
    const cache = new SchemaCache(emptyMapping);

    await Promise.all([cache.get("a", fakePool), cache.get("b", fakePool), cache.get("a", fakePool)]);

    expect(introspectDatabase).toHaveBeenCalledTimes(2);
  });

  it("serves later calls from the cache", async () => {
    const cache = new SchemaCache(emptyMapping);

    const first = await cache.get("main", fakePool);
    const second = await cache.get("main", fakePool);

    expect(introspectDatabase).toHaveBeenCalledTimes(1);
    expect(second).toBe(first);
  });

  it("re-introspects after a failure instead of caching the rejection", async () => {
    const cache = new SchemaCache(emptyMapping);
    introspectDatabase.mockRejectedValueOnce(new Error("connection refused"));

    await expect(cache.get("main", fakePool)).rejects.toThrow("connection refused");
    await expect(cache.get("main", fakePool)).resolves.toBeDefined();
    expect(introspectDatabase).toHaveBeenCalledTimes(2);
  });

  it("re-introspects after clear()", async () => {
    const cache = new SchemaCache(emptyMapping);

    await cache.get("main", fakePool);
    cache.clear("main");
    await cache.get("main", fakePool);

    expect(introspectDatabase).toHaveBeenCalledTimes(2);
  });
});
