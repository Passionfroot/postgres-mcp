import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { SourceConfig } from "../src/types.js";

const createTunnel = vi.hoisted(() => vi.fn());

vi.mock("../src/tunnel.js", async () => {
  const actual = await vi.importActual<typeof import("../src/tunnel.js")>("../src/tunnel.js");
  return { ...actual, createTunnel };
});

const { ConnectionManager } = await import("../src/connections.js");

const tunneledSource: SourceConfig = {
  id: "tunneled",
  // Never connected to: nothing in these tests runs a query.
  dsn: "postgresql://user:pw@db.internal:5432/app",
  readonly: true,
  maxRows: 10,
  timeout: 5,
  poolMax: 1,
  poolMaxExplicit: false,
  maxResponseBytes: 1_000_000,
  allowMultiStatements: false,
  sshHost: "bastion.example.com",
  sshUser: "deploy",
  sshKey: "/dev/null",
};

let manager: InstanceType<typeof ConnectionManager>;

beforeEach(() => {
  createTunnel.mockReset();
  createTunnel.mockImplementation(async () => {
    // The real tunnel takes ~200ms to come up. That await is the race window.
    await new Promise((resolve) => setTimeout(resolve, 50));
    return { localHost: "127.0.0.1", localPort: 1, close: async () => undefined };
  });
});

afterEach(async () => {
  await manager?.shutdown();
});

describe("getPool", () => {
  it("creates exactly one tunnel and one pool for concurrent cold starts", async () => {
    manager = new ConnectionManager([tunneledSource]);

    const pools = await Promise.all(Array.from({ length: 10 }, () => manager.getPool("tunneled")));

    expect(createTunnel).toHaveBeenCalledTimes(1);
    expect(new Set(pools).size).toBe(1);
  });

  it("still returns the cached pool on later calls", async () => {
    manager = new ConnectionManager([tunneledSource]);

    const first = await manager.getPool("tunneled");
    const second = await manager.getPool("tunneled");

    expect(createTunnel).toHaveBeenCalledTimes(1);
    expect(second).toBe(first);
  });

  it("does not cache a failed creation", async () => {
    manager = new ConnectionManager([tunneledSource]);
    createTunnel.mockRejectedValueOnce(new Error("ssh refused"));

    await expect(manager.getPool("tunneled")).rejects.toThrow("ssh refused");
    await expect(manager.getPool("tunneled")).resolves.toBeDefined();
    expect(createTunnel).toHaveBeenCalledTimes(2);
  });
});

describe("pool timeouts", () => {
  it("orders statement_timeout below query_timeout below connectionTimeoutMillis", async () => {
    manager = new ConnectionManager([{ ...tunneledSource, timeout: 30 }]);

    const pool = await manager.getPool("tunneled");
    const { statement_timeout: statement, query_timeout: query, connectionTimeoutMillis: connect } =
      pool.options as unknown as {
        statement_timeout: number;
        query_timeout: number;
        connectionTimeoutMillis: number;
      };

    expect(statement).toBe(30_000);
    expect(query).toBe(32_000);
    expect(connect).toBe(37_000);
    expect(statement).toBeLessThan(query);
    expect(query).toBeLessThan(connect);
  });
});
