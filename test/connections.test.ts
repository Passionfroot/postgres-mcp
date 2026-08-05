import { EventEmitter } from "node:events";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { SourceConfig } from "../src/types.js";

// vi.mock factories are hoisted above imports, so shared state must come from vi.hoisted.
const shared = vi.hoisted(() => ({
  poolConfigs: [] as Record<string, unknown>[],
  poolErrorHandlers: [] as ((err: Error) => void)[],
  sshInstances: [] as EventEmitter[],
  sshEndCalls: 0,
  // When true, the next pg.Pool construction kills the most recent ssh tunnel synchronously from
  // inside its own constructor -- i.e. after createTunnel() resolved but before ConnectionManager
  // registers the pool entry.
  killTunnelOnPoolConstruction: false,
  // When true, the next ssh connect() fails instead of becoming ready, simulating a tunnel that
  // never establishes (e.g. the bastion refuses the connection).
  failNextConnect: false,
}));

vi.mock("ssh2", async () => {
  const { EventEmitter: EE } = await import("node:events");
  class FakeSSH extends EE {
    constructor() {
      super();
      shared.sshInstances.push(this);
    }
    connect() {
      if (shared.failNextConnect) {
        shared.failNextConnect = false;
        setImmediate(() => this.emit("error", new Error("connection refused")));
        return this;
      }
      setImmediate(() => this.emit("ready"));
      return this;
    }
    forwardOut() {}
    // Real ssh2 end() closes the socket, and "close" lands once the TCP close completes; emitting
    // it manually lets a test control exactly when it lands relative to recreation.
    end() {
      shared.sshEndCalls += 1;
    }
  }
  return { default: { Client: FakeSSH } };
});

vi.mock("node:net", async (orig) => {
  const actual = await orig<typeof import("node:net")>();
  const { EventEmitter: EE } = await import("node:events");
  let nextPort = 15432;
  class FakeServer extends EE {
    port = nextPort++;
    listen(_p: number, _h: string, cb: () => void) {
      setImmediate(cb);
      return this;
    }
    address() {
      return { port: this.port, family: "IPv4", address: "127.0.0.1" };
    }
    close(cb?: () => void) {
      cb?.();
      return this;
    }
  }
  const createServer = () => new FakeServer();
  return {
    ...actual,
    default: { ...actual.default, createServer },
    createServer,
  };
});

vi.mock("node:fs", async (orig) => {
  const actual = await orig<typeof import("node:fs")>();
  const readFileSync = () => Buffer.from("fake-key");
  return {
    ...actual,
    default: { ...actual.default, readFileSync },
    readFileSync,
  };
});

vi.mock("pg", () => {
  class FakePool {
    constructor(config: Record<string, unknown>) {
      shared.poolConfigs.push(config);
      if (shared.killTunnelOnPoolConstruction) {
        shared.killTunnelOnPoolConstruction = false;
        shared.sshInstances.at(-1)!.emit("close");
      }
    }
    on(event: string, handler: (err: Error) => void) {
      if (event === "error") shared.poolErrorHandlers.push(handler);
    }
    async connect() {
      return { query: async () => ({ rows: [] }), release() {} };
    }
    async end() {}
  }
  return {
    default: {
      Pool: FakePool,
      // connections.ts registers tz-naive type parsers at import time.
      types: {
        setTypeParser: () => {},
        builtins: { DATE: 1082, TIMESTAMP: 1114 },
      },
    },
  };
});

const { ConnectionManager } = await import("../src/connections.js");

function source(overrides: Partial<SourceConfig> = {}): SourceConfig {
  return {
    id: "test",
    dsn: "postgres://localhost/test",
    readonly: false,
    timeout: 10,
    maxRows: 1000,
    poolMax: 1,
    allowMultiStatements: false,
    ...overrides,
  } as SourceConfig;
}

function tunneledSource(overrides: Partial<SourceConfig> = {}): SourceConfig {
  return source({
    dsn: "postgres://u:p@db.internal:5432/test",
    sshHost: "bastion",
    sshUser: "u",
    sshKey: "/fake/key",
    ...overrides,
  });
}

function resetShared() {
  shared.poolConfigs.length = 0;
  shared.poolErrorHandlers.length = 0;
  shared.sshInstances.length = 0;
  shared.sshEndCalls = 0;
  shared.killTunnelOnPoolConstruction = false;
  shared.failNextConnect = false;
}

describe("pool timeouts", () => {
  beforeEach(resetShared);

  /**
   * These three collapsed onto one value is what broke queries against a healthy database:
   * connectionTimeoutMillis also bounds pg-pool's queue wait, so at pool_max 1 a second concurrent
   * query failed with "timeout exceeded when trying to connect" while the first was still running.
   */
  it("keeps statement_timeout < query_timeout < connectionTimeoutMillis", async () => {
    const manager = new ConnectionManager([source({ timeout: 10 })]);
    await manager.getPool("test");

    const config = shared.poolConfigs[0];
    const statement = config.statement_timeout as number;
    const query = config.query_timeout as number;
    const connect = config.connectionTimeoutMillis as number;

    expect(statement).toBe(10_000);
    expect(query).toBeGreaterThan(statement);
    expect(connect).toBeGreaterThan(query);
  });

  it("leaves room for a query that runs the full statement_timeout to be queued behind", async () => {
    const manager = new ConnectionManager([source({ timeout: 30 })]);
    await manager.getPool("test");

    const config = shared.poolConfigs[0];
    // A second query queued behind one that runs the full statement_timeout must not be failed by
    // the acquire timer.
    expect(config.connectionTimeoutMillis as number).toBeGreaterThan(30_000);
  });

  it("scales the timeouts with the source timeout", async () => {
    const manager = new ConnectionManager([source({ timeout: 5 })]);
    await manager.getPool("test");

    expect(shared.poolConfigs[0].statement_timeout).toBe(5_000);
    expect(shared.poolConfigs[0].query_timeout as number).toBeGreaterThan(
      5_000
    );
  });
});

describe("tunnel recreation races", () => {
  const tick = () => new Promise((r) => setImmediate(() => setImmediate(r)));

  beforeEach(resetShared);

  it("a stale close from a superseded tunnel does not kill the pool that replaced it", async () => {
    const manager = new ConnectionManager([tunneledSource()]);

    await manager.getPool("test");
    expect(shared.poolConfigs.length).toBe(1);
    const oldSsh = shared.sshInstances[0];

    // A pool-level error marks the pool dead while the tunnel is still alive.
    shared.poolErrorHandlers[0](new Error("idle client error"));

    // Recreate: destroyPoolAndTunnel ends the old pool and calls ssh.end() on the old tunnel, then
    // a fresh tunnel + pool are built and stored under the same source id.
    await manager.getPool("test");
    expect(shared.poolConfigs.length).toBe(2);
    expect(shared.sshInstances.length).toBe(2);

    // The OLD ssh connection's close now lands (a FIN to an unreachable peer can sit in FIN_WAIT
    // for a while). It must not be able to mark whatever pool is currently registered -- the
    // healthy pool 2 -- dead.
    oldSsh.emit("close");
    await tick();

    const pool = await manager.getPool("test");
    expect(pool).toBe(await manager.getPool("test"));
    expect(shared.poolConfigs.length).toBe(2);
    expect(shared.sshInstances.length).toBe(2);
  });

  it("tunnel.close() does not fire onDown for its own resulting close event", async () => {
    const manager = new ConnectionManager([tunneledSource()]);

    await manager.getPool("test");
    shared.poolErrorHandlers[0](new Error("idle client error"));

    // destroyPoolAndTunnel awaits tunnel.close(), which calls ssh.end() -- our fake client counts
    // that call but (unlike real ssh2) does not itself emit "close". Emit it here to model the
    // real async "close" landing after the intentional shutdown already completed.
    await manager.getPool("test");
    expect(shared.sshEndCalls).toBe(1);

    shared.sshInstances[0].emit("close");
    await tick();

    // Still just the recreated pool; the self-inflicted close must not trigger another teardown.
    expect(shared.poolConfigs.length).toBe(2);
  });

  it("recovers when the tunnel dies in the gap between resolving and pool registration", async () => {
    const manager = new ConnectionManager([tunneledSource()]);
    // Kills the tunnel synchronously from inside the (fake) pg.Pool constructor -- after
    // createTunnel() resolved but before ConnectionManager registers the pool entry.
    shared.killTunnelOnPoolConstruction = true;

    await manager.getPool("test");
    expect(shared.poolConfigs.length).toBe(1);

    // A correct implementation recreates on the next access instead of wedging on a pool that was
    // registered "alive" over an already-dead tunnel.
    await manager.getPool("test");
    expect(shared.poolConfigs.length).toBe(2);
  });
});

describe("concurrent getPool", () => {
  beforeEach(resetShared);

  it("two concurrent first calls for the same source build only one tunnel and one pool", async () => {
    const manager = new ConnectionManager([tunneledSource()]);

    const [a, b] = await Promise.all([
      manager.getPool("test"),
      manager.getPool("test"),
    ]);

    expect(shared.sshInstances.length).toBe(1);
    expect(shared.poolConfigs.length).toBe(1);
    expect(a).toBe(b);
  });

  it("two concurrent calls on a dead pool recreate only once", async () => {
    const manager = new ConnectionManager([tunneledSource()]);
    await manager.getPool("test");
    shared.poolErrorHandlers[0](new Error("idle client error"));

    const [a, b] = await Promise.all([
      manager.getPool("test"),
      manager.getPool("test"),
    ]);

    expect(shared.poolConfigs.length).toBe(2);
    expect(shared.sshInstances.length).toBe(2);
    expect(a).toBe(b);
  });
});

describe("getPool caching", () => {
  beforeEach(resetShared);

  it("returns the cached pool on later sequential calls", async () => {
    const manager = new ConnectionManager([tunneledSource()]);

    const first = await manager.getPool("test");
    const second = await manager.getPool("test");

    expect(shared.sshInstances.length).toBe(1);
    expect(second).toBe(first);
  });

  it("does not cache a failed creation", async () => {
    const manager = new ConnectionManager([tunneledSource()]);
    shared.failNextConnect = true;

    await expect(manager.getPool("test")).rejects.toThrow("connection refused");
    await expect(manager.getPool("test")).resolves.toBeDefined();
    expect(shared.sshInstances.length).toBe(2);
  });
});
