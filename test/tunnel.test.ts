import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";

// vi.mock factories are hoisted above imports, so shared state must come from vi.hoisted.
const shared = vi.hoisted(() => ({
  sshInstances: [] as EventEmitter[],
  // autoReady off lets a test drive the pre-establishment path (emit "error" before "ready").
  autoReady: true,
  // When true, forwardOut throws synchronously ("Not connected"), as ssh2 does on a dead client.
  forwardOutThrows: false,
  // The proxy's per-connection handler, captured so a test can drive an incoming connection.
  connectionListener: undefined as ((socket: unknown) => void) | undefined,
}));

vi.mock("ssh2", async () => {
  const { EventEmitter: EE } = await import("node:events");
  class FakeSSH extends EE {
    constructor() {
      super();
      shared.sshInstances.push(this);
    }
    connect() {
      if (shared.autoReady) setImmediate(() => this.emit("ready"));
      return this;
    }
    forwardOut(
      _a: string,
      _b: number,
      _c: string,
      _d: number,
      cb: (err: Error | undefined, stream: EventEmitter) => void
    ) {
      if (shared.forwardOutThrows) throw new Error("Not connected");
      cb(undefined, new EE());
    }
    end() {}
  }
  return { default: { Client: FakeSSH } };
});

vi.mock("node:net", async (orig) => {
  const actual = await orig<typeof import("node:net")>();
  const { EventEmitter: EE } = await import("node:events");
  class FakeServer extends EE {
    listen(_port: number, _host: string, cb: () => void) {
      setImmediate(cb);
      return this;
    }
    address() {
      return { port: 15432, family: "IPv4", address: "127.0.0.1" };
    }
    close(cb?: () => void) {
      cb?.();
      return this;
    }
  }
  const createServer = (listener: (socket: unknown) => void) => {
    shared.connectionListener = listener;
    return new FakeServer();
  };
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

import { createTunnel } from "../src/tunnel.js";

const cfg = {
  sshHost: "bastion",
  sshUser: "u",
  sshKeyPath: "/fake/key",
  remoteHost: "db.internal",
  remotePort: 5432,
  keepaliveInterval: 30_000,
};

afterEach(() => {
  shared.autoReady = true;
  shared.forwardOutThrows = false;
  shared.connectionListener = undefined;
  shared.sshInstances.length = 0;
});

describe("createTunnel", () => {
  it("resolves once ssh is ready and the proxy is listening", async () => {
    const handle = await createTunnel(cfg);
    expect(handle.localHost).toBe("127.0.0.1");
    expect(handle.localPort).toBe(15432);
  });

  it("destroys the socket instead of crashing when forwardOut throws on a dead tunnel", async () => {
    await createTunnel(cfg);
    shared.forwardOutThrows = true;
    const socket = Object.assign(new EventEmitter(), {
      remoteAddress: "127.0.0.1",
      remotePort: 1234,
      destroy: vi.fn(),
      pipe: vi.fn(),
    });
    // Driving an incoming proxy connection must not throw out of the handler (which would crash
    // the process as an uncaught exception); the socket is destroyed instead.
    expect(() => shared.connectionListener!(socket)).not.toThrow();
    expect(socket.destroy).toHaveBeenCalled();
  });

  it("routes a post-establishment ssh error to onDown instead of a rejection", async () => {
    const onDown = vi.fn();
    await createTunnel(cfg, onDown);
    shared.sshInstances.at(-1)!.emit("error", new Error("socket hang up"));
    expect(onDown).toHaveBeenCalledTimes(1);
    expect(onDown).toHaveBeenCalledWith(
      expect.stringContaining("socket hang up")
    );
  });

  it("routes a post-establishment close to onDown", async () => {
    const onDown = vi.fn();
    await createTunnel(cfg, onDown);
    shared.sshInstances.at(-1)!.emit("close");
    expect(onDown).toHaveBeenCalledTimes(1);
    expect(onDown).toHaveBeenCalledWith(expect.stringContaining("closed"));
  });

  it("rejects and does NOT call onDown when ssh errors before establishment", async () => {
    shared.autoReady = false;
    const onDown = vi.fn();
    const pending = createTunnel(cfg, onDown);
    shared.sshInstances.at(-1)!.emit("error", new Error("auth failed"));
    await expect(pending).rejects.toThrow(/auth failed/);
    expect(onDown).not.toHaveBeenCalled();
  });

  it("does not call onDown for the close event resulting from its own close()", async () => {
    const onDown = vi.fn();
    const handle = await createTunnel(cfg, onDown);

    await handle.close();
    // Real ssh2 emits "close" asynchronously once end() finishes tearing down the TCP socket, well
    // after close() has already resolved. Without a closing flag this is indistinguishable from an
    // unexpected death and fires onDown again for a tunnel that was intentionally shut down.
    shared.sshInstances.at(-1)!.emit("close");
    expect(onDown).not.toHaveBeenCalled();
  });

  it("does not call onDown for an error firing after its own close()", async () => {
    const onDown = vi.fn();
    const handle = await createTunnel(cfg, onDown);

    await handle.close();
    shared.sshInstances.at(-1)!.emit("error", new Error("socket hang up"));
    expect(onDown).not.toHaveBeenCalled();
  });
});
