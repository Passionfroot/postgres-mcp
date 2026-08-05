import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { createServer as createHttpServer, type IncomingMessage, type ServerResponse } from "node:http";
import net from "node:net";
import os from "node:os";

import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";

import { logger } from "./logger.js";

export interface HttpTransportOptions {
  host: string;
  port: number;
  /** When set, every request must send `Authorization: Bearer <token>`. */
  token?: string;
  /**
   * Builds a fresh MCP server per client session. Expensive shared state stays in the
   * closure. May be async: some servers are only constructible asynchronously.
   */
  createServer: () =>
    | { connect(transport: StreamableHTTPServerTransport): Promise<void>; close(): Promise<void> }
    | Promise<{ connect(transport: StreamableHTTPServerTransport): Promise<void>; close(): Promise<void> }>;
  /** Session limits, overridable so tests can exercise them on a human timescale. */
  maxSessions?: number;
  sessionIdleMs?: number;
  sessionSweepMs?: number;
}

const MCP_PATH = "/mcp";

/**
 * The only bodies this endpoint accepts are JSON-RPC MCP messages, the largest of which is a tool
 * call carrying a SQL string. 4 MB is far above any real one and far below V8's ~512 MB string
 * limit, so an oversized body is refused before it can be buffered or stringified.
 */
export const MAX_BODY_BYTES = 4 * 1024 * 1024;

/**
 * Each session holds a whole McpServer, and nothing but an explicit DELETE used to remove one.
 * Cap the map so a peer cannot grow it without bound, and reclaim sessions no client has touched.
 */
export const MAX_SESSIONS = 256;
export const SESSION_IDLE_MS = 30 * 60_000;
export const SESSION_SWEEP_MS = 60_000;

/** How long close() lets in-flight responses finish before it cuts the remaining sockets. */
const DRAIN_TIMEOUT_MS = 2_000;

interface Session {
  transport: StreamableHTTPServerTransport;
  server: { close(): Promise<void> };
  lastSeen: number;
}

class BodyTooLargeError extends Error {}
class InvalidJsonError extends Error {}

/** `localhost`, 127.0.0.0/8 and ::1 all reach only this machine. */
export function isLoopbackHost(host: string) {
  if (host === "localhost") return true;
  const family = net.isIP(host);
  if (family === 4) return host.startsWith("127.");
  if (family === 6) {
    if (host === "::1") return true;
    const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/i.exec(host);
    return mapped ? mapped[1].startsWith("127.") : false;
  }
  return false;
}

/** IPv6 literals need brackets in a Host header and in a URL; IPv4 and names must not have them. */
export function formatHostPort(host: string, port: number) {
  return net.isIPv6(host) ? `[${host}]:${port}` : `${host}:${port}`;
}

/**
 * A client never sends the wildcard address as its own Host header; it sends whatever address
 * or name it dialed. Bound to `0.0.0.0` or `::`, the wildcard itself is not a `Host` any real
 * request can match, so list the machine's actual non-internal interfaces (and its hostname)
 * instead. Bound to a specific address, that address is what clients dial, so it alone matches.
 */
export function reachableHostPorts(host: string, port: number) {
  if (host !== "0.0.0.0" && host !== "::") {
    return [formatHostPort(host, port)];
  }

  // Lowercase: a Host header's hostname is case-insensitive, and the SDK's own transport (via
  // @hono/node-server) parses it through the URL class, which lowercases hostnames on the way
  // through and then rejects any request whose raw Host header doesn't match that lowercase
  // form byte-for-byte. os.hostname() is whatever case the machine was named with.
  const hostPorts = [formatHostPort(os.hostname().toLowerCase(), port)];
  for (const addrs of Object.values(os.networkInterfaces())) {
    for (const addr of addrs ?? []) {
      if (addr.internal) continue;
      hostPorts.push(formatHostPort(addr.address, port));
    }
  }
  return hostPorts;
}

/**
 * An error message can carry text a peer chose (a bad URL, a rejected header value). Flatten
 * control characters so it cannot forge log lines, and bound the length.
 */
export function sanitizeForLog(value: string) {
  return value.replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, 200);
}

/**
 * Reads a POST body under MAX_BODY_BYTES. Rejects on the declared Content-Length before
 * reading a byte when the client sends one; the streaming cap inside readBody covers
 * chunked uploads that declare nothing. Shared by every request path that can carry a
 * body, so none of them can fall through to the SDK's own unbounded `req.json()`.
 */
function readCappedBody(req: IncomingMessage) {
  const declaredLength = Number(req.headers["content-length"]);
  if (Number.isFinite(declaredLength) && declaredLength > MAX_BODY_BYTES) {
    throw new BodyTooLargeError(`Request body exceeds ${MAX_BODY_BYTES} bytes`);
  }
  return readBody(req);
}

function readBody(req: IncomingMessage) {
  return new Promise<unknown>((resolve, reject) => {
    let chunks: Buffer[] = [];
    let size = 0;

    req.on("data", (c: Buffer) => {
      size += c.length;
      if (size > MAX_BODY_BYTES) {
        chunks = [];
        req.pause();
        reject(new BodyTooLargeError(`Request body exceeds ${MAX_BODY_BYTES} bytes`));
        return;
      }
      chunks.push(c);
    });
    req.on("error", reject);
    req.on("end", () => {
      // Buffer.concat().toString() throws above V8's max string length. This listener runs outside
      // any promise, so the throw has to be caught here or it takes the whole process down.
      try {
        const raw = Buffer.concat(chunks).toString("utf8");
        if (!raw) return resolve(undefined);
        resolve(JSON.parse(raw));
      } catch {
        reject(new InvalidJsonError("Invalid JSON body"));
      }
    });
  });
}

function sendJson(res: ServerResponse, status: number, body: unknown) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) });
  res.end(payload);
}

function rpcError(res: ServerResponse, status: number, code: number, message: string) {
  sendJson(res, status, { jsonrpc: "2.0", error: { code, message }, id: null });
}

export async function startHttpServer(options: HttpTransportOptions) {
  const sessions = new Map<string, Session>();
  const maxSessions = options.maxSessions ?? MAX_SESSIONS;
  const sessionIdleMs = options.sessionIdleMs ?? SESSION_IDLE_MS;
  const sessionSweepMs = options.sessionSweepMs ?? SESSION_SWEEP_MS;

  // Loopback-only by default. The SDK's Host check is what actually blocks DNS rebinding; the
  // Origin check below is the spec's belt-and-braces requirement on top of it.
  const isLoopback = isLoopbackHost(options.host);
  const boundHostPort = formatHostPort(options.host, options.port);
  const allowedHosts = [
    ...new Set([
      ...reachableHostPorts(options.host, options.port),
      `localhost:${options.port}`,
      `127.0.0.1:${options.port}`,
      `[::1]:${options.port}`,
    ]),
  ];
  // The SDK only reads Origin when allowedOrigins is non-empty, and it lets a request with no
  // Origin through either way. That is deliberate here: MCP clients are not browsers and send
  // none, while a browser always does, so a cross-origin page gets 403.
  const allowedOrigins = allowedHosts.flatMap((hostPort) => [`http://${hostPort}`, `https://${hostPort}`]);

  function isAllowedHost(req: IncomingMessage) {
    const host = req.headers.host;
    return typeof host === "string" && allowedHosts.includes(host);
  }

  // On loopback the OS already limits reach to this machine. Off loopback it doesn't,
  // so refuse to expose the sources to the network without a token.
  if (!isLoopback && !options.token) {
    throw new Error(
      `Refusing to bind ${options.host} without --token. A non-loopback bind is reachable ` +
        "from the network; pass --token (or set the *_MCP_TOKEN env var), or bind 127.0.0.1."
    );
  }

  const expectedAuth = options.token
    ? createHash("sha256").update(`Bearer ${options.token}`).digest()
    : undefined;

  function isAuthorized(req: IncomingMessage) {
    if (!expectedAuth) return true;
    const header = req.headers.authorization;
    if (typeof header !== "string") return false;
    // Digest both sides so the lengths always match and the compare stays constant-time.
    return timingSafeEqual(createHash("sha256").update(header).digest(), expectedAuth);
  }

  async function closeSession(sessionId: string) {
    const session = sessions.get(sessionId);
    if (!session) return;
    sessions.delete(sessionId);
    await session.server.close().catch(() => undefined);
    logger.info(`Session closed: ${sessionId} (${sessions.size} active)`);
  }

  const sweep = setInterval(() => {
    const cutoff = Date.now() - sessionIdleMs;
    for (const [sessionId, session] of sessions) {
      if (session.lastSeen < cutoff) {
        logger.info(`Session idle for ${sessionIdleMs}ms, reclaiming: ${sessionId}`);
        void closeSession(sessionId);
      }
    }
  }, sessionSweepMs);
  // Never hold the event loop open on account of the sweep.
  sweep.unref();

  async function handleInitialize(req: IncomingMessage, res: ServerResponse, body: unknown) {
    const server = await options.createServer();
    // A client that drops the initialize response never learns the session id, so nothing can ever
    // use or delete that session. Reclaim it. The flag covers both orderings, since the abort can
    // land either side of the session being created.
    //
    // On SDK 1.30 the initialize response is written and ended before a fast abort is observed, so
    // in that case writableEnded is already true and the idle sweep is what reclaims. This stays as
    // the net for a response that genuinely fails to complete.
    let isResponseAborted = false;

    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      enableDnsRebindingProtection: true,
      allowedHosts,
      allowedOrigins,
      onsessioninitialized: (sessionId) => {
        sessions.set(sessionId, { transport, server, lastSeen: Date.now() });
        logger.info(`Session opened: ${sessionId} (${sessions.size} active)`);
        if (isResponseAborted) {
          logger.info(`Initialize response never landed, reclaiming session: ${sessionId}`);
          void closeSession(sessionId);
        }
      },
      onsessionclosed: (sessionId) => {
        // Must close the server, not just drop the map entry: the transport terminates
        // itself but the McpServer it was connected to would otherwise never be released.
        void closeSession(sessionId);
      },
    });

    await server.connect(transport);

    res.on("close", () => {
      if (res.writableEnded) return;
      isResponseAborted = true;
      const sessionId = transport.sessionId;
      if (sessionId) void closeSession(sessionId);
    });

    await transport.handleRequest(req, res, body);
  }

  const httpServer = createHttpServer((req, res) => {
    void (async () => {
      try {
        const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);

        // Ahead of every other check, including /health: a Host header that doesn't match is
        // exactly the DNS-rebinding shape, and /health leaking the live session count to it is
        // as much a rebinding target as /mcp is.
        if (!isAllowedHost(req)) {
          return rpcError(res, 403, -32000, `Invalid Host header: ${sanitizeForLog(req.headers.host ?? "")}`);
        }

        if (url.pathname === "/health") {
          // Liveness has to work without a token. The session count is operational detail, so it
          // only goes to a caller that is authorized (or to anyone when no token is configured).
          return sendJson(res, 200, isAuthorized(req) ? { status: "ok", sessions: sessions.size } : { status: "ok" });
        }

        if (url.pathname !== MCP_PATH) {
          return rpcError(res, 404, -32601, `Not found. MCP endpoint is ${MCP_PATH}`);
        }

        if (!isAuthorized(req)) {
          return rpcError(res, 401, -32001, "Unauthorized");
        }

        const sessionId = req.headers["mcp-session-id"];
        const existing = typeof sessionId === "string" ? sessions.get(sessionId) : undefined;

        if (existing) {
          existing.lastSeen = Date.now();
          // Only POST carries a body; the SDK dispatches GET (SSE) and DELETE without reading one.
          const body = req.method === "POST" ? await readCappedBody(req) : undefined;
          return await existing.transport.handleRequest(req, res, body);
        }

        // A session id we don't know is 404 per the spec, distinct from the 400 below for a
        // non-initialize request that carries no session id at all.
        if (typeof sessionId === "string") {
          return rpcError(res, 404, -32001, "Session not found");
        }

        if (req.method !== "POST") {
          return rpcError(res, 400, -32000, "Missing session. Send an initialize request first.");
        }

        const body = await readCappedBody(req);
        if (!isInitializeRequest(body)) {
          return rpcError(res, 400, -32000, "Missing session. Send an initialize request first.");
        }

        if (sessions.size >= maxSessions) {
          logger.warn(`Refusing new session: ${sessions.size} already open (max ${maxSessions})`);
          return rpcError(res, 503, -32000, `Too many sessions (max ${maxSessions}). Retry later.`);
        }

        await handleInitialize(req, res, body);
      } catch (err) {
        if (err instanceof BodyTooLargeError) {
          // The peer is still uploading. Flush the 413 first and only then cut the socket, or it
          // sees a reset instead of the status.
          if (!res.headersSent) {
            res.setHeader("Connection", "close");
            res.on("finish", () => req.destroy());
            rpcError(res, 413, -32600, err.message);
          } else {
            req.destroy();
          }
          return;
        }

        if (err instanceof InvalidJsonError) {
          if (!res.headersSent) rpcError(res, 400, -32700, "Parse error: invalid JSON body");
          return;
        }

        const message = err instanceof Error ? err.message : String(err);
        logger.error(`Request failed: ${sanitizeForLog(message)}`);
        if (!res.headersSent) rpcError(res, 500, -32603, "Internal server error");
        else res.end();
      }
    })();
  });

  await new Promise<void>((resolve, reject) => {
    httpServer.once("error", reject);
    httpServer.listen(options.port, options.host, resolve);
  });

  if (!options.token) {
    logger.warn("No --token set. Any process on this machine can reach this server.");
  }

  return {
    url: `http://${boundHostPort}${MCP_PATH}`,
    async close() {
      clearInterval(sweep);

      // Stop accepting first, then end the sessions, then give whatever is still writing a bounded
      // window. Without the deadline an in-flight query keeps a socket open and close() never
      // resolves.
      const closed = new Promise<void>((resolve) => httpServer.close(() => resolve()));
      httpServer.closeIdleConnections();
      await Promise.all([...sessions.keys()].map(closeSession));
      // Ending the sessions leaves their sockets idle rather than closed, so sweep again: without
      // this a held-open SSE stream costs the full drain deadline on every shutdown.
      httpServer.closeIdleConnections();

      let drainTimer: NodeJS.Timeout | undefined;
      const drained = await Promise.race([
        closed.then(() => true),
        new Promise<boolean>((resolve) => {
          drainTimer = setTimeout(() => resolve(false), DRAIN_TIMEOUT_MS);
        }),
      ]);
      if (drainTimer) clearTimeout(drainTimer);

      if (!drained) {
        logger.warn(`Connections still open after ${DRAIN_TIMEOUT_MS}ms, closing them`);
        httpServer.closeAllConnections();
        await closed;
      }
    },
  };
}
