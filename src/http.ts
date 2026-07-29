import { randomUUID } from "node:crypto";
import { createServer as createHttpServer, type IncomingMessage, type ServerResponse } from "node:http";

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
}

const MCP_PATH = "/mcp";

interface Session {
  transport: StreamableHTTPServerTransport;
  server: { close(): Promise<void> };
}

function readBody(req: IncomingMessage) {
  return new Promise<unknown>((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("error", reject);
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      if (!raw) return resolve(undefined);
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new Error("Invalid JSON body"));
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

  // Loopback-only by default, plus the SDK's Host/Origin checks, so a browser on
  // another origin cannot reach this server via DNS rebinding.
  const isLoopback = options.host === "127.0.0.1" || options.host === "::1" || options.host === "localhost";
  const allowedHosts = [`${options.host}:${options.port}`, `localhost:${options.port}`, `127.0.0.1:${options.port}`];

  // On loopback the OS already limits reach to this machine. Off loopback it doesn't,
  // so refuse to expose the sources to the network without a token.
  if (!isLoopback && !options.token) {
    throw new Error(
      `Refusing to bind ${options.host} without --token. A non-loopback bind is reachable ` +
        "from the network; pass --token (or set the *_MCP_TOKEN env var), or bind 127.0.0.1."
    );
  }

  function isAuthorized(req: IncomingMessage) {
    if (!options.token) return true;
    const header = req.headers.authorization;
    return typeof header === "string" && header === `Bearer ${options.token}`;
  }

  async function closeSession(sessionId: string) {
    const session = sessions.get(sessionId);
    if (!session) return;
    sessions.delete(sessionId);
    await session.server.close().catch(() => undefined);
    logger.info(`Session closed: ${sessionId} (${sessions.size} active)`);
  }

  async function handleInitialize(req: IncomingMessage, res: ServerResponse, body: unknown) {
    const server = await options.createServer();
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      enableDnsRebindingProtection: true,
      allowedHosts,
      onsessioninitialized: (sessionId) => {
        sessions.set(sessionId, { transport, server });
        logger.info(`Session opened: ${sessionId} (${sessions.size} active)`);
      },
      onsessionclosed: (sessionId) => {
        // Must close the server, not just drop the map entry: the transport terminates
        // itself but the McpServer it was connected to would otherwise never be released.
        void closeSession(sessionId);
      },
    });

    await server.connect(transport);
    await transport.handleRequest(req, res, body);
  }

  const httpServer = createHttpServer((req, res) => {
    void (async () => {
      try {
        const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);

        if (url.pathname === "/health") {
          return sendJson(res, 200, { status: "ok", sessions: sessions.size });
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
          return await existing.transport.handleRequest(req, res);
        }

        // A session id we don't know is 404 per the spec, distinct from the 400 below for a
        // non-initialize request that carries no session id at all.
        if (typeof sessionId === "string") {
          return rpcError(res, 404, -32001, "Session not found");
        }

        if (req.method !== "POST") {
          return rpcError(res, 400, -32000, "Missing session. Send an initialize request first.");
        }

        const body = await readBody(req);
        if (!isInitializeRequest(body)) {
          return rpcError(res, 400, -32000, "Missing session. Send an initialize request first.");
        }

        await handleInitialize(req, res, body);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        logger.error(`Request failed: ${message}`);
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
    url: `http://${options.host}:${options.port}${MCP_PATH}`,
    async close() {
      await Promise.all([...sessions.keys()].map(closeSession));
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    },
  };
}
