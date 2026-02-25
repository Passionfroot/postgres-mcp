import fs from "node:fs";
import net from "node:net";
import ssh2 from "ssh2";
const { Client: SSHClient } = ssh2;

import type { TunnelConfig, TunnelHandle } from "./types.js";

import { logger } from "./logger.js";

/** Parse host and port from a PostgreSQL DSN string. Defaults to port 5432 if not specified. */
export function parseDsnHostPort(dsn: string): { host: string; port: number } {
  const url = new URL(dsn);
  const host = url.hostname;
  const port = url.port ? parseInt(url.port, 10) : 5432;
  return { host, port };
}

/**
 * Create an SSH tunnel using a local TCP proxy pattern.
 *
 * Spawns a local net.Server on an ephemeral port that proxies each incoming TCP connection through
 * an SSH tunnel via ssh2 forwardOut(). pg.Pool connects to the local proxy as if it were the remote
 * database.
 */
export function createTunnel(config: TunnelConfig): Promise<TunnelHandle> {
  return new Promise((resolve, reject) => {
    const ssh = new SSHClient();
    const activeSockets = new Set<net.Socket>();

    const proxyServer = net.createServer((socket) => {
      activeSockets.add(socket);
      socket.on("close", () => activeSockets.delete(socket));

      const srcAddr = socket.remoteAddress ?? "127.0.0.1";
      const srcPort = socket.remotePort ?? 0;

      ssh.forwardOut(
        srcAddr,
        srcPort,
        config.remoteHost,
        config.remotePort,
        (err, stream) => {
          if (err) {
            logger.error(`SSH forwardOut failed: ${err.message}`, {
              remoteHost: config.remoteHost,
              remotePort: config.remotePort,
            });
            socket.destroy();
            return;
          }

          socket.pipe(stream).pipe(socket);

          stream.on("error", () => socket.destroy());
          socket.on("error", () => stream.destroy());
        }
      );
    });

    proxyServer.on("error", (err) => {
      logger.error(`Proxy server error: ${err.message}`);
    });

    ssh.on("ready", () => {
      proxyServer.listen(0, "127.0.0.1", () => {
        const addr = proxyServer.address();
        if (!addr || typeof addr === "string") {
          reject(new Error("Proxy server returned unexpected address format"));
          return;
        }

        logger.info(
          `SSH tunnel established: 127.0.0.1:${addr.port} -> ${config.remoteHost}:${config.remotePort} via ${config.sshHost}`
        );

        resolve({
          localHost: "127.0.0.1",
          localPort: addr.port,
          close: () =>
            new Promise<void>((res) => {
              for (const socket of activeSockets) {
                socket.destroy();
              }
              activeSockets.clear();

              proxyServer.close(() => {
                ssh.end();
                res();
              });
            }),
        });
      });
    });

    ssh.on("error", (err) => {
      reject(
        new Error(`SSH tunnel to ${config.sshHost} failed: ${err.message}`)
      );
    });

    let privateKey: Buffer;
    try {
      privateKey = fs.readFileSync(config.sshKeyPath);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      reject(
        new Error(
          `Failed to read SSH private key at ${config.sshKeyPath}: ${message}`
        )
      );
      return;
    }

    ssh.connect({
      host: config.sshHost,
      username: config.sshUser,
      privateKey,
      keepaliveInterval: config.keepaliveInterval,
    });
  });
}
