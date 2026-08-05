import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { DEFAULT_HOST, DEFAULT_PORT, parseArgs } from "../src/args.js";

beforeEach(() => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  delete process.env.POSTGRES_MCP_HOST;
  delete process.env.POSTGRES_MCP_PORT;
  delete process.env.POSTGRES_MCP_TOKEN;
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("parseArgs", () => {
  it("defaults to stdio on the default host and port", () => {
    expect(parseArgs(["cfg.toml"])).toEqual({
      configPath: "cfg.toml",
      useHttp: false,
      host: DEFAULT_HOST,
      port: DEFAULT_PORT,
      token: undefined,
    });
  });

  it("reads the http flags", () => {
    expect(parseArgs(["cfg.toml", "--http", "--port", "9999", "--host", "::1", "--token", "s"])).toEqual({
      configPath: "cfg.toml",
      useHttp: true,
      host: "::1",
      port: 9999,
      token: "s",
    });
  });

  it("ignores an unknown flag instead of refusing to start", () => {
    // The previous release read only argv[2], so a pinned consumer passing anything else kept
    // working. Exiting here would break it on upgrade.
    const args = parseArgs(["cfg.toml", "--verbose"]);

    expect(args).toBeDefined();
    expect(args?.configPath).toBe("cfg.toml");
  });

  it("still refuses a missing config path, --help and a bad port", () => {
    expect(parseArgs([])).toBeUndefined();
    expect(parseArgs(["--help"])).toBeUndefined();
    expect(parseArgs(["cfg.toml", "--port", "0"])).toBeUndefined();
    expect(parseArgs(["cfg.toml", "--port", "nope"])).toBeUndefined();
  });

  it("reads the token from a file so it is not visible in ps", () => {
    const file = path.join(os.tmpdir(), `token-${Date.now()}`);
    fs.writeFileSync(file, "from-file\n");

    try {
      expect(parseArgs(["cfg.toml", "--token-file", file])?.token).toBe("from-file");
    } finally {
      fs.unlinkSync(file);
    }
  });
});
