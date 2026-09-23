import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  CONFIG_PATH_ENV_VAR,
  CONFIG_TOML_ENV_VAR,
  DEFAULT_HOST,
  DEFAULT_PORT,
  parseArgs,
} from "../src/args.js";

beforeEach(() => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  delete process.env.POSTGRES_MCP_HOST;
  delete process.env.POSTGRES_MCP_PORT;
  delete process.env.POSTGRES_MCP_TOKEN;
  delete process.env.POSTGRES_MCP_CONFIG;
  delete process.env.POSTGRES_MCP_CONFIG_TOML;
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("parseArgs", () => {
  it("defaults to stdio on the default host and port", () => {
    expect(parseArgs(["cfg.toml"])).toEqual({
      configSource: { kind: "file", path: "cfg.toml" },
      useHttp: false,
      host: DEFAULT_HOST,
      port: DEFAULT_PORT,
      token: undefined,
    });
  });

  it("reads the http flags", () => {
    expect(parseArgs(["cfg.toml", "--http", "--port", "9999", "--host", "::1", "--token", "s"])).toEqual({
      configSource: { kind: "file", path: "cfg.toml" },
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
    expect(args?.configSource).toEqual({ kind: "file", path: "cfg.toml" });
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

const TOML = 'sources = [{ id = "s", dsn = "postgres://h/d" }]';

describe("parseArgs config source", () => {
  it("takes a positional path as a file source", () => {
    expect(parseArgs(["cfg.toml"])?.configSource).toEqual({ kind: "file", path: "cfg.toml" });
  });

  it(`reads the TOML itself out of ${CONFIG_TOML_ENV_VAR}`, () => {
    process.env[CONFIG_TOML_ENV_VAR] = TOML;
    expect(parseArgs([])?.configSource).toEqual({ kind: "inline", toml: TOML });
  });

  it(`reads a path out of ${CONFIG_PATH_ENV_VAR}, which named a path before it named anything here`, () => {
    process.env[CONFIG_PATH_ENV_VAR] = "/etc/postgres-mcp.toml";
    expect(parseArgs([])?.configSource).toEqual({ kind: "file", path: "/etc/postgres-mcp.toml" });
  });

  it("takes the positional path over either variable", () => {
    process.env[CONFIG_TOML_ENV_VAR] = TOML;
    process.env[CONFIG_PATH_ENV_VAR] = "/etc/postgres-mcp.toml";
    expect(parseArgs(["cfg.toml"])?.configSource).toEqual({ kind: "file", path: "cfg.toml" });
  });

  it("takes the inline TOML over the path variable, since it cannot have been meant as a path", () => {
    process.env[CONFIG_TOML_ENV_VAR] = TOML;
    process.env[CONFIG_PATH_ENV_VAR] = "/etc/postgres-mcp.toml";
    expect(parseArgs([])?.configSource).toEqual({ kind: "inline", toml: TOML });
  });

  it("ignores a whitespace-only variable rather than starting with no sources", () => {
    process.env[CONFIG_TOML_ENV_VAR] = "   ";
    process.env[CONFIG_PATH_ENV_VAR] = "  ";
    expect(parseArgs([])).toBeUndefined();
  });

  it("ignores a whitespace-only argument, which is a wrapper interpolating an unset variable", () => {
    expect(parseArgs(["   "])).toBeUndefined();
  });

  it("reads the http flags with no positional argument", () => {
    process.env[CONFIG_TOML_ENV_VAR] = TOML;
    expect(parseArgs(["--http", "--port", "9999"])).toMatchObject({
      configSource: { kind: "inline", toml: TOML },
      useHttp: true,
      port: 9999,
    });
  });
});
