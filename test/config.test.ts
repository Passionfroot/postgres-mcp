import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { loadConfig } from "../src/config.js";

const tmpDir = os.tmpdir();
const createdFiles: string[] = [];

function writeTempToml(content: string): string {
  const filePath = path.join(
    tmpDir,
    `test-config-${Date.now()}-${Math.random().toString(36).slice(2)}.toml`
  );
  fs.writeFileSync(filePath, content, "utf-8");
  createdFiles.push(filePath);
  return filePath;
}

afterEach(() => {
  for (const f of createdFiles) {
    try {
      fs.unlinkSync(f);
    } catch {
      // ignore cleanup errors
    }
  }
  createdFiles.length = 0;
});

describe("loadConfig", () => {
  it("parses valid TOML with all fields", () => {
    const toml = `
[[sources]]
id = "production"
dsn = "postgres://user:pass@host:5432/db"
readonly = true
max_rows = 500
timeout = 60
ssh_host = "bastion.example.com"
ssh_user = "deploy"
ssh_key = "/absolute/path/to/key.pem"
`;
    const config = loadConfig(writeTempToml(toml));

    expect(config.sources).toHaveLength(1);
    expect(config.sources[0]).toEqual({
      id: "production",
      dsn: "postgres://user:pass@host:5432/db",
      readonly: true,
      maxRows: 500,
      timeout: 60,
      poolMax: 1,
      allowMultiStatements: false,
      role: undefined,
      sessionVars: undefined,
      sshHost: "bastion.example.com",
      sshUser: "deploy",
      sshKey: "/absolute/path/to/key.pem",
    });
  });

  it("applies defaults for optional fields", () => {
    const toml = `
[[sources]]
id = "local"
dsn = "postgres://localhost/mydb"
`;
    const config = loadConfig(writeTempToml(toml));

    expect(config.sources[0].readonly).toBe(false);
    expect(config.sources[0].maxRows).toBe(1000);
    expect(config.sources[0].timeout).toBe(10);
    expect(config.sources[0].poolMax).toBe(1);
    expect(config.sources[0].allowMultiStatements).toBe(false);
    expect(config.sources[0].sshHost).toBeUndefined();
    expect(config.sources[0].sshUser).toBeUndefined();
    expect(config.sources[0].sshKey).toBeUndefined();
  });

  it("expands $VAR in DSN", () => {
    process.env.TEST_PG_PASSWORD = "s3cret";
    const toml = `
[[sources]]
id = "test"
dsn = "postgres://user:$TEST_PG_PASSWORD@host/db"
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources[0].dsn).toBe("postgres://user:s3cret@host/db");
    delete process.env.TEST_PG_PASSWORD;
  });

  it("expands ${VAR} in DSN", () => {
    process.env.TEST_PG_HOST = "db.example.com";
    const toml = `
[[sources]]
id = "test"
dsn = "postgres://user:pass@\${TEST_PG_HOST}/db"
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources[0].dsn).toBe("postgres://user:pass@db.example.com/db");
    delete process.env.TEST_PG_HOST;
  });

  it("throws on undefined env var in DSN", () => {
    delete process.env.NONEXISTENT_VAR_12345;
    const toml = `
[[sources]]
id = "test"
dsn = "postgres://user:$NONEXISTENT_VAR_12345@host/db"
`;
    expect(() => loadConfig(writeTempToml(toml))).toThrow("NONEXISTENT_VAR_12345");
    expect(() => loadConfig(writeTempToml(toml))).toThrow("not set");
  });

  it("expands tilde in ssh_key path", () => {
    const toml = `
[[sources]]
id = "prod"
dsn = "postgres://localhost/db"
ssh_key = "~/.ssh/key.pem"
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources[0].sshKey).toBe(path.join(os.homedir(), ".ssh/key.pem"));
    expect(config.sources[0].sshKey).not.toContain("~");
  });

  it("throws on missing file", () => {
    const fakePath = path.join(tmpDir, "nonexistent-config-file.toml");
    expect(() => loadConfig(fakePath)).toThrow("Failed to read config file");
    expect(() => loadConfig(fakePath)).toThrow(fakePath);
  });

  it("throws on invalid TOML syntax", () => {
    const toml = `
[[sources]]
id = "test
dsn = missing closing quote
`;
    expect(() => loadConfig(writeTempToml(toml))).toThrow("Failed to parse TOML");
  });

  it("throws on missing required field (id)", () => {
    const toml = `
[[sources]]
dsn = "postgres://localhost/db"
`;
    expect(() => loadConfig(writeTempToml(toml))).toThrow("Invalid config");
  });

  it("throws on missing required field (dsn)", () => {
    const toml = `
[[sources]]
id = "test"
`;
    expect(() => loadConfig(writeTempToml(toml))).toThrow("Invalid config");
  });

  it("converts snake_case TOML keys to camelCase in Config", () => {
    const toml = `
[[sources]]
id = "test"
dsn = "postgres://localhost/db"
max_rows = 2000
ssh_host = "bastion.example.com"
ssh_user = "admin"
ssh_key = "/path/to/key.pem"
`;
    const config = loadConfig(writeTempToml(toml));
    const source = config.sources[0];

    expect(source.maxRows).toBe(2000);
    expect(source.sshHost).toBe("bastion.example.com");
    expect(source.sshUser).toBe("admin");
    expect(source.sshKey).toBe("/path/to/key.pem");

    // Verify snake_case keys are NOT present on the returned object
    expect("max_rows" in source).toBe(false);
    expect("ssh_host" in source).toBe(false);
    expect("ssh_user" in source).toBe(false);
    expect("ssh_key" in source).toBe(false);
  });

  it("parses role and session_vars", () => {
    const toml = `
[[sources]]
id = "production"
dsn = "postgres://localhost/db"
readonly = true
role = "app_mcp_readonly"
session_vars = { "app.current_tenant_id" = "tenant_123", "app.env" = "production" }
`;
    const config = loadConfig(writeTempToml(toml));
    const source = config.sources[0];

    expect(source.role).toBe("app_mcp_readonly");
    expect(source.sessionVars).toEqual({
      "app.current_tenant_id": "tenant_123",
      "app.env": "production",
    });
  });

  it("expands env vars in session_vars values", () => {
    process.env.TEST_TENANT_ID = "t_abc";
    const toml = `
[[sources]]
id = "test"
dsn = "postgres://localhost/db"
session_vars = { "app.current_tenant_id" = "$TEST_TENANT_ID" }
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources[0].sessionVars).toEqual({
      "app.current_tenant_id": "t_abc",
    });
    delete process.env.TEST_TENANT_ID;
  });

  it("omits role and sessionVars when not configured", () => {
    const toml = `
[[sources]]
id = "local"
dsn = "postgres://localhost/db"
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources[0].role).toBeUndefined();
    expect(config.sources[0].sessionVars).toBeUndefined();
  });

  it("parses multiple sources", () => {
    const toml = `
[[sources]]
id = "production"
dsn = "postgres://prod-host/db"
readonly = true

[[sources]]
id = "staging"
dsn = "postgres://staging-host/db"

[[sources]]
id = "local"
dsn = "postgres://localhost/db"

[[sources]]
id = "snaplet"
dsn = "postgres://localhost/snaplet_db"
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources).toHaveLength(4);
    expect(config.sources.map((s) => s.id)).toEqual(["production", "staging", "local", "snaplet"]);
  });
});
