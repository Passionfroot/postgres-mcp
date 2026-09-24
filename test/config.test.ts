import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  HTTP_DEFAULT_POOL_MAX,
  applyHttpPoolDefaults,
  DSN_SOURCE_ID,
  configFromDsn,
  configOrigin,
  loadConfig,
  loadFromSource,
  parseConfig,
} from "../src/config.js";

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
      poolMaxExplicit: false,
      maxResponseBytes: 1_000_000,
      allowMultiStatements: false,
      readOnlyQueries: false,
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
    expect(config.sources[0].poolMaxExplicit).toBe(false);
    expect(config.sources[0].maxResponseBytes).toBe(1_000_000);
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
    expect(config.sources[0].dsn).toBe(
      "postgres://user:pass@db.example.com/db"
    );
    delete process.env.TEST_PG_HOST;
  });

  it("throws on undefined env var in DSN", () => {
    delete process.env.NONEXISTENT_VAR_12345;
    const toml = `
[[sources]]
id = "test"
dsn = "postgres://user:$NONEXISTENT_VAR_12345@host/db"
`;
    expect(() => loadConfig(writeTempToml(toml))).toThrow(
      "NONEXISTENT_VAR_12345"
    );
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
    expect(config.sources[0].sshKey).toBe(
      path.join(os.homedir(), ".ssh/key.pem")
    );
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
    expect(() => loadConfig(writeTempToml(toml))).toThrow(
      "Failed to parse TOML"
    );
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

  // Zod's error output is what a client actually sees. A major zod bump can change issue shape or
  // message wording without any TypeScript or existing-test breakage, so these pin the exact text.
  describe("error message shape", () => {
    it("keeps the custom message for an empty required string, not a generic type error", () => {
      const toml = `
[[sources]]
id = ""
dsn = "postgres://localhost/db"
`;
      expect(() => loadConfig(writeTempToml(toml))).toThrow(
        "sources.0.id: Source id is required"
      );
    });

    it("reports a clear path and reason for a wrong-typed field, not a garbled dump", () => {
      const toml = `
[[sources]]
id = "test"
dsn = "postgres://localhost/db"
max_rows = "not-a-number"
`;
      let error: Error | undefined;
      try {
        loadConfig(writeTempToml(toml));
      } catch (err) {
        error = err as Error;
      }

      expect(error).toBeDefined();
      expect(error?.message).toContain("sources.0.max_rows");
      expect(error?.message).not.toContain("[object Object]");
      expect(error?.message).not.toContain("undefined");
    });

    it("reports the nested key path for an invalid session_vars value", () => {
      const toml = `
[[sources]]
id = "test"
dsn = "postgres://localhost/db"
session_vars = { role = 5 }
`;
      let error: Error | undefined;
      try {
        loadConfig(writeTempToml(toml));
      } catch (err) {
        error = err as Error;
      }

      expect(error).toBeDefined();
      expect(error?.message).toContain("sources.0.session_vars.role");
      expect(error?.message).not.toContain("[object Object]");
    });
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

  it("ignores an unknown top-level key without failing to load", () => {
    const toml = `
include_prisma_info = false

[[sources]]
id = "local"
dsn = "postgres://localhost/db"
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources).toHaveLength(1);
    expect(config).not.toHaveProperty("includePrismaInfo");
  });

  it("defaults readOnlyQueries off for a plain source", () => {
    const toml = `
[[sources]]
id = "local"
dsn = "postgres://localhost/db"
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources[0].readOnlyQueries).toBe(false);
  });

  it("defaults readOnlyQueries on when role pins the source", () => {
    const toml = `
[[sources]]
id = "tenant"
dsn = "postgres://localhost/db"
role = "zest_mcp_reader"
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources[0].readOnlyQueries).toBe(true);
  });

  it("defaults readOnlyQueries on when session_vars pin the source", () => {
    const toml = `
[[sources]]
id = "tenant"
dsn = "postgres://localhost/db"
session_vars = { "app.partner_id" = "p1" }
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources[0].readOnlyQueries).toBe(true);
  });

  it("lets an explicit read_only_queries override the default", () => {
    const toml = `
[[sources]]
id = "tenant"
dsn = "postgres://localhost/db"
role = "zest_mcp_reader"
read_only_queries = false
`;
    const config = loadConfig(writeTempToml(toml));
    expect(config.sources[0].readOnlyQueries).toBe(false);
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
    expect(config.sources.map((s) => s.id)).toEqual([
      "production",
      "staging",
      "local",
      "snaplet",
    ]);
  });
});

describe("applyHttpPoolDefaults", () => {
  it("raises pool_max for sources that never set one", () => {
    const toml = `
[[sources]]
id = "local"
dsn = "postgres://localhost/mydb"
`;
    const config = applyHttpPoolDefaults(loadConfig(writeTempToml(toml)));

    expect(config.sources[0].poolMax).toBe(HTTP_DEFAULT_POOL_MAX);
  });

  it("leaves an explicit pool_max alone, including an explicit 1", () => {
    const toml = `
[[sources]]
id = "pinned"
dsn = "postgres://localhost/a"
pool_max = 1

[[sources]]
id = "sized"
dsn = "postgres://localhost/b"
pool_max = 3
`;
    const config = applyHttpPoolDefaults(loadConfig(writeTempToml(toml)));

    expect(config.sources[0].poolMax).toBe(1);
    expect(config.sources[1].poolMax).toBe(3);
  });

  it("does not mutate the config it was given", () => {
    const toml = `
[[sources]]
id = "local"
dsn = "postgres://localhost/mydb"
`;
    const original = loadConfig(writeTempToml(toml));
    applyHttpPoolDefaults(original);

    expect(original.sources[0].poolMax).toBe(1);
  });
});

describe("max_response_bytes", () => {
  it("reads a per-source override", () => {
    const toml = `
[[sources]]
id = "local"
dsn = "postgres://localhost/mydb"
max_response_bytes = 250000
`;
    const config = loadConfig(writeTempToml(toml));

    expect(config.sources[0].maxResponseBytes).toBe(250_000);
  });
});

describe("parseConfig", () => {
  it("parses TOML held in a string, so a client with nowhere to write a file can still configure", () => {
    const config = parseConfig(
      `[[sources]]
id = "staging"
dsn = "postgres://user:pass@host:5432/db"
readonly = true
`,
      "POSTGRES_MCP_CONFIG"
    );
    expect(config.sources).toHaveLength(1);
    expect(config.sources[0].id).toBe("staging");
    expect(config.sources[0].readonly).toBe(true);
  });

  it("expands env vars in a dsn the same way a file-based config does", () => {
    process.env.TEST_PARSE_CONFIG_DSN = "postgres://user:pass@host:5432/db";
    try {
      const config = parseConfig(
        `[[sources]]
id = "staging"
dsn = "\${TEST_PARSE_CONFIG_DSN}"
`,
        "POSTGRES_MCP_CONFIG"
      );
      expect(config.sources[0].dsn).toBe("postgres://user:pass@host:5432/db");
    } finally {
      delete process.env.TEST_PARSE_CONFIG_DSN;
    }
  });

  it("parses the one-line inline-array form, which is what fits in an env var that cannot hold newlines", () => {
    const config = parseConfig(
      'sources = [{ id = "staging", dsn = "postgres://user:pass@host:5432/db", readonly = true }]',
      "POSTGRES_MCP_CONFIG"
    );
    expect(config.sources).toHaveLength(1);
    expect(config.sources[0].id).toBe("staging");
    expect(config.sources[0].readonly).toBe(true);
  });

  it("names the origin in a parse error, so the message points at the env var not a path", () => {
    expect(() => parseConfig("this is not toml =", "POSTGRES_MCP_CONFIG")).toThrow(
      /POSTGRES_MCP_CONFIG/
    );
  });

  it("keeps the offending config out of a parse error, since an inline config can hold a password", () => {
    const withPassword = 'sources = [{ id = "s", dsn = "postgres://u:hunter2@h/d" }';

    expect(() => parseConfig(withPassword, "POSTGRES_MCP_CONFIG")).toThrow(
      /POSTGRES_MCP_CONFIG/
    );
    try {
      parseConfig(withPassword, "POSTGRES_MCP_CONFIG");
    } catch (err) {
      expect(String(err)).not.toContain("hunter2");
    }
  });

  it("keeps the file's own content out of a parse error too", () => {
    const filePath = writeTempToml('sources = [{ id = "s", dsn = "postgres://u:hunter2@h/d" }');

    try {
      loadConfig(filePath);
    } catch (err) {
      expect(String(err)).not.toContain("hunter2");
      expect(String(err)).toContain(filePath);
    }
  });

  it("names the origin when an env var referenced by the config is unset", () => {
    delete process.env.PF_DEFINITELY_UNSET;

    expect(() =>
      parseConfig('sources = [{ id = "s", dsn = "${PF_DEFINITELY_UNSET}" }]', "POSTGRES_MCP_CONFIG")
    ).toThrow(/POSTGRES_MCP_CONFIG/);
  });

  it("names the origin when the config has no sources", () => {
    expect(() => parseConfig('prisma_schema_path = "x"', "POSTGRES_MCP_CONFIG")).toThrow(
      /POSTGRES_MCP_CONFIG/
    );
  });
});

describe("tilde expansion outside a source", () => {
  it("expands ~ in audit_log.log_file, so the log does not land in a literal ~ directory", () => {
    const config = parseConfig(
      `[[sources]]
id = "s"
dsn = "postgres://h/d"

[audit_log]
log_file = "~/logs/pg.log"
`,
      "POSTGRES_MCP_CONFIG"
    );

    expect(config.auditLog?.logFile).not.toContain("~");
    expect(config.auditLog?.logFile).toContain("logs/pg.log");
  });

  it("expands ~ in prisma_schema_path", () => {
    const config = parseConfig(
      `prisma_schema_path = "~/app/schema.prisma"

[[sources]]
id = "s"
dsn = "postgres://h/d"
`,
      "POSTGRES_MCP_CONFIG"
    );

    expect(config.prismaSchemaPath).not.toContain("~");
    expect(config.prismaSchemaPath).toContain("app/schema.prisma");
  });
});

describe("configFromDsn", () => {
  it("builds a single read-only source, since a connection string in the environment is a read path", () => {
    const config = configFromDsn("postgres://user:pass@host:5432/db");

    expect(config.sources).toHaveLength(1);
    expect(config.sources[0].id).toBe(DSN_SOURCE_ID);
    expect(config.sources[0].readonly).toBe(true);
    expect(config.sources[0].readOnlyQueries).toBe(true);
    expect(config.prismaSchemaPath).toBeUndefined();
  });

  it("keeps the connection string literal, so a password holding $ survives", () => {
    const dsn = "postgres://user:pa$$word@host:5432/db";

    expect(configFromDsn(dsn).sources[0].dsn).toBe(dsn);
  });

  it("does not expand a ${VAR} in the connection string, which is already a value not a reference", () => {
    process.env.PF_DSN_SHOULD_NOT_EXPAND = "postgres://expanded/db";
    try {
      const dsn = "postgres://user:${PF_DSN_SHOULD_NOT_EXPAND}@host:5432/db";

      expect(configFromDsn(dsn).sources[0].dsn).toBe(dsn);
    } finally {
      delete process.env.PF_DSN_SHOULD_NOT_EXPAND;
    }
  });

  it("refuses an empty connection string rather than starting with a broken source", () => {
    expect(() => configFromDsn("")).toThrow(/POSTGRES_MCP_DSN/);
  });
});

describe("loadFromSource", () => {
  it("reads a file source off disk", () => {
    const filePath = writeTempToml('sources = [{ id = "from-file", dsn = "postgres://h/d" }]');

    expect(loadFromSource({ kind: "file", path: filePath }).sources[0].id).toBe("from-file");
  });

  it("builds a dsn source without touching the filesystem", () => {
    expect(loadFromSource({ kind: "dsn", dsn: "postgres://h/d" }).sources[0].id).toBe(DSN_SOURCE_ID);
  });

  it("names the config a message should point at", () => {
    expect(configOrigin({ kind: "file", path: "/etc/pg.toml" })).toBe("/etc/pg.toml");
    expect(configOrigin({ kind: "dsn", dsn: "postgres://h/d" })).toBe("POSTGRES_MCP_DSN");
  });
});
