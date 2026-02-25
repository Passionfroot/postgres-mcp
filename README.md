# postgres-mcp

A Model Context Protocol server for PostgreSQL with Prisma-aware schema introspection.

## Why not use an off-the-shelf MCP?

Prisma manages foreign key relationships at the application level rather than through database constraints. Off-the-shelf PostgreSQL MCPs only see the raw database schema and miss these relationships entirely. This server merges live database introspection with Prisma schema parsing to give Claude the full picture: table relationships, field name mappings, enum values, and drift detection.

If you don't use Prisma, the server still works — it just shows the raw database schema without Prisma annotations.

## Architecture

```
Claude Code session
  └── MCP server (stdio subprocess)
        ├── pg.Pool (max 1 connection per source)
        │     └── SSH tunnel (if configured) → bastion → PostgreSQL
        ├── Schema cache (Prisma + DB introspection merged)
        └── Audit log (optional rotating file)
```

Each Claude Code session spawns one MCP server process. The server connects lazily to configured database sources on first query. Connections idle-timeout after 5 seconds and are recreated on demand.

## Setup

### 1. Create a config file

Create `postgres-mcp.toml`:

```toml
# Optional: path to your Prisma schema file.
# Enables model name resolution, field mappings, and relationship detection.
# The MCP reads this on startup — no build step needed.
# prisma_schema_path = "~/work/myproject/prisma/schema.prisma"

[[sources]]
id = "production"
dsn = "postgres://$DB_USER:$DB_PASS@db-host:5432/mydb?sslmode=require"
readonly = true

[[sources]]
id = "local"
dsn = "postgres://localhost/mydb"
timeout = 30
pool_max = 3
allow_multi_statements = true
```

See [`postgres-mcp.toml.example`](postgres-mcp.toml.example) for the full reference.

### 2. Add to `.mcp.json`

```json
{
  "mcpServers": {
    "postgres": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@passionfroot/postgres-mcp", "path/to/postgres-mcp.toml"]
    }
  }
}
```

Restart Claude Code and the `mcp__postgres__*` tools will be available.

### 3. Add a skill (recommended)

Copy [`examples/SKILL.md`](examples/SKILL.md) into your project's `.claude/skills/postgres-query/SKILL.md` and customize it with your database names, table mappings, and common query patterns. See [Using a Skill](#using-a-skill) below.

## Tools

### `execute_sql`

Execute SQL against a configured database source. Returns JSON rows.

Safety mechanisms:

- **LIMIT injection**: Single SELECT queries without a LIMIT get one auto-appended. If the SQL parser can't handle the query (PostgreSQL-specific operators, lateral joins), a regex fallback appends `LIMIT N` instead of running unlimited.
- **Statement timeout**: Every query runs under the source's `statement_timeout`. PostgreSQL cancels it server-side.
- **Readonly enforcement**: When `readonly = true`, the server sets `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` before each query.
- **Multi-statement blocking**: When `allow_multi_statements = false` (default), semicolon-separated compound queries are rejected.

### `search_objects`

Search for tables by name (Prisma model name or SQL table name). Returns column-level detail: types, nullability, defaults, FK relationships, and enum values.

### `schema://[database]` (resource)

Returns a lean relationship map: all tables, their Prisma model names, and FK connections (incoming & outgoing). Use this for orientation before drilling into specific tables with `search_objects`.

## Configuration Reference

### Source options

| Field                    | Default    | Description                                                                           |
| ------------------------ | ---------- | ------------------------------------------------------------------------------------- |
| `id`                     | (required) | Unique source identifier (used in tool calls)                                         |
| `dsn`                    | (required) | PostgreSQL connection string. Supports `$VAR` and `${VAR}` env var expansion          |
| `readonly`               | `false`    | Enforce read-only sessions via `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` |
| `timeout`                | `10`       | Statement timeout in seconds                                                          |
| `max_rows`               | `1000`     | Maximum rows returned per query (auto-appended as LIMIT)                              |
| `pool_max`               | `1`        | Maximum connections in the pool                                                       |
| `allow_multi_statements` | `false`    | Allow semicolon-separated multi-statement queries                                     |
| `ssh_host`               | —          | SSH bastion hostname for tunneled connections                                         |
| `ssh_user`               | —          | SSH username                                                                          |
| `ssh_key`                | —          | Path to SSH private key (supports `~` expansion)                                      |

### Global options

| Field                | Default | Description                                                                 |
| -------------------- | ------- | --------------------------------------------------------------------------- |
| `prisma_schema_path` | —       | Path to your `.prisma` schema file. Also discovers `models/*.prisma` files. |

### Audit log options

| Field      | Default    | Description                                                   |
| ---------- | ---------- | ------------------------------------------------------------- |
| `log_file` | (required) | Path to the JSONL audit log file (supports `~` expansion)     |
| `max_size` | unlimited  | Max file size in bytes before rotation. Rotates to `{file}.1` |

## Prisma Schema Integration

The MCP reads your Prisma schema file at runtime via `prisma_schema_path`. It uses a lightweight regex parser — no dependency on `@prisma/internals` or engine binaries.

**What it extracts:**

- Model-to-table mappings (`@@map`)
- Field-to-column mappings (`@map`)
- Relations with `fields`/`references` (`@relation`)
- Composite primary keys (`@@id`)
- Enum definitions

**How syncing works:** The MCP re-reads the Prisma file on each startup. When you update your Prisma schema and restart your Claude Code session, the MCP automatically picks up the changes. There's no build step or cache to invalidate.

**Drift detection:** The merge process compares the Prisma schema against the live database and flags:

- Missing tables (Prisma model exists, DB table doesn't)
- Missing columns (Prisma field exists, DB column doesn't)
- Type mismatches (Prisma says `String`, DB has `integer`)

## Using a Skill

A Claude Code skill teaches Claude _how_ to use your MCP effectively. Without it, Claude can still call the tools but may guess table names or write incorrect SQL. With a skill, Claude follows a structured workflow:

1. **Investigate schema** — Read `schema://[database]` for the relationship overview, then `search_objects` for column-level detail
2. **Write the query** — Using confirmed table and column names
3. **Execute** — Run with `execute_sql`

The skill also documents project-specific gotchas (camelCase columns, Prisma-vs-SQL table names, enum values) that prevent common query mistakes.

**Setup:**

1. Copy [`examples/SKILL.md`](examples/SKILL.md) into `.claude/skills/postgres-query/SKILL.md`
2. Customize the `Available Databases` table with your source IDs
3. Add any project-specific table mappings, gotchas, and common query patterns
4. The skill auto-activates when you ask Claude to query the database

See the example file for the full template.

## Connection Count Impact

Each MCP session opens at most `pool_max` connections per source (default: 1). With 10 engineers:

- **Default config**: 10 sessions × 1 connection = 10 connections
- **Idle release**: Connections drop after 5s of inactivity, so active count is typically lower
- **SSH tunnels**: One tunnel per session per SSH-enabled source

## Development

```bash
git clone https://github.com/Passionfroot/postgres-mcp.git
cd postgres-mcp
npm install
npm run build
npm test
```

### Integration tests

Integration tests require a running PostgreSQL instance. Set `POSTGRES_MCP_TEST_DSN` or they'll default to `postgresql://localhost/postgres`:

```bash
POSTGRES_MCP_TEST_DSN=postgresql://localhost/mydb npm run intg-test
```

Tests auto-skip if the database is unavailable.

### Releasing

1. Add a changeset: `npx changeset`
2. Version: `npx changeset version`
3. Commit and push
4. Create a GitHub Release — CI will publish to npm

## License

MIT
