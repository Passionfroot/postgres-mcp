# postgres-mcp

A Model Context Protocol server for PostgreSQL with Prisma-aware schema introspection.

## Why not use an off-the-shelf MCP?

Prisma manages foreign key relationships at the application level rather than through database constraints. Off-the-shelf PostgreSQL MCPs only see the raw database schema and miss these relationships entirely. This server merges live database introspection with Prisma schema parsing to give Claude the full picture: table relationships, field name mappings, enum values, and drift detection.

If you don't use Prisma, the server still works — it just shows the raw database schema without Prisma annotations.

## Architecture

```
Claude Code session
  └── MCP server (stdio subprocess; or one shared --http process for every session)
        ├── pg.Pool (max 1 connection per source on stdio, 10 under --http)
        │     └── SSH tunnel (if configured) → bastion → PostgreSQL
        ├── Schema cache (Prisma + DB introspection merged)
        └── Audit log (optional rotating file)
```

Over stdio, each Claude Code session spawns one MCP server process. The server connects lazily to configured database sources on first query. Connections idle-timeout after 5 seconds and are recreated on demand.

## Transports

stdio is the default: one process per client, which is what the per-tenant setup below relies on.

`--http` serves Streamable HTTP on `127.0.0.1:7803/mcp` instead, so one process handles every client and the connection pool and schema cache are shared rather than rebuilt per session.

| Flag | Default | |
|---|---|---|
| `--stdio` | on | One process per client |
| `--http` | off | Streamable HTTP, shared across clients |
| `--port <n>` | 7803 | `POSTGRES_MCP_PORT` |
| `--host <addr>` | 127.0.0.1 | `POSTGRES_MCP_HOST` |
| `--token <secret>` | none | Require `Authorization: Bearer <secret>`. `POSTGRES_MCP_TOKEN` |
| `--token-file <path>` | none | Read the token from a file |

An unknown flag is warned about and ignored rather than refused, so a pinned consumer passing a flag an older build ignored still starts.

### What is shared, and what that costs

Everything below is per server, not per client, and every client shares it:

| | Shared behaviour under `--http` |
|---|---|
| Connection pool | `pool_max` becomes a **per-server** budget. Sources that do not set it get `10` under `--http` instead of the stdio default of `1`. A source that sets `pool_max` explicitly keeps its value, and if that value is small its clients queue behind each other on it. |
| Schema cache | Introspected once per database for the whole server, not once per client. |
| Response size | Every response is capped at `max_response_bytes` (default 1 MB). Rows past the cap are dropped and the result is marked `truncated` with a `truncatedReason`. `max_rows` only bounds the row count, which is not the same thing when a row holds 100 kB of text. |
| Sessions | At most 256 live sessions; past that `initialize` answers `503`. A session untouched for 30 minutes is reclaimed. `GET /health` reports the live count. |
| Request bodies | Capped at 4 MB; a larger one gets `413`. |

Set `pool_max` explicitly if you want a different ceiling. Sizing it below the number of clients that query at once means they serialize, and a client that waits longer than its own request timeout (60 s in the MCP TypeScript SDK) sees a timeout rather than slowness.

### Security

- Binding a non-loopback address without `--token` is refused at startup rather than warned about. `localhost`, `127.0.0.0/8` and `::1` all count as loopback.
- The `Host` header must match the bound address, which is what blocks DNS rebinding. Bound to a specific address, that address is the only match; bound to the wildcard `0.0.0.0` or `::`, the machine's non-internal interface addresses and hostname are matched instead, since no real client ever sends the wildcard itself as `Host`. `Origin`, when a request sends one, must be a loopback origin for the bound port; a cross-origin browser request gets `403`. Requests with no `Origin` at all are allowed, because MCP clients are not browsers and do not send one.
- Prefer `POSTGRES_MCP_TOKEN` or `--token-file` over `--token`: an argv token is visible to any process that can run `ps`.
- `GET /health` answers without a token so a supervisor can probe it, subject to the same `Host` check as `/mcp`; it only reports the session count to an authorized caller.
- The transport is plain HTTP with no TLS: no certificate/key option exists. Binding non-loopback sends the bearer token and every request in cleartext. Put a reverse proxy or an SSH tunnel in front for any use beyond a trusted local network.

### HTTP is refused for per-tenant configs

A source with `session_vars` pins one tenant's identity, such as `app.partner_id`, for the life of the process, and RLS is the only thing enforcing that boundary. One server shared across clients cannot honour a per-process pin, so `--http` refuses to start when any source sets `session_vars`. Use stdio for those.

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

- **LIMIT injection**: SELECT queries without a LIMIT get one auto-appended. In a multi-statement batch it goes on the one statement that returns rows, so the server stops there rather than sending everything to be sliced afterwards. If the SQL parser can't handle the query (PostgreSQL-specific operators, lateral joins), a regex fallback appends `LIMIT N` instead of running unlimited.
- **Statement timeout**: Every query runs under the source's `statement_timeout`. PostgreSQL cancels it server-side.
- **Readonly enforcement**: When `readonly = true`, the server sets `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` before each query.
- **Multi-statement blocking**: When `allow_multi_statements = false` (default), a compound query the parser can see is rejected before it is sent. Input the parser reads as one statement and PostgreSQL splits into several is caught after the fact, when the driver returns one result per statement, and is rejected and logged rather than answered. Only `read_only_queries = true` keeps such a statement from running at all.
- **Batch results**: With `allow_multi_statements = true`, a batch returns its last row-returning statement, so `BEGIN; SELECT ...; COMMIT` returns the SELECT. A batch with two statements that both return rows is ambiguous and is rejected before it runs.

### `search_objects`

Search for tables by name (Prisma model name or SQL table name). Returns column-level detail: types, nullability, defaults, FK relationships, and enum values.

### `schema://[database]` (resource)

Returns a lean relationship map: tables, their Prisma model names, and FK connections (incoming & outgoing). Use this for orientation before drilling into specific tables with `search_objects`. With `prisma_schema_path` set the map lists the Prisma-mapped tables; without it, every table in the database.

## Configuration Reference

### Source options

| Field                    | Default    | Description                                                                           |
| ------------------------ | ---------- | ------------------------------------------------------------------------------------- |
| `id`                     | (required) | Unique source identifier (used in tool calls)                                         |
| `dsn`                    | (required) | PostgreSQL connection string. Supports `$VAR` and `${VAR}` env var expansion          |
| `readonly`               | `false`    | Enforce read-only sessions via `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` |
| `timeout`                | `10`       | Statement timeout in seconds                                                          |
| `max_rows`               | `1000`     | Maximum rows returned per query (auto-appended as LIMIT)                              |
| `pool_max`               | `1`        | Maximum connections in the pool. Per client on stdio; per server under `--http`, where sources that omit it get `10` |
| `max_response_bytes`     | `1000000`  | Maximum bytes in a single tool response. Rows past it are dropped and the result marked `truncated` |
| `allow_multi_statements` | `false`    | Allow semicolon-separated multi-statement queries                                     |
| `read_only_queries`      | see below  | Answer only read-only `SELECT` queries; reject everything else. Defaults to `true` when `role` or `session_vars` is set, else `false` |
| `role`                   | —          | `SET ROLE` to this role for each query (e.g. a restricted RLS reader)                 |
| `session_vars`           | —          | GUCs to `SET` per query, e.g. `{ "app.tenant_id" = "$TENANT_ID" }`, read by RLS policies |
| `ssh_host`               | —          | SSH bastion hostname for tunneled connections                                         |
| `ssh_user`               | —          | SSH username                                                                          |
| `ssh_key`                | —          | Path to SSH private key (supports `~` expansion)                                      |

#### Tenant isolation and `read_only_queries`

When a source scopes data to one tenant with `role` + `session_vars` (an RLS reader
plus a GUC like `app.tenant_id` that the policies read), that scope is only as strong
as the SQL the tool is allowed to run. A submitted query can otherwise re-point the
GUC at another tenant or leave the restricted role:

```sql
-- both re-point the tenant and read another tenant's rows
WITH x AS (SELECT set_config('app.tenant_id', 'victim', false)) SELECT * FROM orders, x;
SET app.tenant_id = 'victim';
RESET ROLE;  -- drops back to the connecting role
```

Postgres cannot lock a custom GUC (parameter ACLs are not enforced on placeholder
GUCs), so this is enforced at the query layer instead. `read_only_queries` is an
allowlist: it answers only read-only `SELECT`s and rejects everything else — every
non-SELECT statement (`SET`, `RESET`, `SET ROLE`, `COPY`, `DO`, `CALL`, DML, DDL,
transaction control), any call to a role/GUC setter (`set_config`, `set_role`) or a
server-side file/program/large-object function anywhere in the statement (including
CTEs and subqueries), and any query it cannot parse (which fails closed rather than
run). This is distinct from `readonly`, which only wraps each query in a read-only
transaction. It defaults on for any source that uses `role` or `session_vars`; set it
to `false` for a local source where you want free-form access.

`EXPLAIN` and `EXPLAIN (...)` are allowed and the statement behind them is guarded the
same way. `EXPLAIN ANALYZE` is rejected, because it runs the statement it explains.

The guard also rejects a string literal or a quoted identifier with a backslash
immediately before its closing quote. That is the one place where the SQL parser it
uses and PostgreSQL disagree about where a quoted token ends: the parser reads `\'` and
`\"` as escaped quotes and keeps consuming, while PostgreSQL with
`standard_conforming_strings = on` closes the token there and runs whatever follows the
next `;` as separate statements. Without that check, both

```sql
SELECT 'x\'; SET app.tenant_id TO "victim"; SELECT ...; --'
SELECT "x\"; SET app.tenant_id TO 'victim'; SELECT ...; --"
```

read as one clean `SELECT` to the guard and as three statements to the server.
Backslashes anywhere else in a literal or identifier are fine, as are dollar-quoted
strings and `E'...'` escape strings, whose boundaries both sides agree on. A value that
genuinely ends in a backslash has to be written as `E'\\'` or built with `chr(92)`; the
one form with no workaround is `LIKE ... ESCAPE '\'`, since the parser accepts only a
plain literal there. An identifier that really ends in a backslash cannot be addressed
through a `read_only_queries` source at all.

Because a hand-written lexer can always be wrong about a construct nobody has thought
of, a `read_only_queries` source also sends its statement over PostgreSQL's extended
query protocol. A `Parse` carrying more than one command is refused with `42601` before
any of it executes, so a statement smuggled past the lexer still never runs. The two
checks cover different things and both are needed: the server has no objection to
single-statement smuggling such as
`SELECT 'x\', (SELECT string_agg(val, ',') FROM secrets) --'`, which only the lexer
catches. Sources without `read_only_queries` keep the simple protocol, so
`allow_multi_statements` still works.

> A closed design goes the other way: PR #6 / branch `feat/per-request-session-vars`
> lets the caller supply session variables per request, which is the inverse of what
> `read_only_queries` enforces. Do not resurrect it without reconciling the two, or the
> tenant scope this guard protects becomes caller-controlled again.

### Global options

| Field                | Default | Description                                                                                                             |
| -------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------- |
| `prisma_schema_path` | —       | Path to your `.prisma` schema file. Also discovers `models/*.prisma` files. Leave it unset to run in plain-SQL mode.     |

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

### Without a Prisma schema

`prisma_schema_path` is optional, and everything Prisma-specific switches itself off when it is unset. There is no flag to set — the server has no mapping to talk about, so it stops talking about it:

- Table headers render as `users`, not `users (no Prisma model)`, and columns carry no `(Prisma: fieldName)` suffix.
- The `search_objects` description and its `pattern` parameter stop offering to search by Prisma model name.
- The `schema://` resource description drops its mention of Prisma model names.
- Drift warnings are suppressed. Drift is Prisma-versus-database by definition.
- The `schema://` map lists **every** table in the database. With a mapping loaded it stays filtered to the Prisma-mapped tables, which is the point of that view; without one that filter would leave the resource empty.

The header count on `schema://` always matches the tables the body actually lists, and the FK count only counts edges visible in the map.

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

On stdio each MCP session is its own process and opens at most `pool_max` connections per source (default: 1). With 10 engineers:

- **Default config**: 10 sessions × 1 connection = 10 connections
- **Idle release**: Connections drop after 5s of inactivity, so active count is typically lower
- **SSH tunnels**: One tunnel per session per SSH-enabled source

Under `--http` it is one process for everyone, so the ceiling is `pool_max` connections and one tunnel per source in total, no matter how many clients connect.

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
