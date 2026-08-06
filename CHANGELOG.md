# @passionfroot/postgres-mcp

## 0.4.0

### Minor Changes

- Add automatic expansion of `SELECT *` and `SELECT t.*` to the caller's actually-accessible columns, using the schema cache's privilege-filtered column list. On a source with column-level security, `SELECT * FROM creators` previously failed with `permission denied` on the first restricted column; it now runs as `SELECT "id", "displayName", ... FROM "creators"`, expanded to only the columns the role can read. Falls back to the original SQL unmodified when the query can't be parsed, isn't a single SELECT, or references a table not in the schema cache.

- Suppress Prisma output automatically when no `prisma_schema_path` is configured, and fix the empty `schema://` resource.

  Previously the renderers annotated regardless of whether a mapping had been loaded, so a server with no `prisma_schema_path` tagged every table `(no Prisma model)`, advertised `search_objects` as searchable "by Prisma model name", and described `schema://` as showing Prisma model names — all of it noise for a consumer that only writes SQL.

  `schema://` was worse than noisy: it filtered to Prisma-mapped tables unconditionally, so with no mapping loaded it served a body with no tables at all under a header that still counted the database's foreign keys. It is now the full table list in that case, and stays filtered to the mapped tables when a mapping is loaded.

  - No Prisma annotations, tool wording, resource wording or drift warnings when the parsed mapping is empty.
  - `schema://` lists every table when no mapping is loaded.
  - The `schema://` header count now matches the body: tables counted are the tables rendered, and FKs counted are the edges the map actually shows.
  - Output is unchanged when a Prisma schema is loaded, except that the header's FK count no longer includes edges between two tables the map does not list.

- Harden the `--http` transport for shared use.

  - Request bodies are capped at 4 MB and answered with `413`. An oversized body used to throw `ERR_STRING_TOO_LONG` inside a stream listener and kill the process.
  - `pool_max` is a per-server budget under `--http`; sources that do not set one get `10` instead of `1`, so concurrent clients stop serializing on a single connection.
  - `connectionTimeoutMillis` and `query_timeout` are now set, ordered above `statement_timeout`, so an unreachable host fails on the source's timescale instead of the OS TCP timeout.
  - Concurrent cold starts share the in-flight pool, tunnel and schema introspection instead of each creating their own.
  - `SIGTERM` closes the transport before the pools, so a shutdown with a query in flight exits 0.
  - Tool responses are capped by `max_response_bytes` (new per-source option, default 1 MB).
  - Sessions have a cap (`503` past it) and an idle timeout.
  - `Origin` is validated; IPv6 bind addresses are bracketed; the loopback check covers all of `127.0.0.0/8`.
  - The bearer token is compared in constant time and can be read from a file with `--token-file`.
  - An unknown CLI flag is warned about instead of exiting non-zero.

- Restrict tenant-scoped sources to read-only SELECT queries.

  A new `read_only_queries` source option answers only read-only `SELECT` statements and rejects everything else: non-SELECT statements (`SET`, `RESET`, `SET ROLE`, `COPY`, `DO`, `CALL`, DML, DDL, transaction control), role/GUC setters and server-side file/program functions anywhere in the statement including CTEs and subqueries, multiple statements in one call, and anything the parser cannot verify. `EXPLAIN` without `ANALYZE` is allowed and the statement behind it is guarded the same way.

  It also rejects a string literal or a quoted identifier with a backslash immediately before its closing quote. That is the one construct where the SQL parser and PostgreSQL disagree about where a quoted token ends, which let both `SELECT 'x\'; SET app.tenant_id TO "victim"; SELECT ...; --'` and `SELECT "x\"; SET app.tenant_id TO 'victim'; SELECT ...; --"` read as one clean `SELECT` to the guard and as three statements to the server.

  As a second, independent line of defence, a `read_only_queries` source sends its statement over the extended query protocol. PostgreSQL refuses a `Parse` carrying more than one command with `42601` before executing any of it, so a statement smuggled past the lexer still never runs. Sources without `read_only_queries` keep the simple protocol, so `allow_multi_statements` is unaffected.

  **Behavioural change**: `read_only_queries` defaults to `true` for any source that already sets `role` or `session_vars`, because that is where tenant scope lives and a submitted query could otherwise re-point it. Existing sources of that shape stop accepting non-SELECT SQL. Set `read_only_queries = false` to keep the old behaviour.

  Rejections now carry the specific reason that fired instead of a generic message, and each one emits a `blocked session-mutating query` warning so blocked attempts are visible without an `[audit_log]` configured.

- Annotate incoming foreign keys in `search_objects` and `schema://` with join cardinality (`[1:1]` or `[1:many]`), and the columns they join on. A table with any `[1:many]` incoming FK gets a fan-out warning recommending a subquery, `LATERAL JOIN`, or `DISTINCT ON`. The foreign-key query that backs this is now ordered by constraint and declared column position instead of alphabetically, and checks `SELECT` privilege on the referenced column as well as the referencing one, so a restricted role (e.g. `zest_mcp_reader`) no longer gets a permission-denied error introspecting a table it can otherwise read.

### Patch Changes

- Fix schema introspection returning columns the connected role cannot actually query. `information_schema.columns` returns every column the role has any privilege on, including columns visible only through an inherited REFERENCES grant from a foreign key, not just SELECT — so `search_objects` advertised columns that failed with `permission denied` when queried. Introspection now checks `has_column_privilege(..., 'SELECT')` (and `has_table_privilege` for the PK/FK queries), and applies a source's configured `role`/`session_vars` before introspecting, so the schema reflects the same effective permissions as query execution rather than the connecting role's raw grants.

- Fix `timestamp` and `date` columns, including their array variants, coming back shifted by the server's local UTC offset. These types carry no time zone, but pg's default parser reads them as local time and JSON serialization re-emits that as a UTC instant, so a stored `2026-07-02 11:43:00` came back as `2026-07-02T09:43:00.000Z` on a UTC+2 machine, and plain dates could shift a whole calendar day. `timestamptz` was unaffected. Both types are now returned as the literal string PostgreSQL sent, with no time zone conversion applied.

- Fix `executeQuery` throwing a `TypeError` on multi-statement result sets. node-postgres returns an array of results, one per statement, whenever the server executed more than one command; `executeQuery` read `.rows` off it as if it were a single result. It now returns the batch's last row-returning result, so `BEGIN; SELECT ...; COMMIT` and `SET x; SELECT y` come back with the SELECT rather than with the trailing `COMMIT` or the leading `SET`. `max_rows` is pushed onto that statement as a `LIMIT` where the batch survives a parse and re-print, so the server stops producing rows instead of sending them all across the wire to be sliced here.

  A batch with more than one row-returning statement is ambiguous and is now rejected by the parser before anything is sent to the database, instead of after the whole batch has already run.

  On a source without `allow_multi_statements = true`, an array of results means PostgreSQL split the input into statements that the SQL parser read as one. Such input never passed a multi-statement check, so it is now rejected and logged at that point rather than having its last result returned. The check that runs before execution only rejects what the parser can see; `read_only_queries = true`, which sends the statement over the extended protocol where PostgreSQL refuses a multi-command parse, is what keeps a smuggled statement from running at all.

- Fix the SSH tunnel not recreating after the connection dies post-establishment (e.g. after sleep/wake): a `closing` flag now guards the tunnel and pool against acting on their own `error`/`close` events during an intentional shutdown, and `getPool` memoizes in-flight pool creation per source so concurrent callers can't race into creating two pools for the same source.

- Fix enum values in `search_objects` being truncated for every enum, including ones resolved from the Prisma schema. Truncation now only applies when the enum's values came from a raw database fallback (no Prisma schema loaded, or the Prisma enum wasn't found), matching the original intent of the row-count limit.

- Fix composite foreign keys being reported as non-unique when only a subset of their columns had a unique index. Uniqueness is now checked against every subset of a composite FK's columns, not just an exact match on the full set.

## 0.3.0 and earlier

Not tracked in this file. See `git log`.
