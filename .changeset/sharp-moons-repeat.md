---
"@passionfroot/postgres-mcp": minor
---

Restrict tenant-scoped sources to read-only SELECT queries.

A new `read_only_queries` source option answers only read-only `SELECT` statements and
rejects everything else: non-SELECT statements (`SET`, `RESET`, `SET ROLE`, `COPY`,
`DO`, `CALL`, DML, DDL, transaction control), role/GUC setters and server-side
file/program functions anywhere in the statement including CTEs and subqueries,
multiple statements in one call, and anything the parser cannot verify. `EXPLAIN`
without `ANALYZE` is allowed and the statement behind it is guarded the same way.

It also rejects a string literal or a quoted identifier with a backslash immediately
before its closing quote. That is the one construct where the SQL parser and PostgreSQL
disagree about where a quoted token ends, which let both `SELECT 'x\'; SET
app.tenant_id TO "victim"; SELECT ...; --'` and `SELECT "x\"; SET app.tenant_id TO
'victim'; SELECT ...; --"` read as one clean `SELECT` to the guard and as three
statements to the server.

As a second, independent line of defence, a `read_only_queries` source sends its
statement over the extended query protocol. PostgreSQL refuses a `Parse` carrying more
than one command with `42601` before executing any of it, so a statement smuggled past
the lexer still never runs. Sources without `read_only_queries` keep the simple
protocol, so `allow_multi_statements` is unaffected.

Behavioural change: `read_only_queries` defaults to `true` for any source that already
sets `role` or `session_vars`, because that is where tenant scope lives and a
submitted query could otherwise re-point it. Existing sources of that shape stop
accepting non-SELECT SQL. Set `read_only_queries = false` to keep the old behaviour.

Rejections now carry the specific reason that fired instead of a generic message, and
each one emits a `blocked session-mutating query` warning so blocked attempts are
visible without an `[audit_log]` configured.
