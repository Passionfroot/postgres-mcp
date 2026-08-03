---
"@passionfroot/postgres-mcp": patch
---

Fix `executeQuery` throwing a `TypeError` on multi-statement result sets. On sources configured with `allow_multi_statements = true`, node-postgres returns an array of results instead of a single one; `executeQuery` now returns the final statement's rows (matching `psql`/JDBC batch semantics) and throws an explicit error instead of silently discarding rows if more than one statement in the batch produced them. Sources without `allow_multi_statements = true` keep their guard: parseable multi-statement input is still rejected before reaching the database, unchanged from before this fix.
