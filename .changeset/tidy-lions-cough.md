---
"@passionfroot/postgres-mcp": patch
---

Fix `executeQuery` throwing a `TypeError` on multi-statement result sets. node-postgres returns an array of results, one per statement, whenever the server executed more than one command; `executeQuery` read `.rows` off it as if it were a single result. It now returns the batch's last row-returning result, so `BEGIN; SELECT ...; COMMIT` and `SET x; SELECT y` come back with the SELECT rather than with the trailing `COMMIT` or the leading `SET`. `max_rows` is pushed onto that statement as a `LIMIT` where the batch survives a parse and re-print, so the server stops producing rows instead of sending them all across the wire to be sliced here.

A batch with more than one row-returning statement is ambiguous and is now rejected by the parser before anything is sent to the database, instead of after the whole batch has already run.

On a source without `allow_multi_statements = true`, an array of results means PostgreSQL split the input into statements that the SQL parser read as one. Such input never passed a multi-statement check, so it is now rejected and logged at that point rather than having its last result returned. The check that runs before execution only rejects what the parser can see; `read_only_queries = true`, which sends the statement over the extended protocol where PostgreSQL refuses a multi-command parse, is what keeps a smuggled statement from running at all.
