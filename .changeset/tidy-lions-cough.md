---
"@passionfroot/postgres-mcp": patch
---

Fix `executeQuery` throwing a `TypeError` on multi-statement result sets. node-postgres returns an array of results, one per statement, whenever the server executed more than one command; `executeQuery` read `.rows` off it as if it were a single result. It now returns the batch's last row-returning result, so `BEGIN; SELECT ...; COMMIT` and `SET x; SELECT y` come back with the SELECT rather than with the trailing `COMMIT` or the leading `SET`. A batch with more than one row-returning statement stays rejected, since there is no single result set to return.

On a source without `allow_multi_statements = true`, an array of results means PostgreSQL split the input into statements that the SQL parser read as one. Such input never passed a multi-statement check, so it is now rejected and logged at that point rather than having its last result returned. The check that runs before execution only rejects what the parser can see; `read_only_queries = true`, which sends the statement over the extended protocol where PostgreSQL refuses a multi-command parse, is what keeps a smuggled statement from running at all.
