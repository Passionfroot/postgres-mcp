---
"@passionfroot/postgres-mcp": minor
---

Suppress Prisma output automatically when no `prisma_schema_path` is configured, and fix the empty `schema://` resource.

Previously the renderers annotated regardless of whether a mapping had been loaded, so a server with no `prisma_schema_path` tagged every table `(no Prisma model)`, advertised `search_objects` as searchable "by Prisma model name", and described `schema://` as showing Prisma model names — all of it noise for a consumer that only writes SQL.

`schema://` was worse than noisy: it filtered to Prisma-mapped tables unconditionally, so with no mapping loaded it served a body with no tables at all under a header that still counted the database's foreign keys. It is now the full table list in that case, and stays filtered to the mapped tables when a mapping is loaded.

- No Prisma annotations, tool wording, resource wording or drift warnings when the parsed mapping is empty.
- `schema://` lists every table when no mapping is loaded.
- The `schema://` header count now matches the body: tables counted are the tables rendered, and FKs counted are the edges the map actually shows.
- Output is unchanged when a Prisma schema is loaded, except that the header's FK count no longer includes edges between two tables the map does not list.
