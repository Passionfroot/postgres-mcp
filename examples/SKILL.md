---
name: postgres-query
description: Assists with writing SQL queries for project databases. Orchestrates schema lookup via subagent to keep main context clean. Use when asked to query data, debug database issues, or explore the schema.
allowed-tools: mcp__postgres__execute_sql, mcp__postgres__search_objects
---

# Postgres Query Skill

Write correct SQL queries by looking up the schema first, never guessing table or column names.

## Query Writing Workflow

Follow this three-step pattern to keep the main context clean while getting accurate schema information.

### Step 1: Spawn a Schema Investigation Subagent

Use the Task tool to investigate the schema before writing any query. This keeps column-level detail out of the main conversation.

Pass the subagent this prompt template (fill in `[database]` and `[question]`):

```
Investigate the [database] database schema to help answer: [question]

1. Read schema://[database] for the relationship overview
2. For each relevant table, call search_objects with the table name
3. Return: table names, relevant columns (with types), JOIN paths between tables, any enum values needed

Keep the summary compact -- only include columns relevant to the question.
```

The subagent will return a compact summary with table names, column types, JOIN paths, and enum values.

### Step 2: Write the SQL Query

Use the subagent's summary to write the query. Apply the gotchas below.

### Step 3: Execute

Run with `execute_sql` against the target database.

## Critical Gotchas

### Always Double-Quote camelCase Columns

PostgreSQL folds unquoted identifiers to lowercase. Every camelCase column must be double-quoted:

```sql
-- Wrong
SELECT createdAt, partnerId FROM collaborations;

-- Correct
SELECT "createdAt", "partnerId" FROM collaborations;
```

### Use SQL Table Names, Not Prisma Model Names

Prisma models often have different names than the underlying SQL tables. When in doubt, use `search_objects` with the Prisma model name — it returns the SQL table name.

### Enum Columns Are Strings

Prisma enums map to PostgreSQL custom types. Query them as quoted strings:

```sql
SELECT * FROM orders WHERE status = 'COMPLETED' LIMIT 10;
```

Use `search_objects` to see available enum values for a column.

## Available Databases

<!-- Customize this table with your actual database sources -->

| ID         | Environment | Access     | Use When                           |
| ---------- | ----------- | ---------- | ---------------------------------- |
| local      | Development | read-write | Local development and testing      |
| staging    | Staging     | read-only  | Verifying staging data             |
| production | Production  | read-only  | Debugging live issues              |

Default to `local` for exploration. Use `production` only when debugging live issues.

## Common Query Patterns

<!-- Add project-specific query patterns here. Examples: -->

<!--
### Find User by Email

```sql
SELECT u.id, u.name, u.email
FROM users u
WHERE u.email = 'user@example.com'
LIMIT 1;
```
-->
