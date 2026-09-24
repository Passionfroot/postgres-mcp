---
"@passionfroot/postgres-mcp": minor
---

Take the config from the environment when no config path is given: `POSTGRES_MCP_DSN` holds one connection string and builds a single read-only source, for hosts with nowhere to write a file, and `POSTGRES_MCP_CONFIG` holds a path. An argument still wins over both. A TOML parse error no longer quotes the offending line back, which kept a `dsn` password out of the startup log.
