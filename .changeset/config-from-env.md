---
"@passionfroot/postgres-mcp": minor
---

Read the config from `POSTGRES_MCP_CONFIG` when no config path is given, for hosts that can set an environment variable but have nowhere to write a file. A path passed as an argument still wins.
