---
"@passionfroot/postgres-mcp": minor
---

Harden the `--http` transport for shared use.

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
