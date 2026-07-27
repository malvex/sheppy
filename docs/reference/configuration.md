# Configuration

Sheppy is configured through environment variables, loaded once at import time.

| Variable | Type | Default | Used for |
|---|---|---|---|
| `SHEPPY_BACKEND_URL` | str | (none) | Backend when `Queue()` or the CLI get no explicit backend. Schemes: `redis://`, `rediss://` (TLS), `memory://`. |
| `SHEPPY_QUEUE` | str | `default` | Queue name. Comma-separated values give multiple queues (used by `sheppy work`). |
| `SHEPPY_MAX_CONCURRENT_TASKS` | int | `10` | Max tasks a worker processes concurrently. |
| `SHEPPY_SHUTDOWN_TIMEOUT` | float | `30.0` | Seconds a worker waits for active tasks at shutdown before cancelling them. |
| `SHEPPY_LOG_LEVEL` | str | `info` | Log verbosity: `debug`, `info`, `warning`, or `error`. |

Example:

```bash
export SHEPPY_BACKEND_URL=redis://localhost:6379
export SHEPPY_QUEUE=emails,reports
sheppy work
```
