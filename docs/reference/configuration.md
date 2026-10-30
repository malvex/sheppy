# Configuration

Sheppy is configured through environment variables, loaded once at import time.

| Variable | Type | Default | Used for |
|---|---|---|---|
| `SHEPPY_BACKEND_URL` | str | (none) | Backend when `Queue()` or the CLI get no explicit backend. Schemes: `redis://`, `rediss://` (TLS), `memory://`. |
| `SHEPPY_QUEUE` | str | `default` | Queue name. Comma-separated values give multiple queues (used by `sheppy work`). |
| `SHEPPY_MAX_CONCURRENT_TASKS` | int | `10` | Max tasks a worker processes concurrently. |
| `SHEPPY_SHUTDOWN_TIMEOUT` | float | `30.0` | Seconds a worker waits for active tasks at shutdown before cancelling them. |
| `SHEPPY_LOG_LEVEL` | str | `info` | Log verbosity: `debug`, `info`, `warning`, or `error`. |
| `SHEPPY_LOG_FORMAT` | str | `console` | Worker log output format: `console` (human-readable Rich output) or `json` (newline-delimited JSON). Case-insensitive. |
| `SHEPPY_PRESTART` | str | (none) | Prestart hook as a `"module:function"` path, run once before the `sheppy work` CLI starts the worker. |

Example:

```bash
export SHEPPY_BACKEND_URL=redis://localhost:6379
export SHEPPY_QUEUE=emails,reports
sheppy work
```

## JSON logging

For production deployments that feed logs into a collector, the `sheppy work` CLI can emit structured JSON instead of the default Rich console output:

```bash
sheppy work --log-format json
# or via environment:
SHEPPY_LOG_FORMAT=json sheppy work
```

The environment variable is case-insensitive. Both settings accept only `console` or `json`; other values are rejected.

In JSON mode the worker writes compact newline-delimited JSON (one object per record) to stdout:

```json
{"timestamp":"2026-10-25T15:02:25Z","level":"INFO","logger":"sheppy.worker","message":"Queue drained","queue":"default"}
```

Every record contains:

| Field | Meaning |
|---|---|
| `timestamp` | UTC ISO-8601 timestamp with second precision, ending in `Z`. |
| `level` | Log level name (`DEBUG`, `INFO`, `WARNING`, `ERROR`, ...). |
| `logger` | Full logger name (e.g. `sheppy.worker`). |
| `message` | The log message. |

Console output keeps its short component column; only the JSON records omit it.

Records also carry structured context fields relevant to the event: task records include `function`, `queue`, `task_id`, and `retry_at`/`error` where relevant, and the startup record includes the sanitized `backend_url` (passwords redacted) and the effective worker configuration. Logged exceptions add an `exception` object with `type`, `message`, and `traceback` (no local variables).

JSON mode suppresses the Rich startup/prestart banner and emits structured `Starting worker` and `Running prestart hook` events instead, so all Sheppy-generated output in normal operation is JSON.

!!! note
    JSON mode only formats Sheppy's own logger output. Application, task, or prestart code that prints directly to stdout/stderr or configures its own logging is not captured by this option and may interleave non-JSON output.
