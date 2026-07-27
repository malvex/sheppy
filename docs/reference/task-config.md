# `TaskConfig` model reference

`TaskConfig` holds execution behavior: retry count and delays, timeout, rate limiting, and metadata expiry (`ttl` / `error_ttl`). Set it through the `@task(...)` decorator options rather than constructing it directly.

::: sheppy.models.TaskConfig
