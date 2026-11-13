# `Queue` reference

`Queue` is the main entry point of Sheppy: it stores and tracks tasks. (Workers execute them - see the [`Worker` reference](worker.md).)

Construct it with a `Backend` instance or a URL string: `"redis://localhost:6379"`, `"rediss://…"` for TLS, or `"memory://"` for the in-process backend. With no arguments it falls back to the `SHEPPY_BACKEND_URL` environment variable. When using a URL string, `backend_kwargs` forwards options such as `ttl` and `error_ttl` to the backend constructor.

See the [Quickstart](../quickstart.md) and the [how-to guides](../guides/task-scheduling.md) for examples.

Workflow operations live under `queue.experimental`; see the [Workflows guide](../guides/workflows.md).

::: sheppy.Queue
    options:
        members:
            - add
            - schedule
            - get_task
            - wait_for
            - get_all_tasks
            - get_scheduled
            - get_pending
            - retry
            - size
            - clear
            - add_cron
            - delete_cron
            - get_crons
