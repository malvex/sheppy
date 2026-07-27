# `Queue` reference

`Queue` is the main entry point of Sheppy: it stores and tracks tasks. (Workers execute them - see the [`Worker` reference](worker.md).)

Construct it with a `Backend` instance or a URL string: `"redis://localhost:6379"`, `"rediss://…"` for TLS, or `"memory://"` for the in-process backend. With no arguments it falls back to the `SHEPPY_BACKEND_URL` environment variable.

See the [Quickstart](../tutorials/quickstart.md) and the [how-to guides](../guides/task-scheduling.md) for examples.

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
            - add_workflow
            - wait_for_workflow
            - get_workflow
            - get_all_workflows
            - get_pending_workflows
            - delete_workflow
