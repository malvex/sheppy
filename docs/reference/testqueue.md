# `TestQueue` reference

`TestQueue` mirrors the `Queue` API but runs tasks synchronously in the test process without a separate worker or redis. Tasks only execute when you call `process_next()` or `process_all()`.

See the [Testing tasks](../guides/testing.md) guide for examples.

::: sheppy.TestQueue
    options:
        members:
            - add
            - schedule
            - get_task
            - get_all_tasks
            - get_scheduled
            - get_pending
            - retry
            - size
            - clear
            - add_cron
            - delete_cron
            - get_crons
            - process_next
            - process_all
            - process_scheduled
            - add_workflow
            - get_workflow
            - get_all_workflows
            - get_pending_workflows
            - delete_workflow
            - process_workflow
