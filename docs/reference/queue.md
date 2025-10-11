# `Queue` reference

Sheppy provides a `Queue` class to manage and execute background tasks. The queue supports adding tasks, scheduling them for future execution, retrying failed tasks, and managing periodic tasks using cron expressions.

See [Getting Started](../getting-started/index.md) guide for more details and examples.

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
