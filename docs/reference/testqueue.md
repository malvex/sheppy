# `TestQueue` reference

Sheppy offers a first class support for testing tasks using the `TestQueue` class. This class mimics the behavior of a real queue but operates synchronously, making it ideal for predictable and fast unit tests.

See [Testing tasks](../getting-started/testing.md) guide for more details and examples.

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
