# Cron Jobs

`queue.add_cron()` registers a recurring task. While any worker is running, its cron manager creates task instances from the expression and schedules them; ordinary workers then execute them.

## Register a cron

```python
import asyncio
from sheppy import Queue, task

@task
def ping() -> str:
    print("ping fired")
    return "pong"

async def main() -> None:
    queue = Queue("redis://localhost:6379")
    await queue.add_cron(ping(), "*/1 * * * *")  # every minute

    for cron in await queue.get_crons():
        print("registered:", cron.expression, "| next run:", cron.next_run().isoformat())

if __name__ == "__main__":
    asyncio.run(main())
```

```bash
$ python cron_demo.py
registered: */1 * * * * | next run: 2026-07-19T13:39:00+00:00
```

(The next run depends on when you run it.) Start a worker with `sheppy work` and the task fires within a minute. The worker log shows `Processing task ... (cron_demo:ping)` once per minute.

Expressions use standard five-field cron syntax (`minute hour day-of-month month day-of-week`). An invalid expression raises a `ValidationError` at registration.

## Behavior worth knowing

- `add_cron()` returns `False` for an exact duplicate. Crons have deterministic ids derived from the function, its arguments, and the expression, so registering the same cron twice is safe.
- The cron manager runs inside every worker (unless started with `--disable-cron-manager`) and stays about three scheduled runs ahead. If no worker is running, no task instances are created. Registration alone does not execute anything.
- `queue.delete_cron(task, "*/1 * * * *")` removes a cron; pass the same task and expression you registered.
- `sheppy cron list` shows registered crons from the CLI.

## Declarative cron jobs (pyproject.toml)

Instead of calling `add_cron()`, you can declare cron jobs in your project's `pyproject.toml`:

```toml
[[tool.sheppy.cron]]
task = "myapp.tasks:cleanup"
expression = "0 3 * * *"
args = [30]

[[tool.sheppy.cron]]
task = "myapp.tasks:backup"
expression = "0 4 * * sun"
```

Every worker started with `sheppy work` from that directory reads the file and reconciles continuously:

- crons declared in the file are created and marked as file-managed
- remove an entry from the file and the worker deletes that cron within seconds
- crons added through `queue.add_cron()` are left alone, even when identical to a declaration

Entries take optional `args`, `kwargs`, and `queue` (which defaults to the worker's first queue). Invalid entries are logged and skipped, and if the file is missing or broken the worker keeps the current state instead of deleting anything. Use `Worker(cron_config_file=...)` for the programmatic path.

## When to use cron vs `schedule()`

Use `queue.schedule()` for one-off future tasks ("send this email in 30 minutes"). Use `add_cron()` for recurring work ("nightly cleanup at 03:00"). A cron registration persists until you delete it; a scheduled task fires once.
