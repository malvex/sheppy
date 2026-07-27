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

## When to use cron vs `schedule()`

Use `queue.schedule()` for one-off future tasks ("send this email in 30 minutes"). Use `add_cron()` for recurring work ("nightly cleanup at 03:00"). A cron registration persists until you delete it; a scheduled task fires once.
