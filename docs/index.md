# Sheppy

Sheppy is an async-native task queue for Python. Decorate a function with `@task`, add it to a `Queue`, and a worker process executes it. Arguments are validated with Pydantic, tasks are stored in Redis (and soon Postgres), and workers wait on blocking reads instead of polling.

```python
import asyncio
from sheppy import Queue, task

@task
async def send_email(to: str, subject: str) -> str:
    return f"sent '{subject}' to {to}"

async def main() -> None:
    queue = Queue("memory://")  # use "redis://localhost:6379" in production
    t = send_email("alice@example.com", "Welcome!")  # returns a Task; nothing runs yet
    await queue.add(t)
    done = await queue.wait_for(t)
    print(done.result)

if __name__ == "__main__":
    asyncio.run(main())
```

```bash
$ python example.py
sent 'Welcome!' to alice@example.com
```

The `memory://` backend executes tasks in-process, so this example needs no external services. In production you point the queue at Redis and run one or more worker processes with `sheppy work`.

## Where to go next

- [Quickstart](tutorials/quickstart.md): install, first task, first worker, in about five minutes.
- [How-to guides](guides/task-scheduling.md): scheduling, cron, retries, rate limiting, workflows, dependency injection, testing, FastAPI.
- [API reference](reference/queue.md): exact signatures and behavior of `Queue`, `Worker`, `TestQueue`, and the models.
- [How Sheppy works](explanation/how-it-works.md): the architecture and the delivery guarantees, stated plainly.
- [Code examples](examples/index.md): runnable scripts from the repository, each with the command to run it.
