# Sheppy 🐕

Documentation: <a href="https://docs.sheppy.org" target="_blank">https://docs.sheppy.org</a>

---

## What is Sheppy?

Sheppy is an async-native task queue designed to be simple enough to understand completely, yet powerful enough to handle millions of tasks in production. Built on asyncio from the ground up and uses blocking waits instead of polling. Sheppy scales from the smallest deployments to large distributed systems by simply launching more worker processes.

### Core Principles

- **Async Native**: Built on asyncio from the ground up
- **Simplicity**: Two main concepts - `@task` decorator and `Queue`
- **Low Latency**: Blocking reads instead of polling
- **Type Safety**: Full Pydantic integration for validation and serialization
- **Easy Scaling**: Just run more workers with `sheppy work`
- **No Magic**: Clear and understandable implementation

## TL;DR Quick Start

This is all you need to know:

```python
import asyncio
from datetime import datetime, timedelta
from sheppy import Queue, task, RedisBackend

queue = Queue(RedisBackend("redis://127.0.0.1:6379"))

@task
async def say_hello(to: str) -> str:
    s = f"Hello, {to}!"
    print(s)
    return s

async def main():
    t1 = say_hello("World")
    await queue.add(t1)
    await queue.add(say_hello("Moon"))
    await queue.schedule(say_hello("Patient Person"), at=timedelta(seconds=10))  # runs in 10 seconds from now
    await queue.schedule(say_hello("New Year"), at=datetime.fromisoformat("2026-01-01 00:00:00 +00:00"))

    # await the task completion
    updated_task = await queue.wait_for(t1)

    if updated_task.error:
        print(f"Task failed with error: {updated_task.error}")
    elif updated_task.completed:
        print(f"Task succeed with result: {updated_task.result}")
        assert updated_task.result == "Hello, World!"
    else:
        # note: this won't happen because wait_for doesn't return pending tasks
        print("Task is still pending!")

if __name__ == "__main__":
    asyncio.run(main())
```

Run it:

```bash
# run the app:
python examples/tldr.py  # nothing will happen because worker isn't running

# in another terminal, you can list queued tasks:
sheppy task list  # (shows 2 pending and 2 scheduled tasks)

# run worker process to process the tasks
sheppy work  # (you should see the tasks to get processed, and the app should finish!)
```

For more details, see the <a href="https://docs.sheppy.org/getting-started/" target="_blank">Getting Started Guide</a>.

## Requirements

- Python 3.10+
- Redis 8+

## Developing

```bash
git clone https://github.com/malvex/sheppy.git
cd sheppy
uv sync --group dev

pytest -v tests/ --tb=short
mypy src/
ruff check src/
```

## License

This project is licensed under the terms of the MIT license.
