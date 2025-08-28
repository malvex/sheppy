# Sheppy

Work in progress

## Quick Start

Check `examples/` directory, but tl;dr:

```bash
git clone https://github.com/malvex/sheppy.git
cd sheppy/examples/quick-start
uv sync

# start redis
docker-compose up -d

# start worker process
uv run sheppy work

# in another terminal, run the script
uv run main.py
```

### main.py

```python
import asyncio

from sheppy import Queue, RedisBackend, task


@task
async def calculate(x: int, y: int) -> int:
    return x + y


backend = RedisBackend("redis://127.0.0.1:6379")
queue = Queue("default", backend)


async def main() -> None:
    # create Task
    task = calculate(1, 2)

    # add Task to queue for processing
    await queue.add(task)

    # wait for task to finish
    task = await queue.wait_for_result(task)

    print(f"task result: {task.result}")
    print(f"completed: {task.completed}")
    print(f"error: {task.error}")


if __name__ == "__main__":
    asyncio.run(main())
```

### docker-compose.yml

```yaml
services:
  redis:
    image: redis:8-bookworm
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes

volumes:
  redis_data:
```

### pyproject.toml

```ini
[project]
name = "sheppy-quick-start"
version = "0.1.0"
description = ""
requires-python = ">=3.10"
dependencies = [
    "sheppy @ git+https://github.com/malvex/sheppy@master"
]
```
