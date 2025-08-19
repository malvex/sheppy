# Sheppy

Work in progress

## Quick Start

Check `examples/` directory, but tl;dr:

### main.py

```python
import asyncio
from sheppy import RedisBackend, Queue
from tasks import send_email

backend = RedisBackend("redis://localhost:6379")
queue = Queue("email-queue", backend)

async def main():
    # Create Task
    task = send_email("user@example.com")

    # Add Task to queue for processing
    await queue.add(task)

    # wait for task to finish
    task = await queue.wait_for_finished(task)

    assert task.result == "sent"
    assert task.completed
    assert not task.error


if __name__ == "__main__":
    asyncio.run(main())
```

### tasks.py

```python
from sheppy import task

@task
async def send_email(recipient: str) -> str:
    print(f"Sending email to {recipient}")
    return "sent"
```

### run_worker.py

```python
import asyncio
import logging
from sheppy import Worker
from main import backend

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def run_worker():
    worker = Worker(queue_name="email-queue", backend=backend)
    await worker.work()


if __name__ == "__main__":
    asyncio.run(run_worker())
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
requires-python = ">=3.12"
dependencies = [
    "sheppy @ git+https://github.com/malvex/sheppy@master"
]
```
