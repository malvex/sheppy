# Sheppy Documentation

Welcome to the official documentation for Sheppy - a modern, async-native task queue for Python.

## What is Sheppy?

Sheppy is an async-native task queue designed to be simple enough to understand completely, yet powerful enough to handle millions of tasks in production. Built on asyncio from the ground up and uses blocking waits instead of polling. Sheppy scales from the smallest deployments to large distributed systems by simply launching more worker processes.

### Core Principles

- **Async Native**: Built on asyncio from the ground up
- **Simplicity**: Two main concepts - `@task` decorator and `Queue`
- **Low Latency**: Blocking reads instead of polling
- **Type Safety**: Full Pydantic integration for validation and serialization
- **Easy Scaling**: Just run more workers with `sheppy work`
- **No Magic**: Clear and understandable implementation

### Quick Example

Here's everything you need to know:

```python
import asyncio
from sheppy import Queue, RedisBackend, task

# 1. decorate your function
@task
async def calculate(x: int, y: int) -> int:
    return x + y

# 2. add tasks to a queue
async def main():
    queue = Queue(RedisBackend())
    await queue.add(calculate(1, 2))

if __name__ == "__main__":
    asyncio.run(main())

# 3. start a worker
# $ sheppy work
```

That's it. Everything else is just Python!

## Next Steps

Ready to get started? Head to the **[Getting Started Guide](getting-started/index.md)** to install Sheppy and create your first task!
