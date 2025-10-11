# Sheppy Documentation

Welcome to Sheppy - a modern, async-native task queue for Python.

## What is Sheppy?

Sheppy is an async-first task queue designed to be simple enough to understand completely, yet powerful enough to handle millions of tasks in production. Built on asyncio from the ground up and uses blocking waits instead of polling. Sheppy scales from the smallest deployments to large distributed systems by simply launching more worker processes.

### Core Principles

- **Async Native**: Built on asyncio from day one, not retrofitted
- **Simplicity**: Just two concepts - `@task` decorator and `Queue`
- **Zero Latency**: Redis Streams deliver tasks instantly, no polling
- **Type Safety**: Full Pydantic integration for automatic validation and serialization
- **Scale Naturally**: Just run more workers with `sheppy work`
- **No Magic** - You should be able to understand what's happening

## Why Sheppy?

### The Problem

Most task queues are either:

- **Too Old**: Built before async/await, type hints, and modern Python
- **Too Complex**: Require learning extensive APIs, configuration files, and deployment patterns
- **Too Slow**: Use polling intervals, adding unnecessary latency
- **Too Limited**: Often difficult to extend, Redis only, or missing critical features

### The Solution

The goal for this library is to be "simple, yet powerful". Sheppy is designed to be have minimal API interface with just a few simple concepts to learn, while implementing industry best practices. No complex abstractions, no unnecessary wrappers. Just functions (tasks) and queues. If you can't learn this library in 5 minutes, then I owe you a beer!

Long story short, this is all you need to know:

```python
import asyncio
from sheppy import Queue, RedisBackend, task

# 1. use sheppy task decorator to mark functions as tasks
@task
async def calculate(x: int, y: int) -> int:
    return x + y

# 2. add task to a queue (must be inside an async function)
async def main():
    queue = Queue(RedisBackend())
    await queue.add(calculate(1, 2))

if __name__ == "__main__":
    asyncio.run(main())

# 3. in terminal, spawn a worker process by running:
# $ sheppy work
```

That's it. Everything else is just Python!

## Next Steps

Ready to get started? Head to the **[Getting Started Guide](getting-started/index.md)** to install Sheppy and create your first task!
