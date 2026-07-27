# Using Sheppy without asyncio

`SyncQueue` wraps `Queue` for codebases that don't use asyncio. It mirrors the queue API with plain blocking calls.

```python
from sheppy import SyncQueue, task


@task
def add(x: int, y: int) -> int:
    return x + y


def main() -> None:
    q = SyncQueue("memory://")  # use "redis://localhost:6379" in production

    t = add(2, 3)
    q.add(t)

    done = q.wait_for(t)
    print(done.result)

    q.close()


if __name__ == "__main__":
    main()
```

```bash
$ python sync_example.py
5
```

## How it works

`SyncQueue` runs one asyncio event loop in a background daemon thread and submits every call to it, blocking the calling thread until the result is ready. One instance can be shared between threads. Call `close()` when done to stop the loop.

Two differences from `Queue`:

- The backend can only be given as a URL string (or the `SHEPPY_BACKEND_URL` environment variable), not as a `Backend` instance
- Long waits (like `wait_for`) block the calling thread, not the event loop

Workers are unchanged. `sheppy work` from the command line works the same regardless.
