# Dependency injection

Task functions can declare dependencies with `Depends`, the same pattern FastAPI uses. Dependencies are resolved when the task executes, not when it is queued, so they never need to be serialized.

## Declare a dependency

```python
from sheppy import Depends, task

def get_db() -> str:
    return "PROD DATABASE"

@task
def my_task(db: str = Depends(get_db)) -> str:
    return db
```

The `db` parameter is excluded from the stored task arguments. The worker calls `get_db()` at execution time and passes the result in. The `Annotated` style works too: `db: Annotated[str, Depends(get_db)]`.

What a dependency can be:

- a sync or async function
- another dependency (dependencies resolve recursively)
- a sync or async generator. Only the first yielded value is used, and the generator is *not* closed after the task finishes. Don't rely on teardown code in a generator dependency

## Access the current task

`CURRENT_TASK` is a sentinel default that injects the running `Task` object:

```python
from sheppy import CURRENT_TASK, Task, task

@task
async def report(url: str, current: Task = CURRENT_TASK) -> str:
    return f"attempt {current.retry_count + 1} for {url}"
```

## Override dependencies in tests

`dependency_overrides` maps an original dependency to a replacement. It exists on `TestQueue`, `Worker`, and `MemoryBackend`:

```python title="example_dependency.py"
--8<-- "examples/example_dependency.py"
```

The task resolves `get_db` through the override map and receives `"TESTING DATABASE"`. See [Testing tasks](testing.md) for the full testing setup.
