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

When no value is supplied, the `db` parameter is excluded from the stored task arguments. The worker calls `get_db()` at execution time and passes the result in. An explicitly supplied positional or keyword value is stored and used instead of resolving the dependency. The `Annotated` style works too: `db: Annotated[str, Depends(get_db)]`.

What a dependency can be:

- a sync or async function
- another dependency (dependencies resolve recursively)
- a sync or async generator that yields exactly one value

## Cache dependencies

Within one task execution attempt, the same dependency callable is resolved only once by default. The cached value is shared across repeated parameters and nested dependencies. Each attempt has its own cache, so retries resolve dependencies again.

Set `use_cache=False` on a declaration to resolve that occurrence again without replacing an already cached value:

```python
@task
def my_task(
    shared: str = Depends(get_db),
    fresh: str = Depends(get_db, use_cache=False),
) -> tuple[str, str]:
    return shared, fresh
```

## Generator dependencies and scopes

A generator dependency runs before its first `yield` during resolution. Code after the `yield` runs during cleanup in reverse dependency-entry order, including when the task fails or times out.

```python
def get_db():
    db = connect()
    try:
        yield db
    finally:
        db.close()
```

`Depends` accepts a `scope` that controls when cleanup runs:

- `"request"` (the default): cleanup runs after post-middleware, at the end of the task execution attempt and before the result is persisted. Here, "request" means one attempt, not an HTTP request.
- `"function"`: cleanup runs after the task callable and result construction, before post-middleware. On a task exception, the generator is unwound with that exception before the task is marked failed.

A cleanup error in either scope fails the task attempt and follows the task's normal retry policy.

```python
@task
def my_task(db: str = Depends(get_db, scope="function")) -> str:
    return db
```

A request-scoped generator cannot depend on a function-scoped dependency because the shorter-lived dependency would already be closed during its parent's cleanup. Sheppy raises `DependencyScopeError` for this invalid relationship and does not schedule a retry for the misconfiguration. Each retry creates fresh generator dependencies and runs their cleanup independently.

## Access the current task

`CURRENT_TASK` is a sentinel default that injects the running `Task` object:

```python
from sheppy import CURRENT_TASK, Task, task

@task
async def report(url: str, current: Task = CURRENT_TASK) -> str:
    return f"attempt {current.retry_count + 1} for {url}"
```

Dependencies can declare `CURRENT_TASK` too. Nested dependencies receive the same running `Task` object as the task function.

## Override dependencies in tests

`dependency_overrides` maps an original dependency to a replacement. It exists on `TestQueue`, `Worker`, and `InMemoryBackend`:

```python title="example_dependency.py"
--8<-- "examples/example_dependency.py"
```

The task resolves `get_db` through the override map and receives `"TESTING DATABASE"`. See [Testing tasks](testing.md) for the full testing setup.
