# Validation with Pydantic

Task calls are validated twice: arguments when you create the task, and the result when the worker finishes. Invalid input fails before anything reaches the queue.

## Argument validation

Calling a `@task` function validates the arguments against the function's type hints. Bad arguments raise a Pydantic `ValidationError` immediately:

```python title="examples/pydantic_example.py"
--8<-- "examples/pydantic_example.py"
```

```bash
$ python examples/pydantic_example.py
pydantic_core._pydantic_core.ValidationError: 3 validation errors for UserData
name
  Field required [type=missing, input_value={'invalid': 'input'}, input_type=dict]
email
  Field required [type=missing, input_value={'invalid': 'input'}, input_type=dict]
age
  Field required [type=missing, input_value={'invalid': 'input'}, input_type=dict]
```

Two things to note:

- Plain dicts are automatically converted into annotated models: `process_user(user_data)` accepts the dict on line 25 because `UserData` can be built from it.
- Validation happens on the caller side, at task runtime. A malformed task never reaches the backend.

!!! note
    Arguments must be JSON-serializable as they are stored in the backend as JSON and sent to workers that way.

## Result validation

When the task returns, the worker validates the return value against the return annotation. Reading `task.result` gives back the annotated type: a model instance, not a raw dict:

```python
processed = await queue.wait_for(t)
assert isinstance(processed.result, ProcessResult)
```

## Accessing the current task

Use `CURRENT_TASK` as a parameter default to receive the running `Task` object inside the function:

```python
from sheppy import CURRENT_TASK, Task, task

@task
async def report_progress(url: str, current: Task = CURRENT_TASK) -> str:
    return f"attempt {current.retry_count + 1} for {url}"
```

The parameter is excluded from the stored arguments; the worker injects the current task at execution time.

## Dependency injection

For dependencies like database sessions or HTTP clients, use `Depends`, the same pattern as FastAPI:

```python
from sheppy import Depends, task

def get_db() -> str:
    return "PROD DATABASE"

@task
def my_task(db: str = Depends(get_db)) -> str:
    return db
```

Dependencies can be sync or async, can depend on other dependencies, and can be replaced per queue or worker with `dependency_overrides` (see [Testing tasks](testing.md#overriding-dependencies)).
