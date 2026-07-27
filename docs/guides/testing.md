# Testing Tasks

`TestQueue` runs tasks synchronously in the test process without a separate worker or redis. It is designed for simple testing tasks in unit tests by providing deterministic synchronous execution.

## Basic testing

Here is a simple task to test:

```python title="tasks.py"
--8<-- "examples/testing/basic-example/tasks.py"
```

And its test:

```python title="tests/test_tasks.py"
--8<-- "examples/testing/basic-example/tests/test_tasks.py"
```

Differences from "full" `Queue`:

- Synchronous API, so no `await` and event loops
- Tasks only run when you call `process_next()` or `process_all()`
- Everything happens in the test process and stored in-memory

## Testing task failures

When a task raises, the exception is captured in the `exception` attribute instead of propagating, so you can assert on it:

```python title="tests/test_failure.py"
--8<-- "examples/testing/test_failure.py"
```

## Testing retry logic

Retries in `TestQueue` happen immediately, with no delay, so retry behavior is fast to test:

```python title="tests/test_retry_logic.py"
--8<-- "examples/testing/test_retry_logic.py"
```

Note the first processed attempt has status `retrying`, not `failed`. A task is only `failed` once no attempts remain. Both attempts share the same task id.

## Overriding dependencies

Tasks that use `Depends` can swap their dependencies in tests with `dependency_overrides`:

```python title="example_dependency.py"
--8<-- "examples/example_dependency.py"
```

The task resolves `get_db` through the override map and receives `"TESTING DATABASE"`. The same parameter exists on `Worker` and `MemoryBackend` for integration tests.

`examples/example_timeout.py` shows the same style of test for `timeout` and `retry_on_timeout`.
