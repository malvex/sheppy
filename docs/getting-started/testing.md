# Testing Tasks

Sheppy is built with testing in mind. The `TestQueue` provides a synchronous, deterministic API that makes testing tasks easy and straightforward.

This guide covers testing strategies from basic unit tests to complex retry logic.

## Basic Testing

Here's a simple task to test:

```python title="tasks.py"
--8<-- "examples/testing/basic-example/tasks.py"
```

Testing it is straightforward with `TestQueue`:

```python title="tests/test_tasks.py"
--8<-- "examples/testing/basic-example/tests/test_tasks.py"
```

**Key differences from production `Queue`:**

- Synchronous API (no `await` needed)
- Explicit task processing with `process_next()` or `process_all()`
- Tasks execute immediately in the test process without needing background workers
- Perfect for fast, deterministic unit tests

## Testing Task Failures

Tasks can fail, and you should test that they fail correctly:

```python title="tests/test_failure.py"
--8<-- "examples/testing/test_failure.py"
```

When a task fails, the exception is captured in the `error` attribute. You can assert on this to verify correct error handling.

## Testing Retry Logic

Test that retry configuration works as expected:

```python title="tests/test_retry_logic.py"
--8<-- "examples/testing/test_retry_logic.py"
```

When using `TaskQueue`, retries happen immediately with no delay, keeping tests fast while still validating retry behavior.

## Assertion Helpers

Sheppy provides assertion helpers for cleaner test code:

- `assert_is_new(task)` - Task is new (not yet processed)
- `assert_is_completed(task)` - Task completed successfully with a result
- `assert_is_failed(task)` - Task failed with an error

These raise clear assertion errors if the task isn't in the expected state:

```python title="tests/test_assert_helper_functions.py"
--8<-- "examples/testing/test_assert_helper_functions.py"
```
