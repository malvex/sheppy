# Testing Tasks

Tests are a crucial part of any software development process, ensuring that your application behaves as expected. Sheppy provides first class support for testing tasks and workflows, making it easy to write reliable tests.

This guide covers various strategies and best practices for testing your Sheppy tasks and workflows effectively.

## Getting Started with Testing

Let's say you have a simple task defined as follows:

```python title="tasks.py"
--8<-- "examples/testing/basic-example/tasks.py"
```

You can write tests for this task using `pytest` and Sheppy's `TestQueue`.

```python title="tests/test_tasks.py"
--8<-- "examples/testing/basic-example/tests/test_tasks.py"
```

Tasks are executed synchronously in the test, allowing you to easily assert their outcomes.

### Testing Task Failures

In this example, we have a task `divide` that may raise an exception when dividing by zero. We can write tests to ensure that the task behaves correctly in both success and failure scenarios.

```python title="tests/test_failure.py"
--8<-- "examples/testing/test_failure.py"
```

### Testing Retry Logic

```python title="tests/test_retry_logic.py"
--8<-- "examples/testing/test_retry_logic.py"
```

## Assert helper functions

Sheppy provides several helper functions to assert task states in your tests:

* `assert_is_new(task)`: Asserts the task is instance of Task and is in 'new' state.
* `assert_is_completed(task)`: Asserts the task is instance of Task, has been processed, completed successfully, and has a result.
* `assert_is_failed(task)`: Asserts the task is instance of Task, has been processed, failed, and has an error.

#### Usage

```python title="tests/test_assert_helper_functions.py"
---8<-- "examples/testing/test_assert_helper_functions.py"
```

## Next Steps

You may be interested in exploring the following topics next:

- [Async Task Testing](advanced/async-testing.md) - Learn how to test asynchronous tasks.
- [FastAPI Integration](advanced/fastapi-integration.md) - Learn how to use Sheppy with FastAPI.
- [Examples](../examples/index.md) - Explore real-world usage patterns and examples of Sheppy in action.
