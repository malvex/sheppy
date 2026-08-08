# Workflows

!!! danger "Experimental feature"

    This feature is experimental and should be considered unstable and unsupported.
    API may change at any time and without backwards compatibility.

A workflow chains tasks into a multi-step process where each step can use the results of the previous ones. Write it as a generator function: yield tasks, get their finished versions back.

## Defining a workflow

```python
from sheppy import task
from sheppy.experimental import workflow

@task
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"

@workflow
def greetings_workflow(names: list[str]):
    t1 = yield say_hello("Alice")
    t2 = yield say_hello("Bob")
    tx = yield [say_hello(name) for name in names]  # fan-out: steps run in parallel

    return "\n".join([t1.result, t2.result] + [t.result for t in tx])
```

- Yield a single `Task` for a sequential step, or a `list[Task]` to run steps in parallel.
- The value of the yield expression is the *finished* `Task`: read `task.result` for the return value and check `task.exception` for failures.
- The generator's return value becomes the workflow's `final_result`.
- Calling the decorated function returns a `Workflow` instance; nothing runs yet.

The generator must be a plain (non-async) generator function, and it must be deterministic. On each resume it is replayed from the start with finished tasks fed back in, so the sequence of yields may only depend on previous task results, never on external state or randomness. `Depends` and `CURRENT_TASK` are not allowed in workflow functions.

## Running a workflow

`add_workflow()` starts the run and `wait_for_workflow()` blocks until it completes or fails:

```python
result = await queue.add_workflow(greetings_workflow(["Alex", "John"]))
wf = await queue.wait_for_workflow(result.workflow.id)
print(wf.final_result)
```

`examples/workflows/simple.py` is self-contained (in-memory backend plus an in-process worker):

```python title="examples/workflows/simple.py"
--8<-- "examples/workflows/simple.py"
```

```bash
$ python examples/workflows/simple.py
Hello, Alice!
Hello, Bob!
Hello, Alex!
Hello, John!
```

## Handling failures

A failed task does not fail the workflow by itself. Check `task.exception` on the finished task and take action (roll-back, notify, retry), or raise to fail the whole workflow, which sets `wf.exception`. `examples/workflows/fan_in_fan_out.py` shows the pattern:

```python title="examples/workflows/fan_in_fan_out.py"
--8<-- "examples/workflows/fan_in_fan_out.py"
```

Running it makes `cleanup_old_data` raise; the workflow rolls back, fans out three notifications, and fails as designed:

```bash
$ python examples/workflows/fan_in_fan_out.py
<Worker> Task 0adf46fa-... failed: some random failure happened
<Worker> Workflow b1e4879b-... failed: Exception: Cleanup failed, notifications were sent
Sending email to admin1@example.com, subject Oh no, daily cleanup failed!
Sending email to admin2@example.com, subject Oh no, daily cleanup failed!
Sending email to admin3@example.com, subject Oh no, daily cleanup failed!
Workflow failed as expected: Exception: Cleanup failed, notifications were sent
```

## Notes and limits

- Workflow state is persisted in the backend, so workflows survive restarts, but the workflow function must stay importable at its original `module:function` path.
- Nested workflows: yield a `Workflow` from another `@workflow` function and it runs inline within the parent. The yield expression receives the nested generator's return value.
- In tests, `TestQueue.process_workflow()` drives a workflow synchronously to completion.
