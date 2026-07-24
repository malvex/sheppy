from collections.abc import Callable

import pytest

from sheppy import Queue, Task, Worker
from tests.dependencies import (
    CustomTaskError,
    assert_is_completed,
    assert_is_failed,
    async_failing_task_custom_error,
    failing_task,
    failing_task_custom_error,
    simple_sync_task,
)


async def test_exception_captured_on_failure(queue: Queue, worker: Worker):
    t = failing_task("boom")
    assert await queue.add(t) is True
    await worker.work(max_tasks=1)

    t = await queue.get_task(t)

    assert_is_failed(t)
    assert t.exception is not None
    assert t.exception.type == "ValueError"
    assert t.exception.module == "builtins"
    assert t.exception.message == "boom"
    assert t.exception.args == ("boom",)
    assert t.exception.traceback is not None
    assert "ValueError: boom" in t.exception.traceback
    assert "failing_task" in t.exception.traceback


@pytest.mark.parametrize("failing_fn", [failing_task_custom_error, async_failing_task_custom_error])
async def test_custom_exception_roundtrip_through_backend(queue: Queue, worker: Worker, failing_fn):
    t = failing_fn(5, detail="bad things")
    assert await queue.add(t) is True
    await worker.work(max_tasks=1)

    t = await queue.get_task(t)

    assert_is_failed(t)
    assert t.exception is not None
    assert t.exception.type == "CustomTaskError"
    assert t.exception.module == "tests.dependencies"
    assert t.exception.message == str(CustomTaskError("bad things", 5))
    assert t.exception.args == ("bad things", 5)
    assert t.exception.traceback is not None

    # JSON round trip
    t2 = Task.model_validate_json(t.model_dump_json())
    assert t2.exception == t.exception

    # exception is reconstructable after the round trip
    exc = t.exception.to_exception()
    assert isinstance(exc, CustomTaskError)
    assert exc.args == ("bad things", 5)


async def test_successful_task_has_no_exception(queue: Queue, worker: Worker):
    t = simple_sync_task(1, 2)
    assert await queue.add(t) is True
    await worker.work(max_tasks=1)

    t = await queue.get_task(t)

    assert_is_completed(t)
    assert t.exception is None


async def test_retrying_task_keeps_exception_until_success(task_fail_once_fn: Callable[[], Task], queue: Queue, worker: Worker):
    t = task_fail_once_fn()
    await queue.add(t)

    await worker.work(1)

    t = await queue.get_task(t)
    assert_is_failed(t, status='retrying')
    assert t.exception is not None
    assert str(t.exception) == "Exception: transient error"

    await worker.work(1)

    t = await queue.get_task(t)
    assert_is_completed(t)
    assert t.exception is None
