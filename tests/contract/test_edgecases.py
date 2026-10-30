import asyncio

from sheppy import Queue, Worker, task
from tests.dependencies import assert_is_completed, assert_is_failed


@task
async def task_with_wrong_return_annotation() -> list:
    return 5


@task
def sum_variadic(*values: int) -> int:
    return sum(values)


@task(timeout=0.01)
async def exceed_task_timeout() -> None:
    await asyncio.sleep(1)


@task(timeout=10, retry=1, retry_delay=0, retry_on_timeout=False)
async def raise_user_timeout_error() -> None:
    raise asyncio.TimeoutError("raised by user code")


async def test_wrong_return_annotation(queue: Queue, worker: Worker) -> None:
    t = task_with_wrong_return_annotation()

    await queue.add(t)
    await worker.work(1)

    t = await queue.get_task(t)

    assert_is_failed(t)


async def test_variadic_arguments_are_preserved(queue: Queue, worker: Worker) -> None:
    t = sum_variadic(1, 2, 3)

    await queue.add(t)
    await worker.work(1)

    t = await queue.get_task(t)

    assert_is_completed(t)
    assert t.result == 6


async def test_actual_timeout_is_task_timeout(queue: Queue, worker: Worker) -> None:
    t = exceed_task_timeout()

    await queue.add(t)
    await worker.work(1)

    t = await queue.get_task(t)

    assert_is_failed(t)
    assert t.exception is not None
    assert t.exception.type == "TaskTimeoutError"
    assert t.exception.message == "Task exceeded timeout of 0.01 seconds"


async def test_user_timeout_error_uses_normal_failure_handling(queue: Queue, worker: Worker) -> None:
    t = raise_user_timeout_error()

    await queue.add(t)
    await worker.work(1)

    t = await queue.get_task(t)

    assert t.exception is not None
    assert t.exception.type == "TimeoutError"
    assert t.exception.message == "raised by user code"
    assert t.status == "retrying"
    assert t.result is None
    assert t.finished_at is None
    assert t.retry_count == 1
