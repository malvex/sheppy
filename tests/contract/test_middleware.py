import pytest

from sheppy import Queue, Worker, task
from tests.dependencies import (
    # middleware_change_arg,
    # middleware_change_return_type,
    # middleware_change_return_value,
    # middleware_no_args,
    # middleware_noop,
    # #async_middleware_noop,
    # middleware_noop_no_val,
    # middleware_noop_pass,
    # middleware_noop_return_no_val,
    # middleware_noop_return_only_no_val,
    # middleware_noop_yield_no_val,
    # middleware_noop_yield_only_no_val,
    # middleware_too_many_args,
    simple_async_task,
    task_add_with_middleware_change_arg,
    task_add_with_middleware_change_return_value,
    task_add_with_middleware_multiple,
    task_add_with_middleware_noop,
)


def mw_preprocess_fail(_task, _queue):
    1/0  # noqa:B018
    yield


def mw_postprocess_fail(_task, _queue):
    yield
    1/0  # noqa:B018


@task(middleware=[mw_preprocess_fail])
def task_fail_preprocess() -> int:
    return 5


@task(middleware=[mw_postprocess_fail])
def task_fail_postprocess() -> int:
    return 5


class TestMiddleware:

    async def test_noop(self, queue: Queue, worker: Worker):

        task = task_add_with_middleware_noop(1, 2)

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.get_task(task)

        assert task.result == 3

    async def test_change_arg(self, queue: Queue, worker: Worker):

        task = task_add_with_middleware_change_arg(1, 2)

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.get_task(task)

        assert task.result == 7

    async def test_change_return_value(self, queue: Queue, worker: Worker):

        task = task_add_with_middleware_change_return_value(1, 2)

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.get_task(task)

        assert task.result == 105003

    async def test_multiple(self, queue: Queue, worker: Worker):

        task = task_add_with_middleware_multiple(1, 2)

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.get_task(task)

        assert task.result == 105007

    async def test_backwards_compat(self, queue: Queue, worker: Worker):
        task = simple_async_task(1, 2)
        data = task.model_dump(mode="json")
        data['spec']['middleware'] = "tests.dependencies:middleware_noop"

        task = task.model_validate(data)

        assert not hasattr(task.spec, "middleware")

        # middleware should be silently dropped for backwards compatibility,
        # but everything else should fail so we still forbid extra fields
        data['spec']['something-else'] = "anything"

        with pytest.raises(match="Extra inputs are not permitted"):
            task.model_validate(data)

    async def test_failing_middleware_preprocess(self, queue: Queue, worker: Worker):
        task = task_fail_preprocess()

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.get_task(task)

        assert task.status == "failed"
        assert task.result is None

    async def test_failing_middleware_postprocess(self, queue: Queue, worker: Worker):
        task = task_fail_postprocess()

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.get_task(task)

        assert task.status == "completed"
        assert task.result == 5
