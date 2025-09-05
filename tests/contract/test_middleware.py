from sheppy import Queue, Worker
from tests.dependencies import (
    WrappedNumber,
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
    task_add_with_middleware_change_return_type,
    task_add_with_middleware_change_return_value,
    task_add_with_middleware_multiple,
    task_add_with_middleware_noop,
)


class TestMiddleware:

    async def test_persists_on_task(self, queue: Queue, worker: Worker):
        t1 = simple_async_task(1, 2)
        t2 = task_add_with_middleware_noop(1, 2)

        assert len(t1.internal.middleware) == 0
        assert len(t2.internal.middleware) == 1

        await queue.add([t1, t2])
        await worker.work(max_tasks=2)

        t1 = await queue.refresh(t1)
        t2 = await queue.refresh(t2)

        assert len(t1.internal.middleware) == 0
        assert len(t2.internal.middleware) == 1

        assert t1.result == 3
        assert t2.result == 3


    async def test_noop(self, queue: Queue, worker: Worker):

        task = task_add_with_middleware_noop(1, 2)

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.refresh(task)

        assert task.result == 3

    async def test_change_arg(self, queue: Queue, worker: Worker):

        task = task_add_with_middleware_change_arg(1, 2)

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.refresh(task)

        assert task.result == 7

    async def test_change_return_value(self, queue: Queue, worker: Worker):

        task = task_add_with_middleware_change_return_value(1, 2)

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.refresh(task)

        assert task.result == 100003

    async def test_multiple(self, queue: Queue, worker: Worker):

        task = task_add_with_middleware_multiple(1, 2)

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.refresh(task)

        assert task.result == 100007

    async def test_change_return_type(self, queue: Queue, worker: Worker):

        task = task_add_with_middleware_change_return_type(1, 2)

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.refresh(task)

        assert task.result == WrappedNumber(result=100003, extra="hi from middleware")
