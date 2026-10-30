import asyncio
import contextlib

from asyncer import syncify

from sheppy import Queue, Worker, task

_queue: Queue | None = None


@task
async def asyncer_child(x: int) -> int:
    return x * 2


@task
def asyncer_parent(x: int) -> int:
    queue = _queue
    assert queue is not None

    async def inner() -> int:
        created = asyncer_child(x)
        await queue.add(created)
        processed = await queue.wait_for(created, timeout=2)
        assert processed.result is not None
        return processed.result

    return syncify(inner)()


async def test_syncify_nested_queue_access(queue: Queue, worker: Worker) -> None:
    global _queue
    _queue = queue
    worker.enable_cron_manager = False
    created = asyncer_parent(4)
    await queue.add(created)
    worker_task = asyncio.create_task(worker.work(max_tasks=2, register_signal_handlers=False))
    try:
        processed = await queue.wait_for(created, timeout=3)
        assert processed.status == "completed"
        assert processed.result == 8
        await asyncio.wait_for(worker_task, timeout=1)
    finally:
        if not worker_task.done():
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
        _queue = None
