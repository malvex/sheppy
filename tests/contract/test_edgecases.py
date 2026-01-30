from sheppy import Queue, Worker, task
from tests.dependencies import assert_is_failed


@task
async def task_with_wrong_return_annotation() -> list:
    return 5


async def test_wrong_return_annotation(queue: Queue, worker: Worker) -> None:
    t = task_with_wrong_return_annotation()

    await queue.add(t)
    await worker.work(1)

    t = await queue.get_task(t)

    assert_is_failed(t)
