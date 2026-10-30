from sheppy import Queue, Worker, task
from tests.dependencies import assert_is_completed, assert_is_failed


@task
async def task_with_wrong_return_annotation() -> list:
    return 5


@task
def sum_variadic(*values: int) -> int:
    return sum(values)


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
