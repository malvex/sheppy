from collections.abc import Callable

from sheppy import Queue, Task, Worker
from sheppy.testqueue import assert_is_completed, assert_is_new


async def test_task_chaining(task_chaining_fn: Callable[[], Task], queue: Queue, worker: Worker) -> None:
    t1 = task_chaining_fn(3, asynchronous=True)  # type: Task
    t2 = task_chaining_fn(4, asynchronous=False)  # type: Task

    await queue.add([t1, t2])

    assert await queue.size() == 2

    await worker.work(2)

    assert await queue.size() == 2

    t1 = await queue.get_task(t1)
    t2 = await queue.get_task(t2)

    assert_is_completed(t1)
    assert_is_completed(t2)

    t1_subtask = t1.result
    t2_subtask = t2.result

    assert_is_new(t1_subtask)
    assert_is_new(t2_subtask)
    assert isinstance(t1_subtask, Task)
    assert isinstance(t2_subtask, Task)

    await worker.work(2)

    assert await queue.size() == 0

    t1_subtask = await queue.get_task(t1_subtask)
    t2_subtask = await queue.get_task(t2_subtask)

    assert_is_completed(t1_subtask)
    assert_is_completed(t2_subtask)

    assert t1_subtask.result == 9
    assert t2_subtask.result == 12


async def test_task_chaining_bulk(task_chaining_bulk_fn: Callable[[], Task], queue: Queue, worker: Worker) -> None:
    t1 = task_chaining_bulk_fn(3, asynchronous=True)  # type: Task
    t2 = task_chaining_bulk_fn(4, asynchronous=False)  # type: Task

    await queue.add([t1, t2])

    assert await queue.size() == 2

    await worker.work(2)

    # bulk task chaining - each task should have created 2 tasks, so 2 x 2 = 4
    assert await queue.size() == 4

    t1 = await queue.get_task(t1)
    t2 = await queue.get_task(t2)

    assert_is_completed(t1)
    assert_is_completed(t2)

    # result should be list of tasks
    assert isinstance(t1.result, list)
    assert isinstance(t2.result, list)

    created_subtasks = t1.result + t2.result
    assert len(created_subtasks) == 4

    for subtask in created_subtasks:
        assert isinstance(subtask, Task)
        assert_is_new(subtask)
        # task should not be processed yet
        assert_is_new(await queue.get_task(subtask))

    await worker.work(4)

    # queue should be empty now
    assert await queue.size() == 0

    # convert dict to list so we can verify result in the correct order
    processed_dict = await queue.get_task(created_subtasks)
    processed = [processed_dict[t.id] for t in created_subtasks]

    # subtasks should be all completed successfully
    for subtask in processed:
        assert_is_completed(subtask)

    assert processed[0].result == 9
    assert processed[1].result == 8
    assert processed[2].result == 12
    assert processed[3].result == 10
