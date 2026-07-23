import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from sheppy import (
    CURRENT_TASK,
    MemoryBackend,
    Queue,
    SyncQueue,
    Task,
    TestQueue,
    Worker,
    task,
    workflow,
)


@task
async def async_add(x: int, y: int) -> int:
    return x + y


@task
def sync_add(x: int, y: int) -> int:
    return x + y


@task(retry=2, retry_delay=0.01)
async def fail_once(current: Task = CURRENT_TASK) -> str:
    if current.retry_count == 0:
        raise Exception("transient error")
    return "recovered"


@task(retry=2, retry_delay=0.01)
async def fail_always() -> str:
    raise Exception("permanent failure")


@task
async def rollback() -> str:
    return "rolled back"


@workflow
def sequential_workflow(x: int):
    a = yield async_add(x, 1)
    b = yield async_add(a.result, 10)
    return b.result


@workflow
def sync_task_workflow(x: int):
    a = yield sync_add(x, 1)
    return a.result


@workflow
def fan_out_workflow(values: list[int]):
    tasks = yield [async_add(v, 1) for v in values]
    final = yield async_add(sum(t.result for t in tasks), 0)
    return final.result


@workflow
def inner_workflow(x: int):
    t = yield async_add(x, 1)
    return t.result * 10


@workflow
def outer_workflow(x: int):
    r = yield inner_workflow(x)
    t = yield async_add(r, 100)
    return t.result


@workflow
def flaky_workflow():
    t = yield fail_once()
    if t.error:
        return f"error-path:{t.error}"
    return f"ok:{t.result}"


@workflow
def error_branch_workflow():
    t = yield fail_always()
    if t.error:
        r = yield rollback()
        return f"recovered: {r.result}"
    return "no error"


@workflow
def crashing_workflow():
    a = yield async_add(5, 5)
    raise RuntimeError(f"user code exploded after {a.result}")


@workflow
def invalid_yield_workflow():
    yield 42


@workflow
def empty_workflow():
    if False:  # would be funny to get a random bitflip here one day
        yield async_add(0, 0)
    return "immediate"


@task
async def unannotated_dict_task():
    return {"key": "original"}


@workflow
def mutating_workflow():
    t = yield unannotated_dict_task()
    t.result["key"] = "mutated"  # mutating the result must never corrupt stored state
    t2 = yield async_add(1, 1)
    return t2.result


async def test_add_workflow_stores_state_and_queues_tasks(queue: Queue):
    result = await queue.add_workflow(sequential_workflow(2))

    assert not result.workflow.completed
    assert result.workflow.error is None
    assert len(result.pending_tasks) == 1
    assert result.pending_tasks[0].spec.args == (2, 1)
    assert result.pending_tasks[0].workflow_id == result.workflow.id
    assert result.workflow.task_order == [str(result.pending_tasks[0].id)]

    assert await queue.size() == 1

    stored = await queue.get_workflow(result.workflow.id)
    assert stored is not None
    assert stored.task_order == result.workflow.task_order

    pending_workflows = await queue.get_pending_workflows()
    assert [w.id for w in pending_workflows] == [result.workflow.id]


async def test_sequential_workflow_completes(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    result = await queue.add_workflow(sequential_workflow(2))

    await worker.work(2)

    wf = await queue.get_workflow(result.workflow.id)
    assert wf is not None
    assert wf.completed
    assert wf.error is None
    assert wf.final_result == 13
    assert wf.finished_at is not None

    assert await queue.get_pending_workflows() == []


async def test_sync_task_in_workflow(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    await queue.add_workflow(sync_task_workflow(4))

    await worker.work(1)

    wf = (await queue.get_all_workflows())[0]
    assert wf.completed
    assert wf.final_result == 5


async def test_fan_out_fan_in(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    await queue.add_workflow(fan_out_workflow([1, 2, 3]))

    await worker.work(4)

    wf = (await queue.get_all_workflows())[0]
    assert wf.completed
    assert wf.final_result == 9  # (1+1) + (2+1) + (3+1)


async def test_nested_workflow(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    await queue.add_workflow(outer_workflow(5))

    await worker.work(2)

    wf = (await queue.get_all_workflows())[0]
    assert wf.completed
    assert wf.final_result == 160  # ((5 + 1) * 10) + 100


async def test_resume_after_partial_completion(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    result = await queue.add_workflow(sequential_workflow(2))

    # only the first step is processed
    await worker.work(1)

    wf = await queue.get_workflow(result.workflow.id)
    assert wf is not None
    assert not wf.completed
    assert await queue.size() == 1

    # manual resume while the second step is still queued is a harmless no-op
    resumed = await queue._resume_workflow(result.workflow.id)
    assert not resumed.workflow.completed
    assert await queue.size() == 1  # the queued step is not duplicated

    # finishing the remaining step completes the workflow
    await worker.work(1)

    wf = await queue.get_workflow(result.workflow.id)
    assert wf is not None
    assert wf.completed
    assert wf.final_result == 13


async def test_concurrent_resumes_enqueue_step_exactly_once(queue: Queue):
    result = await queue.add_workflow(sequential_workflow(2))

    # simulate a worker popping and completing the first step (without resuming the workflow afterwards)
    t1 = (await queue._pop_pending())[0]
    done = t1.model_copy(update={
        "status": "completed",
        "result": 3,
        "finished_at": datetime.now(timezone.utc),
    })
    await queue._store_result(done)
    assert await queue.size() == 0

    # two resumes racing to advance the workflow (e.g. two workers)
    await asyncio.gather(
        queue._resume_workflow(result.workflow.id),
        queue._resume_workflow(result.workflow.id),
    )

    # deterministic step IDs make the racing adds deduplicate
    assert await queue.size() == 1

    wf = await queue.get_workflow(result.workflow.id)
    assert wf is not None
    assert not wf.completed
    assert len(wf.task_order) == 2


async def test_failing_task_recovers_via_retry(queue: Queue, worker: Worker):
    """Regression: a task scheduled for retry must NOT resume its workflow prematurely."""
    worker.enable_cron_manager = False
    await queue.add_workflow(flaky_workflow())

    await worker.work(2)  # failed attempt + successful retry

    wf = (await queue.get_all_workflows())[0]
    assert wf.completed
    assert wf.error is None
    assert wf.final_result == "ok:recovered"


async def test_terminal_failure_takes_error_branch(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    await queue.add_workflow(error_branch_workflow())

    await worker.work(3)  # two failed attempts + rollback task

    wf = (await queue.get_all_workflows())[0]
    assert wf.completed
    assert wf.error is None
    assert wf.final_result == "recovered: rolled back"


async def test_workflow_function_raise_fails_workflow(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    result = await queue.add_workflow(crashing_workflow())

    await worker.work(1)

    wf = await queue.get_workflow(result.workflow.id)
    assert wf is not None
    assert not wf.completed
    assert wf.error == "RuntimeError: user code exploded after 10"
    assert wf.finished_at is not None

    # progress made before the error is preserved in the task store
    assert len(wf.task_order) == 1
    finished_task = await queue.get_task(wf.task_order[0])
    assert finished_task is not None
    assert finished_task.status == "completed"
    assert finished_task.result == 10

    assert await queue.get_pending_workflows() == []


async def test_invalid_yield_fails_workflow_immediately(queue: Queue):
    result = await queue.add_workflow(invalid_yield_workflow())

    assert result.pending_tasks == []
    assert result.workflow.error is not None
    assert "Invalid yield" in result.workflow.error

    stored = await queue.get_workflow(result.workflow.id)
    assert stored is not None
    assert stored.error == result.workflow.error


async def test_empty_workflow_completes_immediately(queue: Queue):
    result = await queue.add_workflow(empty_workflow())

    assert result.pending_tasks == []
    assert result.workflow.completed
    assert result.workflow.final_result == "immediate"
    assert await queue.size() == 0


async def test_resume_finished_workflow_is_noop(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    result = await queue.add_workflow(sequential_workflow(2))
    await worker.work(2)

    resumed = await queue._resume_workflow(result.workflow.id)
    assert resumed.workflow.completed
    assert resumed.workflow.final_result == 13
    assert resumed.pending_tasks == []
    assert await queue.size() == 0


async def test_resume_unknown_workflow_raises(queue: Queue):
    with pytest.raises(ValueError, match="Workflow not found"):
        await queue._resume_workflow(uuid4())


async def test_workflow_mutations_cannot_corrupt_stored_task_state(queue: Queue, worker: Worker):
    """Regression: mutating a task result inside a workflow must not corrupt the
    stored task (only happened for memory backend but better be safe than sorry)."""
    worker.enable_cron_manager = False
    await queue.add_workflow(mutating_workflow())

    await worker.work(2)

    wf = (await queue.get_all_workflows())[0]
    assert wf.completed
    assert wf.final_result == 2

    #  stored result of the first task is still untouched
    t1 = await queue.get_task(wf.task_order[0])
    assert t1 is not None
    assert t1.result == {"key": "original"}


async def test_get_workflow_batch(queue: Queue):
    r1 = await queue.add_workflow(sequential_workflow(1))
    r2 = await queue.add_workflow(sequential_workflow(2))

    results = await queue.get_workflow([r1.workflow.id, r2.workflow.id])

    assert set(results) == {r1.workflow.id, r2.workflow.id}


async def test_delete_workflow(queue: Queue):
    result = await queue.add_workflow(sequential_workflow(2))

    assert await queue.delete_workflow(result.workflow.id) is True
    assert await queue.get_workflow(result.workflow.id) is None
    assert await queue.delete_workflow(result.workflow.id) is False


async def test_wait_for_workflow(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    result = await queue.add_workflow(sequential_workflow(2))

    worker_task = asyncio.create_task(worker.work(2))
    wf = await queue.wait_for_workflow(result.workflow.id)
    await worker_task

    assert wf is not None
    assert wf.completed
    assert wf.error is None
    assert wf.final_result == 13


async def test_wait_for_workflow_already_finished(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    result = await queue.add_workflow(sequential_workflow(2))
    await worker.work(2)

    # returns immediately, no timeout error
    wf = await queue.wait_for_workflow(result.workflow.id, timeout=0.01)

    assert wf is not None
    assert wf.completed


async def test_wait_for_workflow_returns_failed_workflow(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    result = await queue.add_workflow(crashing_workflow())

    worker_task = asyncio.create_task(worker.work(1))
    wf = await queue.wait_for_workflow(result.workflow.id)
    await worker_task

    assert wf is not None
    assert not wf.completed
    assert wf.error == "RuntimeError: user code exploded after 10"


async def test_wait_for_workflow_timeout(queue: Queue):
    result = await queue.add_workflow(sequential_workflow(2))  # no worker to drive it

    with pytest.raises(TimeoutError):
        await queue.wait_for_workflow(result.workflow.id, timeout=0.05)


async def test_wait_for_workflow_batch(queue: Queue, worker: Worker):
    worker.enable_cron_manager = False
    r1 = await queue.add_workflow(sequential_workflow(1))
    r2 = await queue.add_workflow(sequential_workflow(2))

    worker_task = asyncio.create_task(worker.work(4))
    results = await queue.wait_for_workflow([r1.workflow.id, r2.workflow.id], timeout=5)
    await worker_task

    assert set(results) == {r1.workflow.id, r2.workflow.id}
    assert results[r1.workflow.id].final_result == 12
    assert results[r2.workflow.id].final_result == 13


async def test_memory_backend_instant_processing_completes_workflow():
    """Regression: default MemoryBackend (instant_processing=True) must drive workflows to completion."""
    queue = Queue(MemoryBackend(), "workflow-instant")
    result = await queue.add_workflow(sequential_workflow(2))

    # the workflow is driven to completion inline while adding;
    # re-fetch to see the final state
    wf = await queue.get_workflow(result.workflow.id)
    assert wf is not None
    assert wf.completed
    assert wf.final_result == 13


def test_testqueue_process_workflow():
    q = TestQueue(name="workflow-testqueue")
    result = q.process_workflow(sequential_workflow(2))

    assert result.workflow.completed
    assert result.workflow.final_result == 13


def test_testqueue_process_workflow_with_retry():
    q = TestQueue(name="workflow-testqueue-retry")
    result = q.process_workflow(flaky_workflow())

    assert result.workflow.completed
    assert result.workflow.final_result == "ok:recovered"


def test_testqueue_process_workflow_error_branch():
    q = TestQueue(name="workflow-testqueue-error")
    result = q.process_workflow(error_branch_workflow())

    assert result.workflow.completed
    assert result.workflow.final_result == "recovered: rolled back"


def test_syncqueue_add_workflow():
    q = SyncQueue("memory://", name="workflow-syncqueue")
    try:
        result = q.add_workflow(sequential_workflow(2))

        stored = q.get_workflow(result.workflow.id)
        assert stored is not None
        assert stored.completed
        assert stored.final_result == 13
    finally:
        q.close()


def test_syncqueue_wait_for_workflow():
    q = SyncQueue("memory://", name="workflow-syncqueue-wait")
    try:
        result = q.add_workflow(sequential_workflow(2))

        wf = q.wait_for_workflow(result.workflow.id)
        assert wf is not None
        assert wf.completed
        assert wf.final_result == 13
    finally:
        q.close()
