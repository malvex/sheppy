import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from sheppy import Queue, Worker
from sheppy.testqueue import assert_is_completed, assert_is_failed, assert_is_new
from tests.dependencies import (
    TaskTestCase,
    TaskTestCases,
    simple_async_task,
    simple_sync_task,
)


class TestSuccessfulTasks:
    @pytest.mark.parametrize("test_case", TaskTestCases.all_successful_tasks(), ids=lambda tc: tc.name)
    async def test_task_execution(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        task = test_case.create_task()

        assert await queue.add(task) == [True]

        # verify initial state
        assert_is_new(task)

        # process the task
        await worker.work(max_tasks=1)

        # no change on the original task
        assert_is_new(task)

        # refresh and verify completion
        processed = await queue.get_task(task)
        assert_is_completed(processed)
        assert processed.result == test_case.expected_result


    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks(), ids=lambda tc: tc.name)
    async def test_wait_for(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        task = test_case.create_task()

        assert await queue.add(task) == [True]

        # process in background
        process_task = asyncio.create_task(worker.work(max_tasks=1))

        # wait for result
        task = await queue.wait_for(task, timeout=3.0)
        assert_is_completed(task)
        assert task.result == test_case.expected_result

        await asyncio.sleep(0.01)
        await process_task

    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks()[1:2], ids=lambda tc: tc.name)
    async def test_wait_for_timeout(self, test_case: TaskTestCase, queue: Queue):

        task = test_case.create_task()
        assert await queue.add(task) == [True]

        # we aren't processing the task => should raise exception
        with pytest.raises(TimeoutError):
            await queue.wait_for(task, timeout=0.2)


class TestFailingTasks:
    @pytest.mark.parametrize("test_case", TaskTestCases.failing_tasks(), ids=lambda tc: tc.name)
    async def test_task_failure(self, test_case: TaskTestCase, queue: Queue, worker: Worker):

        task = test_case.create_task()
        assert await queue.add(task) == [True]
        assert_is_new(task)

        # process the task
        await worker.work(max_tasks=1)

        # no change on the original task
        assert_is_new(task)

        # refresh and verify failure
        task = await queue.get_task(task)
        assert_is_failed(task)
        assert task.error == test_case.expected_error
        assert task.finished_at is not None


class TestTaskSelfReference:
    @pytest.mark.parametrize("test_case", TaskTestCases.self_referencing_tasks(), ids=lambda tc: tc.name)
    async def test_self_reference(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        task = test_case.create_task()
        task_id = task.id

        assert await queue.add(task) == [True]
        await worker.work(max_tasks=1)

        task = await queue.get_task(task)

        assert_is_completed(task)
        assert task.result["result"] == test_case.expected_result
        assert task.result["task_id"] == str(task_id)


class TestScheduledTasks:
    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks(), ids=lambda tc: tc.name)
    async def test_schedule_with_timedelta(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        task = test_case.create_task()
        delay = timedelta(seconds=0.1)

        assert await queue.schedule(task, at=delay) is True

        popped = await queue.pop_pending(timeout=0.01)
        assert not popped

        task = await queue.get_task(task)
        assert_is_new(task)

        await worker.work(max_tasks=1)

        task = await queue.get_task(task)
        assert_is_completed(task)
        assert task.result == test_case.expected_result

    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks()[:1], ids=lambda tc: tc.name)
    async def test_schedule_with_datetime(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        task = test_case.create_task()
        scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=0.1)

        assert await queue.schedule(task, at=scheduled_time) is True

        popped = await queue.pop_pending(timeout=0.01)
        assert not popped

        await asyncio.sleep(0.15)

        await worker.work(max_tasks=1)

        task = await queue.get_task(task)
        assert_is_completed(task)
        assert task.result == test_case.expected_result

    async def test_multiple_scheduled_tasks(self, queue: Queue, worker: Worker):
        task1 = simple_sync_task(1, 2)
        task2 = simple_sync_task(3, 4)
        task3 = simple_sync_task(5, 6)

        assert await queue.schedule(task1, at=timedelta(seconds=0.2)) is True
        assert await queue.schedule(task2, at=timedelta(seconds=0.4)) is True
        assert await queue.schedule(task3, at=timedelta(seconds=0.6)) is True

        popped = await queue.pop_pending(timeout=0.01)
        assert not popped

        await worker.work(max_tasks=1)
        task1 = await queue.get_task(task1)
        assert_is_completed(task1)
        assert task1.result == 3

        task2 = await queue.get_task(task2)
        task3 = await queue.get_task(task3)
        assert_is_new(task2)
        assert_is_new(task3)

        await worker.work(max_tasks=1)
        task2 = await queue.get_task(task2)
        assert_is_completed(task2)
        assert task2.result == 7

        task3 = await queue.get_task(task3)
        assert_is_new(task3)

        await worker.work(max_tasks=1)
        task3 = await queue.get_task(task3)
        assert_is_completed(task3)
        assert task3.result == 11

    async def test_scheduled_task_with_immediate_task(self, queue: Queue, worker: Worker):
        scheduled_task = simple_sync_task(1, 2)
        assert await queue.schedule(scheduled_task, at=timedelta(seconds=0.2)) is True

        immediate_task = simple_sync_task(3, 4)
        assert await queue.add(immediate_task) == [True]

        await worker.work(max_tasks=1)

        immediate_task = await queue.get_task(immediate_task)
        scheduled_task = await queue.get_task(scheduled_task)

        assert_is_completed(immediate_task)
        assert immediate_task.result == 7

        assert_is_new(scheduled_task)

        await worker.work(max_tasks=1)

        scheduled_task = await queue.get_task(scheduled_task)
        assert_is_completed(scheduled_task)
        assert scheduled_task.result == 3

    @pytest.mark.parametrize("test_case", TaskTestCases.failing_tasks()[:1], ids=lambda tc: tc.name)
    async def test_scheduled_failing_task(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        task = test_case.create_task()

        assert await queue.schedule(task, at=timedelta(seconds=0.1)) is True

        await worker.work(max_tasks=1)

        task = await queue.get_task(task)
        assert_is_failed(task)
        assert task.error == test_case.expected_error

    async def test_schedule_in_past(self, queue: Queue, worker: Worker):
        task = simple_sync_task(1, 2)
        past_time = datetime.now(timezone.utc) - timedelta(seconds=10)

        assert await queue.schedule(task, at=past_time) is True

        await worker.work(max_tasks=1)

        task = await queue.get_task(task)
        assert_is_completed(task)
        assert task.result == 3


class TestBatchOperations:
    async def test_batch_add_to_queue(self, queue: Queue, worker: Worker):
        t1 = simple_async_task(1, 2)
        t2 = simple_async_task(5, 20)

        assert await queue.add([t1, t2]) == [True, True]

        assert await queue.size() == 2

        await worker.work(max_tasks=1)

        assert await queue.size() == 1

        t1 = await queue.get_task(t1)
        assert_is_completed(t1)
        assert t1.result == 3

        await worker.work(max_tasks=1)

        assert await queue.size() == 0

        t2 = await queue.get_task(t2)
        assert_is_completed(t2)
        assert t2.result == 25

class TestCronOperations:
    async def test_add_cron(self, queue: Queue):

        crons = [
            (simple_async_task(1, 2), '*/5 * * * *'),
            (simple_async_task(1, 2), '0 * * * *'),  # different cron schedule
            (simple_async_task(2, 2), '*/5 * * * *'),  # different input
            (simple_sync_task(1, 2), '*/5 * * * *'),  # different function
        ]

        for task, schedule in crons:
            for i in range(5):
                # Only first run should be successful, because cron doesn't exist yet.
                # Exactly same cron definitions (same task, same input, same cron schedule)
                # should not be added (would be duplicated crons)
                should_succeed = True if i == 0 else False  # noqa: SIM210

                success = await queue.add_cron(task, schedule)
                assert success is should_succeed

        all_crons = await queue.get_crons()
        assert len(all_crons) == 4

        for task, schedule in crons:
            for i in range(5):
                # same for deleting them - only first should be successful
                should_succeed = True if i == 0 else False  # noqa: SIM210

                success = await queue.delete_cron(task, schedule)
                assert success is should_succeed

        all_crons = await queue.get_crons()
        assert len(all_crons) == 0

    async def test_delete_nonexistent_cron(self, queue: Queue):
        success = await queue.delete_cron(simple_async_task(5, 6), "1 * * * *")
        assert not success
