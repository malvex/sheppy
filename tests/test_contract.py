import asyncio
from datetime import datetime, timedelta, timezone
import pytest

from sheppy import Queue, Worker

from tests.dependencies import TaskTestCases, TaskTestCase


class TestSuccessfulTasks:
    """Test all successful task variations."""

    @pytest.mark.parametrize("test_case", TaskTestCases.all_successful_tasks(), ids=lambda tc: tc.name)
    async def test_task_execution(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        """Test that each task executes successfully."""

        task = test_case.create_task()
        await queue.add(task)

        # verify initial state
        assert not task.completed
        assert not task.error
        assert not task.result
        assert not task.metadata.finished_datetime

        # process the task
        await worker.work(max_tasks=1)

        # refresh and verify completion
        task = await queue.refresh(task)

        assert task.completed
        assert not task.error
        assert task.result == test_case.expected_result
        assert task.metadata.finished_datetime is not None


    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks(), ids=lambda tc: tc.name)
    async def test_wait_for_finished(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        """Test wait_for_finished functionality (testing subset for speed)."""
        task = test_case.create_task()
        await queue.add(task)

        # process in background
        process_task = asyncio.create_task(worker.work(max_tasks=1))

        # wait for result
        task = await queue.wait_for_finished(task, timeout=3.0)
        assert task.result == test_case.expected_result

        assert process_task.done()

    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks()[1:2], ids=lambda tc: tc.name)
    async def test_wait_for_finished_timeout(self, test_case: TaskTestCase, queue: Queue):

        task = test_case.create_task()
        await queue.add(task)

        # we aren't processing the task => should raise exception
        with pytest.raises(TimeoutError):
            await queue.wait_for_finished(task, timeout=0.5)


class TestFailingTasks:
    """Test task failure scenarios."""

    @pytest.mark.parametrize("test_case", TaskTestCases.failing_tasks(), ids=lambda tc: tc.name)
    async def test_task_failure(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        """Test that tasks fail as expected."""

        task = test_case.create_task()
        await queue.add(task)

        # process the task
        await worker.work(max_tasks=1)

        # refresh and verify failure
        task = await queue.refresh(task)

        assert not task.completed
        assert task.error
        assert task.error == test_case.expected_error
        assert task.metadata.finished_datetime is not None


class TestTaskSelfReference:
    """Test tasks with self-reference."""

    @pytest.mark.parametrize("test_case", TaskTestCases.self_referencing_tasks(), ids=lambda tc: tc.name)
    async def test_self_reference(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        """Test tasks that reference themselves."""
        task = test_case.create_task()

        task_id = task.id

        await queue.add(task)
        await worker.work(max_tasks=1)
        task = await queue.refresh(task)

        assert task.completed
        assert not task.error
        assert task.result["result"] == test_case.expected_result
        assert task.result["task_id"] == str(task_id)


class TestScheduledTasks:
    """Test scheduled task functionality."""

    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks(), ids=lambda tc: tc.name)
    async def test_schedule_with_timedelta(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        """Test scheduling a task with a timedelta."""
        task = test_case.create_task()
        delay = timedelta(seconds=0.1)

        # Schedule the task
        result = await queue.schedule(task, at=delay)
        assert result is True

        # Task should not be immediately available for worker to process
        popped = await queue._pop(timeout=0.01)
        assert popped is None

        # Task should not be completed yet
        task = await queue.refresh(task)
        assert not task.completed
        assert not task.error

        # Wait for the scheduled time
        await asyncio.sleep(0.15)

        # Process scheduled tasks
        await worker.work(max_tasks=1)

        # Task should now be completed
        task = await queue.refresh(task)
        assert task.completed
        assert not task.error
        assert task.result == test_case.expected_result

    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks()[:1], ids=lambda tc: tc.name)
    async def test_schedule_with_datetime(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        """Test scheduling a task with a specific datetime."""
        task = test_case.create_task()
        scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=0.1)

        # Schedule the task
        result = await queue.schedule(task, at=scheduled_time)
        assert result is True

        # Task should not be immediately available for worker to process
        popped = await queue._pop(timeout=0.01)
        assert popped is None

        # Wait for the scheduled time
        await asyncio.sleep(0.15)

        # Process scheduled tasks
        await worker.work(max_tasks=1)

        # Task should now be completed
        task = await queue.refresh(task)
        assert task.completed
        assert not task.error
        assert task.result == test_case.expected_result

    async def test_multiple_scheduled_tasks(self, queue: Queue, worker: Worker):
        """Test scheduling multiple tasks at different times."""
        from tests.dependencies import simple_sync_task

        # Create tasks with different scheduled times
        task1 = simple_sync_task(1, 2)
        task2 = simple_sync_task(3, 4)
        task3 = simple_sync_task(5, 6)

        # Schedule tasks at different times with longer intervals for Redis
        await queue.schedule(task1, at=timedelta(seconds=0.2))
        await queue.schedule(task2, at=timedelta(seconds=0.4))
        await queue.schedule(task3, at=timedelta(seconds=0.6))

        # No tasks should be immediately available
        popped = await queue._pop(timeout=0.01)
        assert popped is None

        # Wait and process first task
        await asyncio.sleep(0.25)
        await worker.work(max_tasks=1)
        task1 = await queue.refresh(task1)
        assert task1.completed
        assert task1.result == 3

        # Others should still be pending
        task2 = await queue.refresh(task2)
        task3 = await queue.refresh(task3)
        assert not task2.completed
        assert not task3.completed

        # Wait and process second task
        await asyncio.sleep(0.2)
        await worker.work(max_tasks=1)
        task2 = await queue.refresh(task2)
        assert task2.completed
        assert task2.result == 7

        # Third should still be pending
        task3 = await queue.refresh(task3)
        assert not task3.completed

        # Wait and process third task
        await asyncio.sleep(0.2)
        await worker.work(max_tasks=1)
        task3 = await queue.refresh(task3)
        assert task3.completed
        assert task3.result == 11

    async def test_scheduled_task_with_immediate_task(self, queue: Queue, worker: Worker):
        """Test that immediate tasks are processed before scheduled tasks."""
        from tests.dependencies import simple_sync_task

        # Schedule a task for later
        scheduled_task = simple_sync_task(1, 2)
        await queue.schedule(scheduled_task, at=timedelta(seconds=0.2))

        # Add an immediate task
        immediate_task = simple_sync_task(3, 4)
        await queue.add(immediate_task)

        # Process immediate task first
        await worker.work(max_tasks=1)

        immediate_task = await queue.refresh(immediate_task)
        scheduled_task = await queue.refresh(scheduled_task)

        # Immediate task should be completed
        assert immediate_task.completed
        assert immediate_task.result == 7

        # Scheduled task should still be pending
        assert not scheduled_task.completed

        # Wait for scheduled time and process
        await asyncio.sleep(0.25)
        await worker.work(max_tasks=1)

        scheduled_task = await queue.refresh(scheduled_task)
        assert scheduled_task.completed
        assert scheduled_task.result == 3

    @pytest.mark.parametrize("test_case", TaskTestCases.failing_tasks()[:1], ids=lambda tc: tc.name)
    async def test_scheduled_failing_task(self, test_case: TaskTestCase, queue: Queue, worker: Worker):
        """Test that scheduled tasks can fail as expected."""
        task = test_case.create_task()

        # Schedule the task
        await queue.schedule(task, at=timedelta(seconds=0.1))

        # Wait for scheduled time
        await asyncio.sleep(0.15)

        # Process the task
        await worker.work(max_tasks=1)

        # Task should have failed
        task = await queue.refresh(task)
        assert not task.completed
        assert task.error
        assert task.error == test_case.expected_error

    async def test_schedule_in_past(self, queue: Queue, worker: Worker):
        """Test scheduling a task in the past (should be available immediately)."""
        from tests.dependencies import simple_sync_task

        task = simple_sync_task(1, 2)
        past_time = datetime.now(timezone.utc) - timedelta(seconds=10)

        # Schedule in the past
        await queue.schedule(task, at=past_time)

        # Should be available immediately
        await worker.work(max_tasks=1)

        task = await queue.refresh(task)
        assert task.completed
        assert task.result == 3
