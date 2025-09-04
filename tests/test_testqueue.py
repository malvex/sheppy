from datetime import datetime, timedelta, timezone

import pytest

from sheppy import TestQueue
from tests.dependencies import (
    TaskTestCase,
    TaskTestCases,
    simple_async_task,
    simple_sync_task,
)


class TestSuccessfulTasks:
    """Test all successful task variations using synchronous TestQueue."""

    @pytest.mark.parametrize("test_case", TaskTestCases.all_successful_tasks(), ids=lambda tc: tc.name)
    def test_task_execution(self, test_case: TaskTestCase):
        """Test that each task executes successfully."""
        queue = TestQueue()
        task = test_case.create_task()
        queue.add(task)

        # there should be no change on tasks after queueing
        assert not task.completed
        assert not task.error
        assert not task.result
        assert not task.metadata.finished_datetime

        # process the task
        task = queue.process_next()

        # verify returned task
        assert task is not None
        assert task.completed
        assert not task.error
        assert task.result == test_case.expected_result
        assert task.metadata.finished_datetime is not None

        # task should be in processed list
        assert len(queue.processed_tasks) == 1
        assert queue.processed_tasks[0].id == task.id


    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks(), ids=lambda tc: tc.name)
    def test_process_all(self, test_case: TaskTestCase):
        """Test process_all functionality."""
        queue = TestQueue()
        # add multiple tasks
        tasks = []
        for _ in range(3):
            task = test_case.create_task()
            queue.add(task)
            tasks.append(task)

        # process all tasks
        processed = queue.process_all()

        # verify all returned tasks completed
        assert len(processed) == 3
        for p_task in processed:
            assert p_task.completed
            assert not p_task.error
            assert p_task.result == test_case.expected_result
            assert p_task.metadata.finished_datetime is not None

        # all tasks should be in processed list
        assert len(queue.processed_tasks) == 3
        processed_ids = {t.id for t in queue.processed_tasks}
        task_ids = {t.id for t in tasks}
        assert processed_ids == task_ids


class TestFailingTasks:
    """Test task failure scenarios using synchronous TestQueue."""

    @pytest.mark.parametrize("test_case", TaskTestCases.failing_tasks(), ids=lambda tc: tc.name)
    def test_task_failure(self, test_case: TaskTestCase):
        """Test that tasks fail as expected."""
        queue = TestQueue()
        task = test_case.create_task()
        queue.add(task)

        # process the task
        task = queue.process_next()

        # verify failure on returned task
        assert task is not None
        assert not task.completed
        assert task.error
        assert task.error == test_case.expected_error
        assert task.metadata.finished_datetime is not None

        # failed task should still be in processed list
        assert len(queue.processed_tasks) == 1
        assert queue.processed_tasks[0].id == task.id


class TestTaskSelfReference:
    """Test tasks with self-reference using synchronous TestQueue."""

    @pytest.mark.parametrize("test_case", TaskTestCases.self_referencing_tasks(), ids=lambda tc: tc.name)
    def test_self_reference(self, test_case: TaskTestCase):
        """Test tasks that reference themselves."""
        queue = TestQueue()
        task = test_case.create_task()
        queue.add(task)

        task = queue.process_next()

        # verify returned task
        assert task is not None
        assert task.completed
        assert not task.error
        assert task.result["result"] == test_case.expected_result
        assert task.result["task_id"] == str(task.id)


class TestQueueBehavior:
    """Test TestQueue-specific behavior."""

    def test_empty_queue(self):
        """Test processing an empty queue."""
        queue = TestQueue()
        # should not raise, just return None
        result = queue.process_next()
        assert result is None

        # process_all should also handle empty queue gracefully
        queue.process_all()  # Should not raise

        # no tasks should be in processed list
        assert len(queue.processed_tasks) == 0

    def test_queue_ordering(self):
        """Test that tasks are processed in FIFO order."""
        queue = TestQueue()
        # add tasks with different values
        task1 = simple_sync_task(1, 2)
        task2 = simple_sync_task(3, 4)
        task3 = simple_sync_task(5, 6)

        queue.add(task1)
        queue.add(task2)
        queue.add(task3)

        # process in order
        task1 = queue.process_next()
        assert task1 is not None
        assert task1.id == task1.id
        assert task1.completed
        assert task1.result == 3

        task2 = queue.process_next()
        assert task2 is not None
        assert task2.id == task2.id
        assert task2.completed
        assert task2.result == 7

        task3 = queue.process_next()
        assert task3 is not None
        assert task3.id == task3.id
        assert task3.completed
        assert task3.result == 11

        # check processed order
        assert len(queue.processed_tasks) == 3

    def test_mixed_sync_async_tasks(self):
        """Test that TestQueue handles both sync and async tasks."""

        queue = TestQueue()
        sync_task = simple_sync_task(1, 2)
        async_task = simple_async_task(3, 4)

        queue.add(sync_task)
        queue.add(async_task)

        # process both
        processed = queue.process_all()

        assert len(processed) == 2
        assert processed[0].completed
        assert processed[0].result == 3
        assert processed[0].id == sync_task.id

        assert processed[1].completed
        assert processed[1].result == 7
        assert processed[1].id == async_task.id

        # original tasks remain unchanged
        assert not sync_task.completed
        assert not async_task.completed

        # both tasks should be in processed list
        assert len(queue.processed_tasks) == 2


class TestScheduledTasks:
    """Test scheduled task functionality with TestQueue."""

    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks(), ids=lambda tc: tc.name)
    def test_schedule_with_timedelta(self, test_case: TaskTestCase):
        """Test scheduling a task with a timedelta."""
        queue = TestQueue()
        task = test_case.create_task()

        # Schedule the task for 1 second from now
        delay = timedelta(seconds=1)
        result = queue.schedule(task, at=delay)
        assert result is True

        # Task should not be immediately available
        immediate = queue.process_next()
        assert immediate is None

        # Process scheduled tasks (with time in the future)
        future_time = datetime.now(timezone.utc) + timedelta(seconds=2)
        processed = queue.process_scheduled(at=future_time)

        # Task should have been processed
        assert len(processed) == 1
        processed_task = processed[0]
        assert processed_task.completed
        assert not processed_task.error
        assert processed_task.result == test_case.expected_result

        # Task should be in processed list
        assert len(queue.processed_tasks) == 1
        assert queue.processed_tasks[0].id == task.id

    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks()[:1], ids=lambda tc: tc.name)
    def test_schedule_with_datetime(self, test_case: TaskTestCase):
        """Test scheduling a task with a specific datetime."""
        queue = TestQueue()
        task = test_case.create_task()

        # Schedule for a specific time
        scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=1)
        result = queue.schedule(task, at=scheduled_time)
        assert result is True

        # Process scheduled tasks at that time
        processed = queue.process_scheduled(at=scheduled_time)

        # Task should have been processed
        assert len(processed) == 1
        assert processed[0].completed
        assert processed[0].result == test_case.expected_result

    def test_multiple_scheduled_tasks(self):
        """Test scheduling multiple tasks at different times."""
        queue = TestQueue()

        # Create tasks with different scheduled times
        task1 = simple_sync_task(1, 2)
        task2 = simple_sync_task(3, 4)
        task3 = simple_sync_task(5, 6)

        now = datetime.now(timezone.utc)

        # Schedule tasks at different times
        queue.schedule(task1, at=now + timedelta(seconds=1))
        queue.schedule(task2, at=now + timedelta(seconds=2))
        queue.schedule(task3, at=now + timedelta(seconds=3))

        # Process scheduled tasks up to 1.5 seconds
        processed = queue.process_scheduled(at=now + timedelta(seconds=1.5))

        # Only first task should be processed
        assert len(processed) == 1
        assert processed[0].result == 3

        # Process up to 2.5 seconds
        processed = queue.process_scheduled(at=now + timedelta(seconds=2.5))

        # Second task should be processed
        assert len(processed) == 1
        assert processed[0].result == 7

        # Process all remaining
        processed = queue.process_scheduled(at=now + timedelta(seconds=4))

        # Third task should be processed
        assert len(processed) == 1
        assert processed[0].result == 11

        # All tasks should be in processed list
        assert len(queue.processed_tasks) == 3

    def test_scheduled_task_with_immediate_task(self):
        """Test that immediate and scheduled tasks work independently."""
        queue = TestQueue()

        # Schedule a task for later
        scheduled_task = simple_sync_task(1, 2)
        future_time = datetime.now(timezone.utc) + timedelta(seconds=1)
        queue.schedule(scheduled_task, at=future_time)

        # Add an immediate task
        immediate_task = simple_sync_task(3, 4)
        queue.add(immediate_task)

        # Process immediate task
        processed_immediate = queue.process_next()
        assert processed_immediate is not None
        assert processed_immediate.completed
        assert processed_immediate.result == 7

        # No more immediate tasks
        assert queue.process_next() is None

        # Process scheduled task
        processed_scheduled = queue.process_scheduled(at=future_time)
        assert len(processed_scheduled) == 1
        assert processed_scheduled[0].completed
        assert processed_scheduled[0].result == 3

        # Both should be in processed list
        assert len(queue.processed_tasks) == 2

    @pytest.mark.parametrize("test_case", TaskTestCases.failing_tasks()[:1], ids=lambda tc: tc.name)
    def test_scheduled_failing_task(self, test_case: TaskTestCase):
        """Test that scheduled tasks can fail as expected."""
        queue = TestQueue()
        task = test_case.create_task()

        # Schedule the task
        future_time = datetime.now(timezone.utc) + timedelta(seconds=1)
        queue.schedule(task, at=future_time)

        # Process the task
        queue.process_scheduled(at=future_time)

        # Task should be in failed list
        assert len(queue.failed_tasks) == 1
        failed = queue.failed_tasks[0]
        assert not failed.completed
        assert failed.error
        assert failed.error == test_case.expected_error

    def test_schedule_in_past(self):
        """Test scheduling a task in the past (should be available immediately)."""
        queue = TestQueue()
        task = simple_sync_task(1, 2)

        past_time = datetime.now(timezone.utc) - timedelta(seconds=10)

        # Schedule in the past
        queue.schedule(task, at=past_time)

        # Should be available when processing scheduled tasks for "now"
        processed = queue.process_scheduled()

        assert len(processed) == 1
        assert processed[0].completed
        assert processed[0].result == 3

    def test_process_scheduled_empty(self):
        """Test processing scheduled tasks when queue is empty."""
        queue = TestQueue()

        # Should handle empty queue gracefully
        processed = queue.process_scheduled()
        assert processed == []
        assert len(queue.processed_tasks) == 0

    def test_scheduled_tasks_ordering(self):
        """Test that scheduled tasks are processed in chronological order."""
        queue = TestQueue()

        now = datetime.now(timezone.utc)

        # Add tasks in non-chronological order
        task3 = simple_sync_task(5, 6)
        task1 = simple_sync_task(1, 2)
        task2 = simple_sync_task(3, 4)

        queue.schedule(task3, at=now + timedelta(seconds=3))
        queue.schedule(task1, at=now + timedelta(seconds=1))
        queue.schedule(task2, at=now + timedelta(seconds=2))

        # Process all scheduled tasks
        processed = queue.process_scheduled(at=now + timedelta(seconds=5))

        # Should be processed in chronological order
        assert len(processed) == 3
        assert processed[0].result == 3  # task1
        assert processed[1].result == 7  # task2
        assert processed[2].result == 11  # task3


class TestCronOperations:
    """Test cron task operations with TestQueue."""

    def test_add_cron(self):
        queue = TestQueue()

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
                should_succeed = True if i == 0 else False
                print(i, should_succeed)

                success = queue.add_cron(task, schedule)
                assert success is should_succeed

        all_crons = queue.list_crons()
        assert len(all_crons) == 4

        for task, schedule in crons:
            for i in range(5):
                # same for deleting them - only first should be successful
                should_succeed = True if i == 0 else False

                success = queue.delete_cron(task, schedule)
                assert success is should_succeed

        all_crons = queue.list_crons()
        assert len(all_crons) == 0

    def test_delete_nonexistent_cron(self):
        queue = TestQueue()
        success = queue.delete_cron(simple_async_task(5, 6), "1 * * * *")
        assert success == False
