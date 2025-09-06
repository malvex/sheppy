from datetime import datetime, timedelta, timezone

import pytest

from sheppy import TestQueue
from tests.dependencies import (
    TaskTestCase,
    TaskTestCases,
    simple_async_task,
    simple_sync_task,
    WrappedNumber,
    task_add_with_middleware_noop,
    task_add_with_middleware_change_arg,
    task_add_with_middleware_change_return_value,
    task_add_with_middleware_multiple,
    task_add_with_middleware_change_return_type,
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
        assert not task.finished_at

        # process the task
        task = queue.process_next()

        # verify returned task
        assert task is not None
        assert task.completed
        assert not task.error
        assert task.result == test_case.expected_result
        assert task.finished_at is not None

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
            assert p_task.finished_at is not None

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
        assert task.finished_at is not None

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

        delay = timedelta(hours=1)
        result = queue.schedule(task, at=delay)
        assert result is True

        immediate = queue.process_next()
        assert immediate is None

        future_time = datetime.now(timezone.utc) + timedelta(hours=2)
        processed = queue.process_scheduled(at=future_time)

        assert len(processed) == 1
        processed_task = processed[0]
        assert processed_task.completed
        assert not processed_task.error
        assert processed_task.result == test_case.expected_result

        assert len(queue.processed_tasks) == 1
        assert queue.processed_tasks[0].id == task.id

    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks()[:1], ids=lambda tc: tc.name)
    def test_schedule_with_datetime(self, test_case: TaskTestCase):
        """Test scheduling a task with a specific datetime."""
        queue = TestQueue()
        task = test_case.create_task()

        scheduled_time = datetime.now(timezone.utc) + timedelta(hours=1)
        result = queue.schedule(task, at=scheduled_time)
        assert result is True

        processed = queue.process_scheduled(at=scheduled_time)

        assert len(processed) == 1
        assert processed[0].completed
        assert processed[0].result == test_case.expected_result

    def test_multiple_scheduled_tasks(self):
        """Test scheduling multiple tasks at different times."""
        queue = TestQueue()

        task1 = simple_sync_task(1, 2)
        task2 = simple_sync_task(3, 4)
        task3 = simple_sync_task(5, 6)

        now = datetime.now(timezone.utc)

        queue.schedule(task1, at=now + timedelta(hours=1))
        queue.schedule(task2, at=now + timedelta(hours=2))
        queue.schedule(task3, at=now + timedelta(hours=3))

        processed = queue.process_scheduled(at=now + timedelta(hours=1))

        assert len(processed) == 1
        assert processed[0].result == 3

        processed = queue.process_scheduled(at=now + timedelta(hours=2))

        assert len(processed) == 1
        assert processed[0].result == 7

        processed = queue.process_scheduled(at=now + timedelta(hours=10))

        assert len(processed) == 1
        assert processed[0].result == 11

        assert len(queue.processed_tasks) == 3

    def test_scheduled_task_with_immediate_task(self):
        """Test that immediate and scheduled tasks work independently."""
        queue = TestQueue()

        scheduled_task = simple_sync_task(1, 2)
        future_time = datetime.now(timezone.utc) + timedelta(hours=1)
        queue.schedule(scheduled_task, at=future_time)

        immediate_task = simple_sync_task(3, 4)
        queue.add(immediate_task)

        processed_immediate = queue.process_next()
        assert processed_immediate is not None
        assert processed_immediate.completed
        assert processed_immediate.result == 7

        assert queue.process_next() is None

        processed_scheduled = queue.process_scheduled(at=future_time)
        assert len(processed_scheduled) == 1
        assert processed_scheduled[0].completed
        assert processed_scheduled[0].result == 3

        assert len(queue.processed_tasks) == 2

    @pytest.mark.parametrize("test_case", TaskTestCases.failing_tasks()[:1], ids=lambda tc: tc.name)
    def test_scheduled_failing_task(self, test_case: TaskTestCase):
        """Test that scheduled tasks can fail as expected."""
        queue = TestQueue()
        task = test_case.create_task()

        future_time = datetime.now(timezone.utc) + timedelta(hours=1)
        queue.schedule(task, at=future_time)

        queue.process_scheduled(at=future_time)

        assert len(queue.failed_tasks) == 1
        failed = queue.failed_tasks[0]
        assert not failed.completed
        assert failed.error
        assert failed.error == test_case.expected_error

    def test_schedule_in_past(self):
        """Test scheduling a task in the past (should be available immediately)."""
        queue = TestQueue()
        task = simple_sync_task(1, 2)

        past_time = datetime.now(timezone.utc) - timedelta(hours=10)

        queue.schedule(task, at=past_time)

        processed = queue.process_scheduled()

        assert len(processed) == 1
        assert processed[0].completed
        assert processed[0].result == 3

    def test_process_scheduled_empty(self):
        """Test processing scheduled tasks when queue is empty."""
        queue = TestQueue()

        processed = queue.process_scheduled()
        assert processed == []
        assert len(queue.processed_tasks) == 0

    def test_scheduled_tasks_ordering(self):
        """Test that scheduled tasks are processed in chronological order."""
        queue = TestQueue()

        now = datetime.now(timezone.utc)

        task3 = simple_sync_task(5, 6)
        task1 = simple_sync_task(1, 2)
        task2 = simple_sync_task(3, 4)

        queue.schedule(task3, at=now + timedelta(hours=3))
        queue.schedule(task1, at=now + timedelta(hours=1))
        queue.schedule(task2, at=now + timedelta(hours=2))

        processed = queue.process_scheduled(at=now + timedelta(hours=5))

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


class TestMiddleware:

    def test_persists_on_task(self):
        queue = TestQueue()

        t1 = simple_async_task(1, 2)
        t2 = task_add_with_middleware_noop(1, 2)

        assert len(t1.spec.middleware) == 0
        assert len(t2.spec.middleware) == 1

        queue.add([t1, t2])
        t1, t2 = queue.process_all()

        assert len(t1.spec.middleware) == 0
        assert len(t2.spec.middleware) == 1

        assert t1.result == 3
        assert t2.result == 3

    def test_noop(self):
        queue = TestQueue()

        task = task_add_with_middleware_noop(1, 2)

        queue.add(task)
        task = queue.process_next()

        assert task.result == 3

    def test_change_arg(self):
        queue = TestQueue()

        task = task_add_with_middleware_change_arg(1, 2)

        queue.add(task)
        task = queue.process_next()

        assert task.result == 7

    def test_change_return_value(self):
        queue = TestQueue()

        task = task_add_with_middleware_change_return_value(1, 2)

        queue.add(task)
        task = queue.process_next()

        assert task.result == 100003

    def test_multiple(self):
        queue = TestQueue()

        task = task_add_with_middleware_multiple(1, 2)

        queue.add(task)
        task = queue.process_next()

        assert task.result == 100007

    def test_change_return_type(self):
        queue = TestQueue()

        task = task_add_with_middleware_change_return_type(1, 2)

        queue.add(task)
        task = queue.process_next()

        assert task.result == WrappedNumber(result=100003, extra="hi from middleware")
