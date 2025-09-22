from datetime import datetime, timedelta, timezone

import pytest

from sheppy import TestQueue
from sheppy.testqueue import assert_is_completed, assert_is_failed, assert_is_new
from tests.dependencies import (
    TaskTestCase,
    TaskTestCases,
    WrappedNumber,
    simple_async_task,
    simple_sync_task,
    task_add_with_middleware_change_arg,
    task_add_with_middleware_change_return_type,
    task_add_with_middleware_change_return_value,
    task_add_with_middleware_multiple,
    task_add_with_middleware_noop,
)


class TestSuccessfulTasks:
    @pytest.mark.parametrize("test_case", TaskTestCases.all_successful_tasks(), ids=lambda tc: tc.name)
    def test_task_execution(self, test_case: TaskTestCase):
        queue = TestQueue()
        task = test_case.create_task()
        queue.add(task)

        # there should be no change on tasks after queueing
        assert_is_new(task)

        # process the task
        processed = queue.process_next()

        # original task must not be modified
        assert_is_new(task)

        # verify returned task
        assert_is_completed(processed)
        assert processed.result == test_case.expected_result

        # task should be in processed list
        assert len(queue.processed_tasks) == 1
        assert queue.processed_tasks[0].id == processed.id
        assert_is_completed(queue.processed_tasks[0])


    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks(), ids=lambda tc: tc.name)
    def test_process_all(self, test_case: TaskTestCase):
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
        for t in processed:
            assert_is_completed(t)
            assert t.result == test_case.expected_result

        # all tasks should be in processed list
        assert len(queue.processed_tasks) == 3
        processed_ids = {t.id for t in queue.processed_tasks}
        task_ids = {t.id for t in tasks}
        assert processed_ids == task_ids


class TestFailingTasks:
    @pytest.mark.parametrize("test_case", TaskTestCases.failing_tasks(), ids=lambda tc: tc.name)
    def test_task_failure(self, test_case: TaskTestCase):
        queue = TestQueue()
        task = test_case.create_task()
        queue.add(task)

        # process the task
        processed = queue.process_next()

        # original task must not be modified
        assert_is_new(task)

        # verify failure on returned task
        assert_is_failed(processed)
        assert processed.error == test_case.expected_error
        assert processed.finished_at is not None

        # failed task should still be in processed list
        assert len(queue.processed_tasks) == 1
        assert queue.processed_tasks[0].id == processed.id
        assert_is_failed(queue.processed_tasks[0])


class TestTaskSelfReference:
    @pytest.mark.parametrize("test_case", TaskTestCases.self_referencing_tasks(), ids=lambda tc: tc.name)
    def test_self_reference(self, test_case: TaskTestCase):
        queue = TestQueue()
        task = test_case.create_task()
        queue.add(task)

        task = queue.process_next()

        # verify returned task
        assert_is_completed(task)
        assert task.result == {"result": test_case.expected_result, "task_id": str(task.id)}


class TestQueueBehavior:
    def test_empty_queue(self):
        queue = TestQueue()
        # should not raise, just return None
        result = queue.process_next()
        assert result is None

        # process_all should also handle empty queue gracefully
        queue.process_all()  # Should not raise

        # no tasks should be in processed list
        assert len(queue.processed_tasks) == 0

    def test_queue_ordering(self):
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
        assert_is_completed(task1)
        assert task1.result == 3

        task2 = queue.process_next()
        assert_is_completed(task2)
        assert task2.result == 7

        task3 = queue.process_next()
        assert_is_completed(task3)
        assert task3.result == 11

        # check processed order
        assert len(queue.processed_tasks) == 3

    def test_mixed_sync_async_tasks(self):
        queue = TestQueue()
        sync_task = simple_sync_task(1, 2)
        async_task = simple_async_task(3, 4)

        assert queue.add(sync_task) == [True]
        assert queue.add(async_task) == [True]

        # process both
        processed = queue.process_all()

        assert len(processed) == 2
        assert_is_completed(processed[0])
        assert processed[0].result == 3
        assert processed[0].id == sync_task.id

        assert_is_completed(processed[1])
        assert processed[1].result == 7
        assert processed[1].id == async_task.id

        # original task must not be modified
        assert_is_new(sync_task)
        assert_is_new(async_task)

        # both tasks should be in processed list
        assert len(queue.processed_tasks) == 2


class TestScheduledTasks:
    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks(), ids=lambda tc: tc.name)
    def test_schedule_with_timedelta(self, test_case: TaskTestCase):
        queue = TestQueue()
        task = test_case.create_task()

        delay = timedelta(hours=1)
        assert queue.schedule(task, at=delay) is True

        immediate = queue.process_next()
        assert immediate is None

        future_time = datetime.now(timezone.utc) + timedelta(hours=2)
        processed = queue.process_scheduled(at=future_time)

        assert len(processed) == 1
        processed_task = processed[0]
        assert_is_completed(processed_task)
        assert processed_task.result == test_case.expected_result

        assert len(queue.processed_tasks) == 1
        assert queue.processed_tasks[0].id == task.id
        assert_is_completed(queue.processed_tasks[0])

    @pytest.mark.parametrize("test_case", TaskTestCases.subset_successful_tasks()[:1], ids=lambda tc: tc.name)
    def test_schedule_with_datetime(self, test_case: TaskTestCase):
        queue = TestQueue()
        task = test_case.create_task()

        scheduled_time = datetime.now(timezone.utc) + timedelta(hours=1)
        assert queue.schedule(task, at=scheduled_time) is True

        processed = queue.process_scheduled(at=scheduled_time)

        assert len(processed) == 1
        assert_is_completed(processed[0])
        assert processed[0].result == test_case.expected_result

    def test_multiple_scheduled_tasks(self):
        queue = TestQueue()

        task1 = simple_sync_task(1, 2)
        task2 = simple_sync_task(3, 4)
        task3 = simple_sync_task(5, 6)

        now = datetime.now(timezone.utc)

        assert queue.schedule(task1, at=now + timedelta(hours=1)) is True
        assert queue.schedule(task2, at=now + timedelta(hours=2)) is True
        assert queue.schedule(task3, at=now + timedelta(hours=3)) is True

        processed = queue.process_scheduled(at=now + timedelta(hours=1))

        assert len(processed) == 1
        assert_is_completed(processed[0])
        assert processed[0].result == 3

        processed = queue.process_scheduled(at=now + timedelta(hours=2))

        assert len(processed) == 1
        assert_is_completed(processed[0])
        assert processed[0].result == 7

        processed = queue.process_scheduled(at=now + timedelta(hours=10))

        assert len(processed) == 1
        assert_is_completed(processed[0])
        assert processed[0].result == 11

        assert len(queue.processed_tasks) == 3

    def test_scheduled_task_with_immediate_task(self):
        queue = TestQueue()

        scheduled_task = simple_sync_task(1, 2)
        future_time = datetime.now(timezone.utc) + timedelta(hours=1)
        assert queue.schedule(scheduled_task, at=future_time) is True

        immediate_task = simple_sync_task(3, 4)
        assert queue.add(immediate_task) == [True]

        processed_immediate = queue.process_next()
        assert_is_completed(processed_immediate)
        assert processed_immediate.result == 7

        assert queue.process_next() is None

        processed_scheduled = queue.process_scheduled(at=future_time)
        assert len(processed_scheduled) == 1
        assert_is_completed(processed_scheduled[0])
        assert processed_scheduled[0].result == 3

        assert len(queue.processed_tasks) == 2

    @pytest.mark.parametrize("test_case", TaskTestCases.failing_tasks()[:1], ids=lambda tc: tc.name)
    def test_scheduled_failing_task(self, test_case: TaskTestCase):
        queue = TestQueue()
        task = test_case.create_task()

        future_time = datetime.now(timezone.utc) + timedelta(hours=1)
        assert queue.schedule(task, at=future_time) is True

        processed = queue.process_scheduled(at=future_time)

        assert len(processed) == 1
        assert_is_failed(processed[0])
        assert processed[0].error == test_case.expected_error
        assert len(queue.failed_tasks) == 1

    def test_schedule_in_past(self):
        queue = TestQueue()
        task = simple_sync_task(1, 2)

        past_time = datetime.now(timezone.utc) - timedelta(hours=10)

        assert queue.schedule(task, at=past_time) is True

        processed = queue.process_scheduled()

        assert len(processed) == 1
        assert_is_completed(processed[0])
        assert processed[0].result == 3

    def test_process_scheduled_empty(self):
        queue = TestQueue()

        processed = queue.process_scheduled()
        assert processed == []
        assert len(queue.processed_tasks) == 0

    def test_scheduled_tasks_ordering(self):
        queue = TestQueue()

        now = datetime.now(timezone.utc)

        task3 = simple_sync_task(5, 6)
        task1 = simple_sync_task(1, 2)
        task2 = simple_sync_task(3, 4)

        assert queue.schedule(task3, at=now + timedelta(hours=3)) is True
        assert queue.schedule(task1, at=now + timedelta(hours=1)) is True
        assert queue.schedule(task2, at=now + timedelta(hours=2)) is True

        assert queue.process_next() is None
        assert queue.process_all() == []

        processed = queue.process_scheduled(at=now + timedelta(hours=5))

        assert len(processed) == 3

        assert processed[0].id == task1.id
        assert processed[1].id == task2.id
        assert processed[2].id == task3.id

        assert_is_completed(processed[0])
        assert_is_completed(processed[1])
        assert_is_completed(processed[2])

        assert processed[0].result == 3
        assert processed[1].result == 7
        assert processed[2].result == 11


class TestCronOperations:
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
                should_succeed = True if i == 0 else False  # noqa: SIM210

                success = queue.add_cron(task, schedule)
                assert success is should_succeed

        all_crons = queue.list_crons()
        assert len(all_crons) == 4

        for task, schedule in crons:
            for i in range(5):
                # same for deleting them - only first should be successful
                should_succeed = True if i == 0 else False  # noqa: SIM210

                success = queue.delete_cron(task, schedule)
                assert success is should_succeed

        all_crons = queue.list_crons()
        assert len(all_crons) == 0

    def test_delete_nonexistent_cron(self):
        queue = TestQueue()
        success = queue.delete_cron(simple_async_task(5, 6), "1 * * * *")
        assert not success


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

        assert queue.add(task) == [True]
        task = queue.process_next()

        assert task.result == 3

    def test_change_arg(self):
        queue = TestQueue()

        task = task_add_with_middleware_change_arg(1, 2)

        assert queue.add(task) == [True]
        task = queue.process_next()

        assert task.result == 7

    def test_change_return_value(self):
        queue = TestQueue()

        task = task_add_with_middleware_change_return_value(1, 2)

        assert queue.add(task) == [True]
        task = queue.process_next()

        assert task.result == 100003

    def test_multiple(self):
        queue = TestQueue()

        task = task_add_with_middleware_multiple(1, 2)

        assert queue.add(task) == [True]
        task = queue.process_next()

        assert task.result == 100007

    def test_change_return_type(self):
        queue = TestQueue()

        task = task_add_with_middleware_change_return_type(1, 2)

        assert queue.add(task) == [True]
        task = queue.process_next()

        assert task.result == WrappedNumber(result=100003, extra="hi from middleware")
