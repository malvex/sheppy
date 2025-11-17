import asyncio
from datetime import datetime, timedelta, timezone
from typing import overload
from uuid import UUID

from .backend.memory import MemoryBackend
from .models import Task, TaskCron
from .queue import Queue
from .utils.task_execution import TaskProcessor


class TestQueue:
    """A simple in-memory queue for testing purposes.

    This queue does not require any external services and processes tasks synchronously.
    It is designed to be used in synchronous tests and follows the same execution flow as a real queue.

    Args:
        name (str): Name of the queue. Defaults to "test-queue".

    Attributes:
        processed_tasks (list[Task]): List of tasks that have been processed.
        failed_tasks (list[Task]): List of tasks that have failed during processing.

    Example:
        ```python
        # tests/test_tasks.py
        from sheppy import task, TestQueue
        from sheppy.testqueue import assert_is_new, assert_is_completed, assert_is_failed


        @task
        async def add(x: int, y: int) -> int:
            return x + y


        @task
        async def divide(x: int, y: int) -> float:
            return x / y


        def test_add_task():
            q = TestQueue()

            t = add(1, 2)

            # use helper function to check task is new
            assert_is_new(t)

            # add task to the queue
            success = q.add(t)
            assert success is True

            # check queue size
            assert q.size() == 1

            # process the task
            processed_task = q.process_next()

            # check task is completed
            assert_is_completed(processed_task)
            assert processed_task.result == 3

            # check queue size is now zero
            assert q.size() == 0


        def test_failing_task():
            q = TestQueue()

            t = divide(1, 0)

            # use helper function to check task is new
            assert_is_new(t)

            # add task to the queue
            success = q.add(t)
            assert success is True

            # check queue size
            assert q.size() == 1

            # process the task
            processed_task = q.process_next()

            # check task is failed
            assert_is_failed(processed_task)
            assert processed_task.result is None
            assert processed_task.error == "ZeroDivisionError: division by zero"

            # check queue size is now zero
            assert q.size() == 0
        ```
    """
    __test__ = False

    def __init__(
        self,
        name: str = "test-queue",
        #dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None  # ! FIXME
    ):
        self.name = name

        self._backend = MemoryBackend()
        self._backend._connected = True
        self._queue = Queue(self._backend, self.name)
        #self._dependency_resolver = DependencyResolver(dependency_overrides)
        self._worker_id = "TestQueue"
        self._task_processor = TaskProcessor()

        self.processed_tasks: list[Task] = []
        self.failed_tasks: list[Task] = []

    @overload
    def add(self, task: Task) -> bool: ...

    @overload
    def add(self, task: list[Task]) -> list[bool]: ...

    def add(self, task: Task | list[Task]) -> bool | list[bool]:
        """
        Add task into the queue. Accept list of tasks for batch add.

        Args:
            task: Instance of a Task, or list of Task instances for batch mode.

        Returns:
            Success boolean, or list of booleans in batch mode.

        Example:
            ```python
            q = TestQueue()

            # add single task
            success = q.add(task)
            assert success is True

            # add multiple tasks
            results = q.add([task1, task2, task3])
            assert results == [True, True, True]
            ```
        """
        return asyncio.run(self._queue.add(task))  # type: ignore[return-value]

    @overload
    def get_task(self, task: Task | UUID | str) -> Task | None: ...

    @overload
    def get_task(self, task: list[Task | UUID | str]) -> dict[UUID, Task]: ...

    def get_task(self, task: Task | UUID | str | list[Task | UUID | str]) -> Task | None | dict[UUID, Task]:
        """Get task by id.

        Args:
            task: Instance of a Task or its ID, or list of Task instances/IDs for batch mode.

        Returns:
            (Task|None): Instance of a Task or None if not found.
            (dict[UUID, Task]): *(In batch mode)* Returns Dictionary of Task IDs to Task instances.
        """
        return asyncio.run(self._queue.get_task(task))

    def get_all_tasks(self) -> list[Task]:
        """Get all tasks, including completed/failed ones.

        Returns:
            List of all tasks
        """
        return asyncio.run(self._queue.get_all_tasks())

    def get_pending(self, count: int = 1) -> list[Task]:
        """List pending tasks.

        Args:
            count: Number of pending tasks to retrieve.

        Returns:
            List of pending tasks
        """
        return asyncio.run(self._queue.get_pending(count))

    def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        """Schedule task to be processed after certain time.

        Args:
            task (Task): Instance of a Task
            at (datetime | timedelta): When to process the task.<br>
                                       If timedelta is provided, it will be added to current time.<br>
                                       *Note: datetime must be offset-aware (i.e. have timezone info).*

        Returns:
            Success boolean

        Example:
            ```python
            q = TestQueue()

            # schedule task to be processed after 10 minutes
            success = q.schedule(task, timedelta(minutes=10))
            assert success is True

            # ... or at specific time
            q.schedule(task, datetime.fromisoformat("2026-01-01 00:00:00 +00:00"))
            ```
        """
        return asyncio.run(self._queue.schedule(task, at))

    def get_scheduled(self) -> list[Task]:
        """List scheduled tasks.

        Returns:
            List of scheduled tasks
        """
        return asyncio.run(self._queue.get_scheduled())

    def retry(self, task: Task | UUID | str, at: datetime | timedelta | None = None, force: bool = False) -> bool:
        """Retry failed task.

        Args:
            task: Instance of a Task or its ID
            at: When to retry the task.<br>
                - If None (default), retries immediately.<br>
                - If timedelta is provided, it will be added to current time.<br>
                *Note: datetime must be offset-aware (i.e. have timezone info).*
            force: If True, allows retrying even if task has completed successfully. Defaults to False.

        Returns:
            Success boolean

        Raises:
            ValueError: If task has already completed successfully and force is not set to True.
            TypeError: If provided datetime is not offset-aware.

        Example:
            ```python
            q = TestQueue()

            # retry immediately
            success = q.retry(task)
            assert success is True

            # or retry after 5 minutes
            q.retry(task, at=timedelta(minutes=5))

            # or at specific time
            q.retry(task, at=datetime.fromisoformat("2026-01-01 00:00:00 +00:00"))

            # force retry even if task completed successfully
            q.retry(task, force=True)
            ```
        """
        return asyncio.run(self._queue.retry(task, at, force))

    def size(self) -> int:
        """Get number of pending tasks in the queue.

        Returns:
            Number of pending tasks

        Example:
            ```python
            q = TestQueue()

            q.add(task)

            count = q.size()
            assert count == 1
            ```
        """
        return asyncio.run(self._queue.size())

    def clear(self) -> int:
        """Clear all tasks, including completed ones."""
        return asyncio.run(self._queue.clear())

    def add_cron(self, task: Task, cron: str) -> bool:
        """Add a cron job to run a task on a schedule.

        Args:
            task: Instance of a Task
            cron: Cron expression string (e.g. "*/5 * * * *" to run every 5 minutes)

        Returns:
            Success boolean

        Example:
            ```python
            q = TestQueue()

            @task
            async def say_hello(to: str) -> str:
                print(f"[{datetime.now()}] Hello, {to}!")

            # schedule task to run every minute
            q.add_cron(say_hello("World"), "* * * * *")
            ```
        """
        return asyncio.run(self._queue.add_cron(task, cron))

    def delete_cron(self, task: Task, cron: str) -> bool:
        """Delete a cron job.

        Args:
            task: Instance of a Task
            cron: Cron expression string used when adding the cron job

        Returns:
            Success boolean

        Example:
            ```python
            q = TestQueue()

            # delete previously added cron job
            success = q.delete_cron(say_hello("World"), "* * * * *")
            assert success is True
            ```
        """
        return asyncio.run(self._queue.delete_cron(task, cron))

    def get_crons(self) -> list[TaskCron]:
        """List all cron jobs.

        Returns:
            List of TaskCron instances

        Example:
            ```python
            q = TestQueue()

            crons = q.get_crons()

            for cron in crons:
                print(f"Cron ID: {cron.id}, Expression: {cron.expression}, Task Spec: {cron.spec}")
            ```
        """
        return asyncio.run(self._queue.get_crons())

    def process_next(self) -> Task | None:
        """Process the next pending task in the queue.

        Returns:
            The processed Task instance, or None if no pending tasks.

        Example:
            ```python
            q = TestQueue()

            q.add(task)
            processed_task = q.process_next()
            assert processed_task is not None
            assert processed_task.completed is True
            ```
        """
        async def _process_next_async() -> Task | None:
            tasks = await self._queue.pop_pending(limit=1)
            return await self._execute_task(tasks[0]) if tasks else None

        return asyncio.run(_process_next_async())

    def process_all(self) -> list[Task]:
        """Process all pending tasks in the queue.

        Returns:
            List of processed Task instances.
        """
        processed = []

        while task := self.process_next():
            processed.append(task)

        return processed

    def process_scheduled(self, at: datetime | timedelta | None = None) -> list[Task]:
        """Process scheduled tasks that are due by the specified time.

        Args:
            at (datetime | timedelta | None): The cutoff time to process scheduled tasks.
                - If datetime is provided, tasks scheduled up to that time will be processed.
                - If timedelta is provided, it will be added to the current time to determine the cutoff time.
                - If None (default), processes tasks scheduled up to the current time.
                *Note: datetime must be offset-aware (i.e. have timezone info).*

        Returns:
            List of processed Task instances.
        """
        if isinstance(at, timedelta):
            at = datetime.now(timezone.utc) + at
        elif at is None:
            at = datetime.now(timezone.utc)

        async def _process_scheduled_async(at: datetime) -> list[Task]:
            tasks = [Task.model_validate(t) for t in await self._backend.pop_scheduled(self.name, at)]
            return [await self._execute_task(task) for task in tasks]

        return asyncio.run(_process_scheduled_async(at))

    async def _execute_task(self, __task: Task) -> Task:
        _, task = await self._task_processor.execute_task(__task, self._worker_id)

        self.processed_tasks.append(task)

        if task.error:
            self.failed_tasks.append(task)

            if task.should_retry:
                # retry immediately
                await self._queue.retry(task)

        await self._backend.store_result(self.name, task.model_dump(mode='json'))

        data = await self._backend.get_tasks(self.name, [str(task.id)])
        stored_task_data = data.get(str(task.id))

        if stored_task_data:
            return Task.model_validate(stored_task_data)

        return task


def assert_is_new(task: Task | None) -> None:
    """Assert that the task is new (not yet processed). Useful in tests.

    Args:
        task (Task|None): The task to check.

    Raises:
        AssertionError: If the task is not new.
    """
    assert task is not None
    assert isinstance(task, Task)

    assert task.completed is False
    assert task.error is None
    assert task.result is None
    assert task.finished_at is None


def assert_is_completed(task: Task | None) -> None:
    """Assert that the task is completed (processed successfully). Useful in tests.

    Args:
        task (Task|None): The task to check.

    Raises:
        AssertionError: If the task is not completed successfully.
    """
    assert task is not None
    assert isinstance(task, Task)

    assert task.completed is True
    assert task.error is None
    assert task.finished_at is not None


def assert_is_failed(task: Task | None) -> None:
    """Assert that the task has failed (processed with error). Useful in tests.

    Args:
        task (Task|None): The task to check.

    Raises:
        AssertionError: If the task has not failed.
    """
    assert task is not None
    assert isinstance(task, Task)

    assert not task.completed
    assert task.error is not None
    assert task.result is None

    if not task.should_retry:
        assert task.finished_at is not None
