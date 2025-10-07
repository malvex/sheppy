from datetime import datetime, timedelta, timezone
from typing import overload
from uuid import UUID

from .backend.base import Backend
from .models import Task, TaskCron
from .task_factory import TaskFactory


class Queue:
    """
    `Queue` class provides an easy way to manage task queue.

    Parameters:
        backend (sheppy.Backend): An instance of task backend (e.g. `sheppy.RedisBackend`)
        name (str): Name of the queue
    """

    def __init__(self, backend: Backend, name: str = "default"):
        self.name = name
        self.backend = backend

    @overload
    async def add(self, task: Task) -> bool: ...

    @overload
    async def add(self, task: list[Task]) -> list[bool]: ...

    async def add(self, task: Task | list[Task]) -> bool | list[bool]:
        """
        Add task into the queue. Accept list of tasks for batch add.

        Args:
            task: Instance of a Task, or list of Task instances for batch mode.

        Returns:
            Success boolean, or list of booleans in batch mode.

        Example:
            ```python
            q = Queue(...)
            success = await q.add(task)
            assert success is True

            # batch mode
            success = await q.add([task1, task2])
            assert success == [True, True]  # returns list of booleans in batch mode
            ```
        """
        await self.__ensure_backend_is_connected()

        if isinstance(task, list):
            batch_mode = True
            tasks = [t.model_dump(mode='json') for t in task]
        else:
            batch_mode = False
            tasks = [task.model_dump(mode='json')]

        success = await self.backend.append(self.name, tasks)

        return success if batch_mode else success[0]

    @overload
    async def get_task(self, task: Task | UUID | str) -> Task | None: ...

    @overload
    async def get_task(self, task: list[Task | UUID | str]) -> dict[UUID, Task]: ...

    async def get_task(self, task: Task | UUID | str | list[Task | UUID | str]) -> dict[UUID, Task] | Task | None:
        """Get task by id.

        Args:
            task: Instance of a Task or its ID, or list of Task instances/IDs for batch mode.

        Returns:
            (Task|None): Instance of a Task or None if not found.
            (dict[UUID, Task]): *(In batch mode)* Returns Dictionary of Task IDs to Task instances.
        """
        await self.__ensure_backend_is_connected()

        task_ids, batch_mode = self._get_task_ids(task)
        task_results = await self.backend.get_tasks(self.name, task_ids)

        if batch_mode:
            return {UUID(t_id): Task.model_validate(t) for t_id, t in task_results.items()}

        td = task_results.get(task_ids[0])

        return Task.model_validate(td) if td else None

    async def get_all_tasks(self) -> list[Task]:
        """Get all tasks, including completed/failed ones.

        Returns:
            List of all tasks
        """
        await self.__ensure_backend_is_connected()
        tasks_data = await self.backend.get_all_tasks(self.name)
        return [Task.model_validate(t) for t in tasks_data]

    async def get_pending(self, count: int = 1) -> list[Task]:
        """List pending tasks.

        Args:
            count: Number of pending tasks to retrieve.

        Returns:
            List of pending tasks
        """
        if count <= 0:
            raise ValueError("Value must be larger than zero")

        await self.__ensure_backend_is_connected()

        return [Task.model_validate(t) for t in await self.backend.get_pending(self.name, count)]

    async def schedule(self, task: Task, at: datetime | timedelta) -> bool:
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
            from datetime import datetime, timedelta

            q = Queue(...)
            # schedule task to be processed after 10 minutes
            await q.schedule(task, timedelta(minutes=10))

            # ... or at specific time
            await q.schedule(task, datetime.fromisoformat("2026-01-01 00:00:00 +00:00"))
            ```
        """
        await self.__ensure_backend_is_connected()

        if isinstance(at, timedelta):
            at = datetime.now(timezone.utc) + at

        if not at.tzinfo:
            raise TypeError("provided datetime must be offset-aware")

        task.__dict__["scheduled_at"] = at

        return await self.backend.schedule(self.name, task.model_dump(mode="json"), at)

    async def get_scheduled(self) -> list[Task]:
        """List scheduled tasks.

        Returns:
            List of scheduled tasks
        """
        await self.__ensure_backend_is_connected()
        return [Task.model_validate(t) for t in await self.backend.get_scheduled(self.name)]

    @overload
    async def wait_for(self, task: Task | UUID | str, timeout: float = 0) -> Task | None: ...

    @overload
    async def wait_for(self, task: list[Task | UUID | str], timeout: float = 0) -> dict[UUID, Task]: ...

    async def wait_for(self, task: Task | UUID | str | list[Task | UUID | str], timeout: float = 0) -> dict[UUID, Task] | Task | None:
        """Wait for task to complete and return updated task instance.

        Args:
            task: Instance of a Task or its ID, or list of Task instances/IDs for batch mode.
            timeout: Maximum time to wait in seconds. Default is 0 (wait indefinitely).<br>
                     If timeout is reached, returns None (or partial results in batch mode).<br>
                     In batch mode, this is the maximum time to wait for all tasks to complete.<br>
                     Note: In non-batch mode, if timeout is reached and no task is found, a TimeoutError is raised.

        Returns:
            Instance of a Task or None if not found or timeout reached.<br>In batch mode, returns dictionary of Task IDs to Task instances (partial results possible on timeout).

        Raises:
            TimeoutError: If timeout is reached and no task is found (only in non-batch mode).

        Example:
            ```python
            q = Queue(...)

            # wait indefinitely for task to complete
            updated_task = await q.wait_for(task)
            assert updated_task.completed is True

            # wait up to 5 seconds for task to complete
            try:
                updated_task = await q.wait_for(task, timeout=5)
                if updated_task:
                    assert updated_task.completed is True
                else:
                    print("Task not found or still pending after timeout")
            except TimeoutError:
                print("Task did not complete within timeout")

            # batch mode
            updated_tasks = await q.wait_for([task1, task2, task3], timeout=10)

            for task_id, task in updated_tasks.items():
                print(f"Task {task_id} completed: {task.completed}")

            # Note: updated_tasks may contain only a subset of tasks if timeout is reached
            ```
        """
        await self.__ensure_backend_is_connected()

        task_ids, batch_mode = self._get_task_ids(task)
        task_results = await self.backend.get_results(self.name, task_ids, timeout)

        if batch_mode:
            return {UUID(t_id): Task.model_validate(t) for t_id, t in task_results.items()}

        td = task_results.get(task_ids[0])

        return Task.model_validate(td) if td else None

    async def retry(self, task: Task | UUID | str, at: datetime | timedelta | None = None, force: bool = False) -> bool:
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
            q = Queue(...)

            # retry task immediately
            success = await q.retry(task)
            assert success is True

            # or retry after 5 minutes
            await q.retry(task, at=timedelta(minutes=5))

            # or at specific time
            await q.retry(task, at=datetime.fromisoformat("2026-01-01 00:00:00 +00:00"))

            # force retry even if task is completed (= finished successfully)
            await q.retry(task, force=True)
            ```
        """
        _task = await self.get_task(task)  # ensure_backend_is_connected is called in get_task already
        if not _task:
            return False

        if not force and _task.completed:
            raise ValueError("Task has already completed successfully, use force to retry anyways")

        needs_update = False  # temp hack
        if _task.finished_at:
            needs_update = True
            _task.__dict__["last_retry_at"] = datetime.now(timezone.utc)
            _task.__dict__["next_retry_at"] = datetime.now(timezone.utc)
            _task.__dict__["finished_at"] = None

        if at:
            if isinstance(at, timedelta):
                at = datetime.now(timezone.utc) + at

            if not at.tzinfo:
                raise TypeError("provided datetime must be offset-aware")

            if needs_update:
                _task.__dict__["next_retry_at"] = at
                _task.__dict__["scheduled_at"] = at

            return await self.backend.schedule(self.name, _task.model_dump(mode="json"), at, unique=False)

        success = await self.backend.append(self.name, [_task.model_dump(mode="json")], unique=False)
        return success[0]

    async def size(self) -> int:
        """Get number of pending tasks in the queue.

        Returns:
            Number of pending tasks

        Example:
            ```python
            q = Queue(...)

            await q.add(task)

            count = await q.size()
            assert count == 1
            ```
        """
        await self.__ensure_backend_is_connected()
        return await self.backend.size(self.name)

    async def clear(self) -> int:
        """Clear all tasks, including completed ones."""
        await self.__ensure_backend_is_connected()
        return await self.backend.clear(self.name)

    async def add_cron(self, task: Task, cron: str) -> bool:
        """Add a cron job to run a task on a schedule.

        Args:
            task: Instance of a Task
            cron: Cron expression string (e.g. "*/5 * * * *" to run every 5 minutes)

        Returns:
            Success boolean

        Example:
            ```python
            q = Queue(...)

            @task
            async def say_hello(to: str) -> str:
                print(f"[{datetime.now()}] Hello, {to}!")

            # schedule task to run every minute
            await q.add_cron(say_hello("World"), "* * * * *")
            ```
        """
        await self.__ensure_backend_is_connected()
        task_cron = TaskFactory.create_cron_from_task(task, cron)
        return await self.backend.add_cron(self.name, str(task_cron.deterministic_id), task_cron.model_dump(mode="json"))

    async def delete_cron(self, task: Task, cron: str) -> bool:
        """Delete a cron job.

        Args:
            task: Instance of a Task
            cron: Cron expression string used when adding the cron job

        Returns:
            Success boolean

        Example:
            ```python
            q = Queue(...)

            # delete previously added cron job
            success = await q.delete_cron(say_hello("World"), "* * * * *")
            assert success is True
            ```
        """
        await self.__ensure_backend_is_connected()
        task_cron = TaskFactory.create_cron_from_task(task, cron)
        return await self.backend.delete_cron(self.name, str(task_cron.deterministic_id))

    async def get_crons(self) -> list[TaskCron]:
        """List all cron jobs.

        Returns:
            List of TaskCron instances

        Example:
            ```python
            q = Queue(...)

            crons = await q.get_crons()

            for cron in crons:
                print(f"Cron ID: {cron.id}, Expression: {cron.expression}, Task Spec: {cron.spec}")
            ```
        """
        await self.__ensure_backend_is_connected()
        return [TaskCron.model_validate(tc) for tc in await self.backend.get_crons(self.name)]

    async def pop_pending(self, limit: int = 1, timeout: float | None = None) -> list[Task]:
        """Get next task to process. Used internally by workers.

        Args:
            limit: Maximum number of tasks to return
            timeout: Timeout for waiting for tasks

        Returns:
            List of popped tasks
        """
        await self.__ensure_backend_is_connected()

        if limit <= 0:
            raise ValueError("Pop limit must be greater than zero.")

        tasks_data = await self.backend.pop(self.name, limit, timeout)
        return [Task.model_validate(t) for t in tasks_data]

    async def enqueue_scheduled(self, now: datetime | None = None) -> list[Task]:
        """Enqueue scheduled tasks that are ready to be processed. Used internally by workers.

        Args:
            now: Current time for scheduling

        Returns:
            List of enqueued tasks
        """
        await self.__ensure_backend_is_connected()

        tasks_data = await self.backend.pop_scheduled(self.name, now)
        tasks = [Task.model_validate(t) for t in tasks_data]
        await self.backend.append(self.name, tasks_data, unique=False)

        return tasks

    async def __ensure_backend_is_connected(self) -> None:
        """Automatically connects backend on first async call."""
        if not self.backend.is_connected:
            await self.backend.connect()

    def _get_task_ids(self, task: list[Task | UUID | str] | Task | UUID | str) -> tuple[list[str], bool]:
        batch_mode = True
        if not isinstance(task, list):
            task = [task]
            batch_mode = False

        # set to deduplicate task ids
        task_ids = list({str(t.id if isinstance(t, Task) else t) for t in task})

        return task_ids, batch_mode
