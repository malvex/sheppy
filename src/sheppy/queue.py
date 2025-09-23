from datetime import datetime, timedelta, timezone
from uuid import UUID

from .backend.base import Backend
from .models import Task, TaskCron
from .task_factory import TaskFactory


class Queue:

    def __init__(self, backend: Backend, name: str = "default"):
        self.name = name
        self.backend = backend

    async def add(self, task: Task | list[Task]) -> list[bool]:
        """Add task into the queue. Accept list of tasks for batch add."""
        await self.__ensure_backend_is_connected()

        if isinstance(task, list):
            tasks = [t.model_dump(mode='json') for t in task]
        else:
            tasks = [task.model_dump(mode='json')]

        success = await self.backend.create_tasks(self.name, tasks)

        to_queue = []
        for i, t in enumerate(tasks):
            if success[i]:
                to_queue.append(t)

        await self.backend.append(self.name, to_queue)

        return success

    async def get_task(self, task: Task | UUID) -> Task | None:
        task_id = task.id if isinstance(task, Task) else task

        await self.__ensure_backend_is_connected()
        task_data = await self.backend.get_task(self.name, str(task_id))

        return Task.model_validate(task_data) if task_data else None

    async def get_all_tasks(self) -> list[Task]:
        """Get all tasks, including completed/failed ones."""
        await self.__ensure_backend_is_connected()
        tasks_data = await self.backend.get_all_tasks(self.name)
        return [Task.model_validate(t) for t in tasks_data]

    async def get_pending(self, count: int = 1) -> list[Task]:
        """List pending tasks."""

        if count <= 0:
            raise ValueError("Value must be larger than zero")

        await self.__ensure_backend_is_connected()

        return [Task.model_validate(t) for t in await self.backend.get_pending(self.name, count)]

    async def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        """Schedule task to be processed after certain time."""
        await self.__ensure_backend_is_connected()

        if isinstance(at, timedelta):
            at = datetime.now(timezone.utc) + at

        if not at.tzinfo:
            raise TypeError("provided datetime must be offset-aware")

        task.__dict__["scheduled_at"] = at
        task_data = task.model_dump(mode="json")

        success = await self.backend.create_tasks(self.name, [task_data])
        if not success[0]:
            return False

        return await self.backend.schedule(self.name, task_data, at)

    async def get_scheduled(self) -> list[Task]:
        """List scheduled tasks."""
        await self.__ensure_backend_is_connected()
        return [Task.model_validate(t) for t in await self.backend.get_scheduled(self.name)]

    async def wait_for(self, task: Task | UUID, timeout: float = 0) -> Task | None:
        await self.__ensure_backend_is_connected()

        task_id = task.id if isinstance(task, Task) else task
        task_data = await self.backend.get_result(self.name, str(task_id), timeout)

        return Task.model_validate(task_data) if task_data else None

    async def retry(self, task: Task | UUID, at: datetime | timedelta | None = None, force: bool = False) -> bool:
        """Retry failed task."""
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

            return await self.backend.schedule(self.name, _task.model_dump(mode="json"), at)

        return await self.backend.append(self.name, [_task.model_dump(mode="json")])

    async def size(self) -> int:
        """Get number of pending tasks in the queue."""
        await self.__ensure_backend_is_connected()
        return await self.backend.size(self.name)

    async def clear(self) -> int:
        """Clear all tasks, including completed ones."""
        await self.__ensure_backend_is_connected()
        return await self.backend.clear(self.name)

    async def add_cron(self, task: Task, cron: str) -> bool:
        await self.__ensure_backend_is_connected()
        task_cron = TaskFactory.create_cron_from_task(task, cron)
        return await self.backend.add_cron(self.name, str(task_cron.deterministic_id), task_cron.model_dump(mode="json"))

    async def delete_cron(self, task: Task, cron: str) -> bool:
        await self.__ensure_backend_is_connected()
        task_cron = TaskFactory.create_cron_from_task(task, cron)
        return await self.backend.delete_cron(self.name, str(task_cron.deterministic_id))

    async def get_crons(self) -> list[TaskCron]:
        await self.__ensure_backend_is_connected()
        return [TaskCron.model_validate(tc) for tc in await self.backend.get_crons(self.name)]

    async def pop_pending(self, limit: int = 1, timeout: float | None = None) -> list[Task]:
        """Get next task to process. Used internally by workers."""
        await self.__ensure_backend_is_connected()

        if limit <= 0:
            raise ValueError("Pop limit must be greater than zero.")

        tasks_data = await self.backend.pop(self.name, limit, timeout)
        return [Task.model_validate(t) for t in tasks_data]

    async def enqueue_scheduled(self, now: datetime | None = None) -> list[Task]:
        """Enqueue scheduled tasks that are ready to be processed. Used internally by workers."""
        await self.__ensure_backend_is_connected()

        tasks_data = await self.backend.pop_scheduled(self.name, now)
        tasks = [Task.model_validate(t) for t in tasks_data]
        await self.backend.append(self.name, tasks_data)

        return tasks

    async def __ensure_backend_is_connected(self) -> None:
        """Automatically connects backend on first async call."""
        if not self.backend.is_connected:
            await self.backend.connect()
