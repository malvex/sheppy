from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from .backend.base import Backend
from .task import Task, TaskCron, TaskFactory


class Queue:

    def __init__(self, name: str, backend: Backend):
        self.name = name
        self.backend = backend

    async def add(self, task: Task | list[Task]) -> bool:
        """Add task into the queue. Accept list of tasks for batch add."""
        await self._ensure_backend_is_connected()

        if isinstance(task, list):
            data: list[dict[str, Any]] | dict[str, Any] = [t.model_dump(mode='json') for t in task]
        else:
            data = task.model_dump(mode='json')

        return await self.backend.append(self.name, data)

    async def get_task(self, task: Task | UUID) -> Task | None:
        task_id = task.id if isinstance(task, Task) else task

        await self._ensure_backend_is_connected()
        task_data = await self.backend.get_task(self.name, str(task_id))

        return Task.model_validate(task_data) if task_data else None

    async def refresh(self, task: Task | UUID) -> Task | None:  # ! FIXME
        return await self.get_task(task)

    async def peek(self, count: int = 1) -> list[Task]:
        """Peek into the queue without removing tasks from it."""
        await self._ensure_backend_is_connected()

        return [Task.model_validate(t) for t in await self.backend.peek(self.name, count)]

    async def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        """Schedule task to be processed after certain time."""
        await self._ensure_backend_is_connected()

        if isinstance(at, timedelta):
            at = datetime.now(timezone.utc) + at

        return await self.backend.schedule(self.name, task.model_dump(mode='json'), at)

    async def enqueue_scheduled(self, now: datetime | None = None) -> list[Task]:
        """Enqueue scheduled tasks that are ready to be processed."""
        await self._ensure_backend_is_connected()

        tasks = [Task.model_validate(t) for t in await self.backend.get_scheduled(self.name, now)]

        for task in tasks:
            await self.add(task)

        return tasks

    async def list_scheduled(self) -> list[tuple[datetime, Task]]:
        """List scheduled tasks."""
        await self._ensure_backend_is_connected()

        scheduled_tasks = []
        for task_data in await self.backend.list_scheduled(self.name):
            scheduled_at = task_data.pop("_scheduled_at")
            scheduled_tasks.append((scheduled_at, Task.model_validate(task_data)))

        return scheduled_tasks

    async def wait_for_result(self, task: Task | UUID, timeout: float = 0) -> Task | None:
        await self._ensure_backend_is_connected()

        task_id = task.id if isinstance(task, Task) else task
        task_data = await self.backend.get_result(self.name, str(task_id), timeout)

        return Task.model_validate(task_data) if task_data else None

    async def size(self) -> int:
        """Get number of pending tasks in the queue."""
        await self._ensure_backend_is_connected()
        return await self.backend.size(self.name)

    async def clear(self) -> int:
        """Clear all pending tasks."""
        await self._ensure_backend_is_connected()
        return await self.backend.clear(self.name)

    async def _ensure_backend_is_connected(self) -> None:
        """Automatically connects backend on first async call."""
        if not self.backend.is_connected:
            await self.backend.connect()

    async def _pop(self, limit: int = 1, timeout: float | None = None) -> list[Task]:
        """Get next task to process. Used by workers."""
        await self._ensure_backend_is_connected()

        if limit <= 0:
            raise ValueError("Pop limit must be greater than zero.")

        tasks_data = await self.backend.pop(self.name, limit, timeout)
        return [Task.model_validate(t) for t in tasks_data]

    async def get_all_tasks(self) -> list[Task]:
        """Get all tasks, including completed/failed ones."""
        await self._ensure_backend_is_connected()
        tasks_data = await self.backend.get_all_tasks(self.name)
        return [Task.model_validate(t) for t in tasks_data]

    async def add_cron(self, task: Task, cron: str) -> bool:
        await self._ensure_backend_is_connected()
        task_cron = TaskFactory.create_cron_from_task(task, cron, self.name)
        return await self.backend.add_cron(self.name, task_cron.model_dump(mode="json"))

    async def delete_cron(self, task: Task, cron: str) -> bool:
        await self._ensure_backend_is_connected()
        task_cron = TaskFactory.create_cron_from_task(task, cron, self.name)
        return await self.backend.delete_cron(self.name, str(task_cron.id))

    async def list_crons(self) -> list[TaskCron]:
        await self._ensure_backend_is_connected()
        return [TaskCron.model_validate(tc) for tc in await self.backend.list_crons(self.name)]
