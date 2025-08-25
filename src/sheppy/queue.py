from datetime import datetime, timedelta, timezone
from uuid import UUID

from .backend.base import Backend
from .task import Task


class Queue:

    def __init__(self, name: str, backend: Backend):
        self.name = name
        self.backend = backend

    async def add(self, task: Task) -> bool:
        """Add task into the queue."""
        await self._ensure_backend_is_connected()

        return await self.backend.append(self.name, task.model_dump(mode='json'))

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

        # Convert timedelta to datetime
        if isinstance(at, timedelta):
            at = datetime.now(timezone.utc) + at

        return await self.backend.schedule(self.name, task.model_dump(mode='json'), at)

    async def get_scheduled(self, now: datetime | None = None) -> list[Task]:
        await self._ensure_backend_is_connected()

        return [Task.model_validate(t) for t in await self.backend.get_scheduled(self.name, now)]

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

    async def _pop(self, timeout: float | None = None) -> Task | None:
        """Get next task to process. Used by workers."""
        await self._ensure_backend_is_connected()
        task_data = await self.backend.pop(self.name, timeout)
        return Task.model_validate(task_data) if task_data else None

    async def _acknowledge(self, task_id: UUID) -> bool:
        """Used by workers."""
        await self._ensure_backend_is_connected()
        return await self.backend.acknowledge(self.name, str(task_id))
