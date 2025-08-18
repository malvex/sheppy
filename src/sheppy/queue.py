from datetime import datetime, timedelta, timezone
from uuid import UUID

from .backend.base import Backend
from .task import Task


class Queue:

    def __init__(self, name: str, backend: Backend):
        self.name = name
        self.backend = backend

    async def _ensure_backend_is_connected(self) -> None:
        """Automatically connects backend on first async call."""
        if not self.backend.is_connected:
            await self.backend.connect()

    async def add(self, task: Task) -> bool:
        await self._ensure_backend_is_connected()
        # Store queue reference in the task for wait_for_result
        task.internal.queue = self
        return await self.backend.append(self.name, task.model_dump(mode='json'))

    async def pop(self, timeout: float | None = None) -> Task | None:
        await self._ensure_backend_is_connected()
        task_data = await self.backend.pop(self.name, timeout)

        return Task.model_validate(task_data) if task_data else None

    async def peek(self, count: int = 1) -> list[Task]:
        await self._ensure_backend_is_connected()
        return [Task.model_validate(task_data) for task_data in await self.backend.peek(self.name, count)]

    async def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        await self._ensure_backend_is_connected()
        # Store queue reference in the task for wait_for_result (unlikely to be used with schedule)
        task.internal.queue = self

        # Convert timedelta to datetime
        if isinstance(at, timedelta):
            at = datetime.now(timezone.utc) + at

        return await self.backend.schedule(self.name, task.model_dump(mode='json'), at)

    async def get_scheduled(self, now: datetime | None = None) -> list[Task]:
        await self._ensure_backend_is_connected()
        return [Task.model_validate(task_data) for task_data in await self.backend.get_scheduled(self.name, now)]

    async def acknowledge(self, task_id: UUID) -> bool:
        await self._ensure_backend_is_connected()
        return await self.backend.acknowledge(self.name, str(task_id))

    async def size(self) -> int:
        await self._ensure_backend_is_connected()
        return await self.backend.size(self.name)

    async def clear(self) -> int:
        await self._ensure_backend_is_connected()
        return await self.backend.clear(self.name)

    async def get_task(self, task_id: UUID) -> Task | None:
        await self._ensure_backend_is_connected()
        task_data = await self.backend.get_task(self.name, str(task_id))

        return Task.model_validate(task_data) if task_data else None
