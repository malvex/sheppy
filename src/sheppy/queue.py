import asyncio
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
        """Add task into the queue."""
        await self._ensure_backend_is_connected()

        return await self.backend.append(self.name, task.model_dump(mode='json'))

    async def pop(self, timeout: float | None = None) -> Task | None:
        """Get next task from the queue for processing. Used by workers."""
        await self._ensure_backend_is_connected()

        task_data = await self.backend.pop(self.name, timeout)

        if not task_data:
            return None

        return Task.model_validate(task_data)

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

    async def refresh(self, task: Task | UUID) -> Task | None:  # ! FIXME
        return await self.get_task(task)

    async def wait_for_finished(self, task: Task | UUID, timeout: float = 0, poll_interval: float = 0.1) -> Task | None:  # ! FIXME
        task_id = task.id if isinstance(task, Task) else task

        start_time = asyncio.get_event_loop().time()

        while True:
            _task = await self.get_task(task_id)

            # Check if task is finished (either completed successfully or failed)
            if _task and _task.metadata.finished_datetime is not None:
                if _task.error:
                    raise ValueError(f"Task failed: {_task.error}")
                return _task

            if timeout > 0:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")

            await asyncio.sleep(poll_interval)  # ! FIXME - remove polling

    async def acknowledge(self, task_id: UUID) -> bool:
        await self._ensure_backend_is_connected()
        return await self.backend.acknowledge(self.name, str(task_id))

    async def size(self) -> int:
        """Get number of pending tasks in the queue."""
        await self._ensure_backend_is_connected()
        return await self.backend.size(self.name)

    async def clear(self) -> int:
        """Clear all pending tasks."""
        await self._ensure_backend_is_connected()
        return await self.backend.clear(self.name)

    async def get_task(self, task: Task | UUID) -> Task | None:
        task_id = task.id if isinstance(task, Task) else task

        await self._ensure_backend_is_connected()
        task_data = await self.backend.get_task(self.name, str(task_id))

        return Task.model_validate(task_data) if task_data else None
