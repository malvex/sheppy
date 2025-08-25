import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from .backend.memory import MemoryBackend
from .queue import Queue
from .task import Task
from .utils.dependency_injection import DependencyResolver
from .utils.task_execution import execute_task, get_available_tasks, update_failed_task


class TestQueue:
    __test__ = False

    def __init__(
        self,
        name: str = "test-queue",
        dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None
    ):
        self.name = name

        self._backend = MemoryBackend()
        self._queue = Queue(self.name, self._backend)
        self._dependency_resolver = DependencyResolver(dependency_overrides)
        self._worker_id = "TestQueue"

        self.processed_tasks: list[Task] = []
        self.failed_tasks: list[Task] = []

    def add(self, task: Task) -> bool:
        return asyncio.run(self._queue.add(task))

    def peek(self, count: int = 1) -> list[Task]:
        return asyncio.run(self._queue.peek(count))

    def size(self) -> int:
        return asyncio.run(self._queue.size())

    def clear(self) -> int:
        return asyncio.run(self._queue.clear())

    def get_task(self, task_id: UUID) -> Task | None:
        return asyncio.run(self._queue.get_task(task_id))

    def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        return asyncio.run(self._queue.schedule(task, at))

    def wait_for_finished(self, task: Task, timeout: float = 0, poll_interval: float = 0.01) -> Any:
        return asyncio.run(self._queue.wait_for_finished(task, timeout, poll_interval))

    def process_next(self) -> Task | None:
        return asyncio.run(self._process_next_async())

    def process_all(self) -> list[Task]:
        processed = []

        while self.size() > 0:
            task = self.process_next()
            if task:
                processed.append(task)
            else:
                break
        return processed

    def process_scheduled(self, at: datetime | timedelta | None = None) -> list[Task]:
        # Convert timedelta to datetime if needed
        if isinstance(at, timedelta):
            at = datetime.now(timezone.utc) + at
        elif at is None:
            at = datetime.now(timezone.utc)

        # Run async processing
        return asyncio.run(self._process_scheduled_async(at))

    async def _process_next_async(self) -> Task | None:
        # Get next task
        tasks = await get_available_tasks(self._queue, limit=1)

        if not tasks:
            return None

        task = tasks[0]

        # Process the task
        try:
            task = await execute_task(task, self._dependency_resolver, self._worker_id)
            self.processed_tasks.append(task)
        except Exception as e:
            # Handle failure
            task = update_failed_task(task, e)

            # Final failure
            if task.metadata.finished_datetime:
                self.failed_tasks.append(task)
                self.processed_tasks.append(task)
            else:
                # For tests, override next_retry_at to be immediate (ignore delay)
                task.metadata.__dict__["next_retry_at"] = datetime.now(timezone.utc)

                # Requeue for immediate retry
                await self._queue.add(task)

        # Store result (whether success or failure)
        await self._backend.store_result(self.name, task.model_dump(mode='json'))

        # Acknowledge task
        await self._queue.acknowledge(task.id)

        # Fetch the task back from backend to simulate serialization/deserialization
        # This ensures Pydantic models in results are converted to dicts
        stored_task_data = await self._backend.get_task(self.name, str(task.id))
        if stored_task_data:
            return Task.model_validate(stored_task_data)
        return task

    async def _process_scheduled_async(self, at: datetime) -> list[Task]:
        processed = []

        # Get all scheduled tasks up to specified time
        tasks = [Task.model_validate(data) for data in await self._backend.get_scheduled(self.name, at)]

        for task in tasks:
            try:
                task = await execute_task(task, self._dependency_resolver, self._worker_id)
                self.processed_tasks.append(task)
                processed.append(task)
            except Exception as e:
                # Handle failure
                task = update_failed_task(task, e)

                # Final failure
                if task.metadata.finished_datetime:
                    self.failed_tasks.append(task)
                    raise

                # Override next_retry_at to be immediate (ignore delay)
                task.metadata.__dict__["next_retry_at"] = datetime.now(timezone.utc)

                # Requeue for immediate retry
                await self._queue.add(task)

            # Store result
            await self._backend.store_result(self.name, task.model_dump(mode='json'))

        return processed
