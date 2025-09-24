import asyncio
from datetime import datetime, timedelta, timezone
from typing import overload
from uuid import UUID

from .backend.memory import MemoryBackend
from .models import Task, TaskCron
from .queue import Queue
from .utils.task_execution import TaskProcessor


class TestQueue:
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
        """Add task into the queue. Accept list of tasks for batch add."""
        return asyncio.run(self._queue.add(task))  # type: ignore[return-value]

    @overload
    def get_task(self, task: Task | UUID) -> Task | None: ...

    @overload
    def get_task(self, task: list[Task | UUID]) -> dict[UUID, Task]: ...

    def get_task(self, task: Task | UUID | list[Task | UUID]) -> Task | None | dict[UUID, Task]:
        return asyncio.run(self._queue.get_task(task))

    def get_all_tasks(self) -> list[Task]:
        """Get all tasks, including completed/failed ones."""
        return asyncio.run(self._queue.get_all_tasks())

    def get_pending(self, count: int = 1) -> list[Task]:
        """List pending tasks."""
        return asyncio.run(self._queue.get_pending(count))

    def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        """Schedule task to be processed after certain time."""
        return asyncio.run(self._queue.schedule(task, at))

    def get_scheduled(self) -> list[Task]:
        """List scheduled tasks."""
        return asyncio.run(self._queue.get_scheduled())

    def retry(self, task: Task | UUID, at: datetime | timedelta | None = None, force: bool = False) -> bool:
        """Retry failed task."""
        return asyncio.run(self._queue.retry(task, at, force))

    def size(self) -> int:
        """Get number of pending tasks in the queue."""
        return asyncio.run(self._queue.size())

    def clear(self) -> int:
        """Clear all tasks, including completed ones."""
        return asyncio.run(self._queue.clear())

    def add_cron(self, task: Task, cron: str) -> bool:
        return asyncio.run(self._queue.add_cron(task, cron))

    def delete_cron(self, task: Task, cron: str) -> bool:
        return asyncio.run(self._queue.delete_cron(task, cron))

    def get_crons(self) -> list[TaskCron]:
        return asyncio.run(self._queue.get_crons())

    def process_next(self) -> Task | None:

        async def _process_next_async() -> Task | None:
            tasks = await self._queue.pop_pending(limit=1)
            return await self._execute_task(tasks[0]) if tasks else None

        return asyncio.run(_process_next_async())

    def process_all(self) -> list[Task]:
        processed = []

        while task := self.process_next():
            processed.append(task)

        return processed

    def process_scheduled(self, at: datetime | timedelta | None = None) -> list[Task]:
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
    assert task is not None
    assert isinstance(task, Task)

    assert task.completed is False
    assert task.error is None
    assert task.result is None
    assert task.finished_at is None


def assert_is_completed(task: Task | None) -> None:
    assert task is not None
    assert isinstance(task, Task)

    assert task.completed is True
    assert task.error is None
    assert task.finished_at is not None


def assert_is_failed(task: Task | None) -> None:
    assert task is not None
    assert isinstance(task, Task)

    assert not task.completed
    assert task.error is not None
    assert task.result is None

    if not task.should_retry:
        assert task.finished_at is not None
