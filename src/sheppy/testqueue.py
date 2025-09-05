import asyncio
from datetime import datetime, timedelta, timezone
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
        self._queue = Queue(self.name, self._backend)
        #self._dependency_resolver = DependencyResolver(dependency_overrides)
        self._worker_id = "TestQueue"
        self._task_processor = TaskProcessor()

        self.processed_tasks: list[Task] = []
        self.failed_tasks: list[Task] = []

    def add(self, task: Task | list[Task]) -> bool:
        return asyncio.run(self._queue.add(task))

    def get_task(self, task_id: UUID) -> Task | None:
        return asyncio.run(self._queue.get_task(task_id))

    def peek(self, count: int = 1) -> list[Task]:
        return asyncio.run(self._queue.peek(count))

    def size(self) -> int:
        return asyncio.run(self._queue.size())

    def clear(self) -> int:
        return asyncio.run(self._queue.clear())

    def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        return asyncio.run(self._queue.schedule(task, at))

    def add_cron(self, task: Task, cron: str) -> bool:
        return asyncio.run(self._queue.add_cron(task, cron))

    def delete_cron(self, task: Task, cron: str) -> bool:
        return asyncio.run(self._queue.delete_cron(task, cron))

    def list_crons(self) -> list[TaskCron]:
        return asyncio.run(self._queue.list_crons())

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

    async def _process_next_async(self) -> Task | None:  # ! FIXME - messy
        tasks = await self._queue._pop(limit=1)

        if not tasks:
            return None

        __task = tasks[0]

        _, task = await self._task_processor.execute_task(__task, self._worker_id)

        self.processed_tasks.append(task)

        if task.error:
            self.failed_tasks.append(task)
            # Final failure
            if not task.finished_at:
                # For tests, override next_retry_at to be immediate (ignore delay)
                task.__dict__["next_retry_at"] = datetime.now(timezone.utc)

                # Requeue for immediate retry
                await self._queue.add(task)

        await self._backend.store_result(self.name, task.model_dump(mode='json'))

        stored_task_data = await self._backend.get_task(self.name, str(task.id))
        if stored_task_data:
            return Task.model_validate(stored_task_data)
        return task

    async def _process_scheduled_async(self, at: datetime) -> list[Task]:  # ! FIXME - messy
        processed = []

        # Get all scheduled tasks up to specified time
        tasks = [Task.model_validate(data) for data in await self._backend.get_scheduled(self.name, at)]

        for __task in tasks:

            _, task = await self._task_processor.execute_task(__task, self._worker_id)

            self.processed_tasks.append(task)
            processed.append(task)

            if task.error:
                self.failed_tasks.append(task)

                # Override next_retry_at to be immediate (ignore delay)  # ! FIXME
                task.__dict__["next_retry_at"] = datetime.now(timezone.utc)

                # Requeue for immediate retry
                await self._queue.add(task)

            # Store result
            await self._backend.store_result(self.name, task.model_dump(mode='json'))

        return processed
