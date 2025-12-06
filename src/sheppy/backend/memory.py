import asyncio
import heapq
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..models import Task
from ..utils.task_execution import TaskProcessor
from .base import Backend, BackendError


@dataclass(order=True)
class ScheduledTask:
    scheduled_time: datetime
    task_id: str = field(compare=False)


class MemoryBackend(Backend):

    def __init__(self,
                 *,
                 instant_processing: bool = True,
                 dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None,
                 ) -> None:

        self._task_metadata: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)  # {QUEUE_NAME: {TASK_ID: task_data}}
        self._pending: dict[str, list[str]] = defaultdict(list)
        self._scheduled: dict[str, list[ScheduledTask]] = defaultdict(list)
        self._crons: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)  # for thread-safety
        self._connected = False

        self._instant_processing = instant_processing
        self._task_processor = TaskProcessor(dependency_overrides=dependency_overrides)
        self._worker_id = "MemoryBackend"

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def _create_tasks(self, queue_name: str, tasks: list[dict[str, Any]]) -> list[bool]:
        self._check_connected()

        async with self._locks[queue_name]:
            success = []
            for task in tasks:
                if task["id"] not in self._task_metadata[queue_name]:
                    self._task_metadata[queue_name][task["id"]] = task
                    success.append(True)
                else:
                    success.append(False)

            return success

    async def append(self, queue_name: str, tasks: list[dict[str, Any]], unique: bool = True) -> list[bool]:
        self._check_connected()

        if unique:
            success = await self._create_tasks(queue_name, tasks)
            to_queue = [t for i, t in enumerate(tasks) if success[i]]
        else:
            success = [True] * len(tasks)
            to_queue = tasks

        async with self._locks[queue_name]:
            for task in to_queue:
                if not unique:
                    self._task_metadata[queue_name][task["id"]] = task

                self._pending[queue_name].append(task["id"])

        if self._instant_processing:
            for i, task_data in enumerate(tasks):
                if success[i]:
                    await self._process_task(queue_name, task_data)

        return success

    async def pop(self, queue_name: str, limit: int = 1, timeout: float | None = None) -> list[dict[str, Any]]:
        self._check_connected()

        start_time = asyncio.get_event_loop().time()

        while True:
            async with self._locks[queue_name]:
                if self._pending[queue_name]:
                    tasks = []
                    q = self._pending[queue_name]

                    for _ in range(min(limit, len(q))):
                        task_id = q.pop(0)
                        task_data = self._task_metadata[queue_name].get(task_id)
                        if task_data:
                            tasks.append(task_data)

                    return tasks

            if timeout is None or timeout <= 0:
                return []

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                return []

            await asyncio.sleep(min(0.05, timeout - elapsed))

    async def get_pending(self, queue_name: str, count: int = 1) -> list[dict[str, Any]]:
        self._check_connected()

        async with self._locks[queue_name]:
            task_ids = list(self._pending[queue_name])[:count]

            tasks = []
            for t in task_ids:
                if task_data := self._task_metadata[queue_name].get(t):
                    tasks.append(task_data)

            return tasks


    async def size(self, queue_name: str) -> int:
        self._check_connected()

        async with self._locks[queue_name]:
            return len(self._pending[queue_name])

    async def clear(self, queue_name: str) -> int:
        self._check_connected()

        async with self._locks[queue_name]:
            queue_size = len(self._task_metadata[queue_name])
            queue_cron_size = len(self._crons[queue_name])

            self._task_metadata[queue_name].clear()
            self._pending[queue_name].clear()
            self._scheduled[queue_name].clear()
            self._crons[queue_name].clear()

            return queue_size + queue_cron_size

    async def get_tasks(self, queue_name: str, task_ids: list[str]) -> dict[str,dict[str, Any]]:
        self._check_connected()

        async with self._locks[queue_name]:
            results = {}
            for task_id in task_ids:
                result = self._task_metadata[queue_name].get(task_id)
                if result:
                    results[task_id] = result

            return results

    async def schedule(self, queue_name: str, task_data: dict[str, Any], at: datetime, unique: bool = True) -> bool:
        self._check_connected()

        if unique:
            success = await self._create_tasks(queue_name, [task_data])
            if not success[0]:
                return False

        async with self._locks[queue_name]:
            if not unique:
                self._task_metadata[queue_name][task_data["id"]] = task_data

        if self._instant_processing:
            await self.append(queue_name, [task_data], unique=False)
        else:
            async with self._locks[queue_name]:
                scheduled_task = ScheduledTask(at, task_data["id"])
                heapq.heappush(self._scheduled[queue_name], scheduled_task)

        return True

    async def pop_scheduled(self, queue_name: str, now: datetime | None = None) -> list[dict[str, Any]]:
        self._check_connected()

        if now is None:
            now = datetime.now(timezone.utc)

        async with self._locks[queue_name]:
            tasks = []
            scheduled_tasks = self._scheduled[queue_name]

            while scheduled_tasks and scheduled_tasks[0].scheduled_time <= now:
                scheduled_task = heapq.heappop(scheduled_tasks)
                task_data = self._task_metadata[queue_name].get(scheduled_task.task_id)
                if task_data:
                    tasks.append(task_data)

            return tasks

    async def store_result(self, queue_name: str, task_data: dict[str, Any]) -> bool:
        self._check_connected()

        async with self._locks[queue_name]:
            self._task_metadata[queue_name][task_data['id']] = task_data

            return True

    async def get_results(self, queue_name: str, task_ids: list[str], timeout: float | None = None) -> dict[str,dict[str, Any]]:
        self._check_connected()

        start_time = asyncio.get_event_loop().time()

        if not task_ids:
            return {}

        results = {}
        remaining_ids = task_ids[:]

        while True:
            async with self._locks[queue_name]:
                for task_id in task_ids:
                    task_data = self._task_metadata[queue_name].get(task_id, {})

                    if task_data.get("finished_at"):
                        results[task_id] = task_data
                        remaining_ids.remove(task_id)

            if not remaining_ids:
                return results

            if timeout is None or timeout < 0:
                return results

            # endless wait if timeout == 0
            if timeout == 0:
                await asyncio.sleep(0.05)
                continue

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                raise TimeoutError(f"Did not complete within {timeout} seconds")

            await asyncio.sleep(min(0.05, timeout - elapsed))

    async def get_stats(self, queue_name: str) -> dict[str, int]:
        self._check_connected()

        async with self._locks[queue_name]:
            return {
                "pending": len(self._pending[queue_name]),
                "completed": len([t for t in self._task_metadata[queue_name].values() if t["finished_at"]]),
                "scheduled": len(self._scheduled[queue_name]),
            }

    async def get_all_tasks(self, queue_name: str) -> list[dict[str, Any]]:
        self._check_connected()

        async with self._locks[queue_name]:
            tasks = self._task_metadata[queue_name]
            return list(tasks.values())

    async def list_queues(self) -> dict[str, int]:
        self._check_connected()

        queues = {}
        for queue_name in self._task_metadata:
            async with self._locks[queue_name]:
                queues[queue_name] = len(self._pending[queue_name])

        return queues

    async def get_scheduled(self, queue_name: str) -> list[dict[str, Any]]:
        self._check_connected()

        async with self._locks[queue_name]:
            tasks = []
            for scheduled_task in self._scheduled[queue_name]:
                task_data = self._task_metadata[queue_name].get(scheduled_task.task_id)
                if task_data:
                    tasks.append(task_data)

            return tasks

    async def add_cron(self, queue_name: str, deterministic_id: str, task_cron: dict[str, Any]) -> bool:
        self._check_connected()

        async with self._locks[queue_name]:
            if deterministic_id not in self._crons[queue_name]:
                self._crons[queue_name][deterministic_id] = task_cron
                return True
            return False

    async def delete_cron(self, queue_name: str, deterministic_id: str) -> bool:
        self._check_connected()

        async with self._locks[queue_name]:
            if deterministic_id in self._crons[queue_name]:
                del self._crons[queue_name][deterministic_id]
                return True
            return False

    async def get_crons(self, queue_name: str) -> list[dict[str, Any]]:
        self._check_connected()

        async with self._locks[queue_name]:
            return list(self._crons[queue_name].values())

    def _check_connected(self) -> None:
        if not self.is_connected:
            raise BackendError("Not connected")

    async def _process_task(self, queue_name: str, task_data: dict[str, Any]) -> None:
        # pop from pending
        task_id = task_data["id"]
        if task_id in self._pending[queue_name]:
            self._pending[queue_name].remove(task_id)

        # process the task
        task = Task.model_validate(task_data)
        _, processed_task = await self._task_processor.process_task(task, self._worker_id)

        # handle retry
        if processed_task.error and processed_task.should_retry and processed_task.next_retry_at is not None:
            async with self._locks[queue_name]:
                processed_data = processed_task.model_dump(mode="json")
                self._task_metadata[queue_name][task_id] = processed_data
                self._pending[queue_name].append(task_id)
            await self._process_task(queue_name, processed_data)
            return

        # handle task chaining
        if processed_task.completed and processed_task.result:
            if isinstance(processed_task.result, Task):
                chained_data = processed_task.result.model_dump(mode="json")
                await self.append(queue_name, [chained_data])

            elif isinstance(processed_task.result, list) and processed_task.result and isinstance(processed_task.result[0], Task):
                chained_tasks = [t.model_dump(mode="json") for t in processed_task.result if isinstance(t, Task)]
                if chained_tasks:
                    await self.append(queue_name, chained_tasks)
        await self.store_result(queue_name, processed_task.model_dump(mode="json"))
