import asyncio
import heapq
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .base import Backend


@dataclass(order=True)
class ScheduledTask:
    scheduled_time: datetime
    task_data: dict[str, Any] = field(compare=False)


class MemoryBackend(Backend):

    def __init__(self) -> None:
        self._queues: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self._scheduled: dict[str, list[ScheduledTask]] = defaultdict(list)
        self._in_progress: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self._results: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)  # for thread-safety
        self._connected = False

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def append(self, queue_name: str, task_data: dict[str, Any]) -> bool:
        async with self._locks[queue_name]:
            self._queues[queue_name].append(task_data)
            return True

    async def pop(self, queue_name: str, timeout: float | None = None) -> dict[str, Any] | None:
        start_time = asyncio.get_event_loop().time()

        while True:
            async with self._locks[queue_name]:
                if self._queues[queue_name]:
                    task_data = self._queues[queue_name].popleft()
                    self._in_progress[queue_name][task_data['id']] = task_data

                    return task_data

            if timeout is None or timeout <= 0:
                return None

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                return None

            await asyncio.sleep(min(0.05, timeout - elapsed))

    async def acknowledge(self, queue_name: str, task_id: str) -> bool:
        async with self._locks[queue_name]:
            if task_id in self._in_progress[queue_name]:
                del self._in_progress[queue_name][task_id]
                return True
            return False

    async def peek(self, queue_name: str, count: int = 1) -> list[dict[str, Any]]:
        async with self._locks[queue_name]:
            return list(self._queues[queue_name])[:count]

    async def size(self, queue_name: str) -> int:
        async with self._locks[queue_name]:
            return len(self._queues[queue_name])

    async def clear(self, queue_name: str) -> int:
        async with self._locks[queue_name]:
            # Count all tasks before clearing
            queue_size = len(self._queues[queue_name])
            scheduled_size = len(self._scheduled[queue_name])
            in_progress_size = len(self._in_progress[queue_name])

            # Clear all data structures for this queue
            self._queues[queue_name].clear()
            self._scheduled[queue_name].clear()
            self._in_progress[queue_name].clear()

            return queue_size + scheduled_size + in_progress_size

    async def get_task(self, queue_name: str, task_id: str) -> dict[str, Any] | None:  # ! FIXME
        async with self._locks[queue_name]:
            # Check in results first (completed tasks)
            if task_id in self._results[queue_name]:
                return self._results[queue_name][task_id]

            # Check in-progress tasks
            if task_id in self._in_progress[queue_name]:
                return self._in_progress[queue_name][task_id]

            # Check in regular queue
            for task_data in self._queues[queue_name]:
                if task_data['id'] == task_id:
                    return task_data

            # Check scheduled tasks
            for scheduled_task in self._scheduled[queue_name]:
                if scheduled_task.task_data['id'] == task_id:
                    return scheduled_task.task_data

            return None

    async def schedule(self, queue_name: str, task_data: dict[str, Any], at: datetime) -> bool:
        async with self._locks[queue_name]:
            scheduled_task = ScheduledTask(at, task_data)
            heapq.heappush(self._scheduled[queue_name], scheduled_task)
            return True

    async def get_scheduled(self, queue_name: str, now: datetime | None = None) -> list[dict[str, Any]]:
        if now is None:
            now = datetime.now(timezone.utc)

        async with self._locks[queue_name]:
            ready_tasks = []
            scheduled_tasks = self._scheduled[queue_name]

            # Remove ready tasks from the scheduled queue and return them
            # Do NOT add them to the regular queue - let the caller decide what to do
            while scheduled_tasks and scheduled_tasks[0].scheduled_time <= now:
                scheduled_task = heapq.heappop(scheduled_tasks)
                ready_tasks.append(scheduled_task.task_data)

            return ready_tasks

    async def store_result(self, queue_name: str, task_data: dict[str, Any]) -> bool:
        async with self._locks[queue_name]:
            task_id = task_data['id']
            self._results[queue_name][task_id] = task_data
            return True

    async def get_result(self, queue_name: str, task_id: str, timeout: float | None = None) -> dict[str, Any] | None:
        start_time = asyncio.get_event_loop().time()

        while True:
            async with self._locks[queue_name]:
                if task_id in self._results[queue_name]:
                    task_data = self._results[queue_name][task_id]
                    # Check if task is finished (has metadata.finished_datetime or completed=True or has error)
                    metadata = task_data.get("metadata", {})
                    if metadata.get("finished_datetime") or task_data.get("completed") or task_data.get("error"):
                        return task_data

            if timeout is None or timeout <= 0:
                return None

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")

            await asyncio.sleep(min(0.05, timeout - elapsed))
