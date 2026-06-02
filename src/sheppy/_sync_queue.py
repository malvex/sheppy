import asyncio
import threading
from collections.abc import Coroutine
from datetime import datetime, timedelta
from typing import Any, TypeVar, overload
from uuid import UUID

from ._workflow import Workflow, WorkflowResult
from .models import Task, TaskCron
from .queue import Queue

T = TypeVar("T")


class SyncQueue:
    def __init__(self, backend: str | None = None, name: str | None = None):
        self._backend = backend
        self._name = name

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._closed = False

        self._queue: Queue = asyncio.run_coroutine_threadsafe(self._create_queue(), self._loop).result()

    async def _create_queue(self) -> Queue:
        return Queue(self._backend, self._name)

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)

    def _run_coro(self, coro: Coroutine[Any, Any, T]) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    # def __enter__(self) -> "SyncQueue":
    #     return self

    # def __exit__(self, *args: object) -> None:
    #     self.close()

    @overload
    def add(self, task: Task) -> bool: ...

    @overload
    def add(self, task: list[Task]) -> list[bool]: ...

    def add(self, task: Task | list[Task]) -> bool | list[bool]:
        if isinstance(task, list):  # mypy needs this
            return self._run_coro(self._queue.add(task))
        return self._run_coro(self._queue.add(task))

    @overload
    def get_task(self, task: Task | UUID | str) -> Task | None: ...

    @overload
    def get_task(self, task: list[Task | UUID | str]) -> dict[UUID, Task]: ...

    def get_task(self, task: Task | UUID | str | list[Task | UUID | str]) -> Task | None | dict[UUID, Task]:
        return self._run_coro(self._queue.get_task(task))

    def get_all_tasks(self) -> list[Task]:
        return self._run_coro(self._queue.get_all_tasks())

    def get_pending(self, count: int = 1) -> list[Task]:
        return self._run_coro(self._queue.get_pending(count))

    def schedule(self, task: Task, at: datetime | timedelta) -> bool:
        return self._run_coro(self._queue.schedule(task, at))

    def get_scheduled(self) -> list[Task]:
        return self._run_coro(self._queue.get_scheduled())

    def retry(self, task: Task | UUID | str, at: datetime | timedelta | None = None, force: bool = False) -> bool:
        return self._run_coro(self._queue.retry(task, at, force))

    def size(self) -> int:
        return self._run_coro(self._queue.size())

    def clear(self) -> int:
        return self._run_coro(self._queue.clear())

    def add_cron(self, task: Task, cron: str) -> bool:
        return self._run_coro(self._queue.add_cron(task, cron))

    def delete_cron(self, task: Task, cron: str) -> bool:
        return self._run_coro(self._queue.delete_cron(task, cron))

    def get_crons(self) -> list[TaskCron]:
        return self._run_coro(self._queue.get_crons())

    @overload
    def wait_for(self, task: Task | UUID | str, timeout: float = 0) -> Task | None: ...

    @overload
    def wait_for(self, task: list[Task | UUID | str], timeout: float = 0) -> dict[UUID, Task]: ...

    def wait_for(self, task: Task | UUID | str | list[Task | UUID | str], timeout: float = 0) -> Task | None | dict[UUID, Task]:
        return self._run_coro(self._queue.wait_for(task, timeout))

    def add_workflow(self, workflow: Workflow) -> WorkflowResult:
        return self._run_coro(self._queue.add_workflow(workflow))

    def resume_workflow(self, workflow: Workflow | UUID | str, task_results: dict[UUID, Task] | None = None) -> WorkflowResult:
        return self._run_coro(self._queue.resume_workflow(workflow, task_results))

    @overload
    def get_workflow(self, workflow: Workflow | UUID | str) -> Workflow | None: ...

    @overload
    def get_workflow(self, workflow: list[Workflow | UUID | str]) -> dict[UUID, Workflow]: ...

    def get_workflow(self, workflow: Workflow | UUID | str | list[Workflow | UUID | str]) -> Workflow | None | dict[UUID, Workflow]:
        return self._run_coro(self._queue.get_workflow(workflow))

    def get_all_workflows(self) -> list[Workflow]:
        return self._run_coro(self._queue.get_all_workflows())

    def get_pending_workflows(self) -> list[Workflow]:
        return self._run_coro(self._queue.get_pending_workflows())

    def delete_workflow(self, workflow: Workflow | UUID | str) -> bool:
        return self._run_coro(self._queue.delete_workflow(workflow))
