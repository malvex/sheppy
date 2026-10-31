from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING, Any, Protocol

from sheppy.models import Task

if TYPE_CHECKING:
    from sheppy._queue import Queue
    from sheppy.models import TaskStatus


class MiddlewareProtocol(Protocol):
    def __call__(self, task: Task, queue: "Queue") -> Generator[Task | None, tuple[Task, Exception | None], Task | None]: ...


class AsyncMiddlewareProtocol(Protocol):
    async def __call__(self, task: Task, queue: "Queue") -> AsyncGenerator[Task | None, tuple[Task, Exception | None]]:
        yield None
        ...


class TaskProcessorProtocol(Protocol):
    middleware: list[AsyncMiddlewareProtocol | MiddlewareProtocol] | None

    async def process_task(self, task: Task, queue: "Queue", worker_id: str) -> tuple[Exception | None, Task]: ...

    async def handle_stale_task(self, task: Task, queue: "Queue") -> Task: ...

    @staticmethod
    def update_task_status(task: Task, status: "TaskStatus") -> Task: ...

    @staticmethod
    def mark_completed(task: Task, result: Any, status: "TaskStatus" = 'completed') -> Task: ...

    @staticmethod
    def mark_failed(task: Task, exception: Exception, status: "TaskStatus" = 'failed') -> Task: ...

    @staticmethod
    def mark_crashed(task: Task, error: str = "Worker crashed during execution", status: "TaskStatus" = 'crashed') -> Task: ...
