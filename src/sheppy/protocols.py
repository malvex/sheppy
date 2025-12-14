from collections.abc import AsyncGenerator, Callable, Generator
from typing import TYPE_CHECKING, Any, Protocol

from sheppy.models import Task

if TYPE_CHECKING:
    from sheppy.models import TaskStatus
    from sheppy.queue import Queue


class MiddlewareProtocol(Protocol):
    def __call__(self, task: Task, queue: "Queue") -> Generator[Task | None, tuple[Task, Exception | None], Task | None]: ...


class AsyncMiddlewareProtocol(Protocol):
    async def __call__(self, task: Task, queue: "Queue") -> AsyncGenerator[Task | None, tuple[Task, Exception | None]]:
        yield None
        ...


class TaskProcessorProtocol(Protocol):
    middleware: list[AsyncMiddlewareProtocol | MiddlewareProtocol] | None
    dependency_overrides: dict[Callable[..., Any], Callable[..., Any]]

    def __init__(
            self,
            /,
            middleware: list[str] | None = None,
            dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None,
    ): ...

    async def process_task(self, task: Task, queue: "Queue", worker_id: str) -> tuple[Exception | None, Task]: ...

    async def _preprocess(self, task: Task, queue: "Queue") -> tuple[Task, list[Any]]: ...

    async def _postprocess(self, task: Task, exception: Exception | None, _generators: list[Any]) -> Task: ...

    @staticmethod
    def update_task_status(task: Task, status: "TaskStatus") -> Task: ...

    @staticmethod
    def mark_completed(task: Task, result: Any, status: "TaskStatus" = 'completed') -> Task: ...

    @staticmethod
    def mark_failed(task: Task, exception: Exception, status: "TaskStatus" = 'failed') -> Task: ...
