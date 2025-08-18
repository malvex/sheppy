import asyncio
import importlib
from collections.abc import Callable
from datetime import datetime, timezone
from functools import wraps
from typing import (
    Any,
    ParamSpec,
    TypeVar,
    get_type_hints,
    overload,
)
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from .utils.argument_processing import process_task_arguments

P = ParamSpec('P')
R = TypeVar('R')


class TaskInternal(BaseModel):
    model_config = ConfigDict()

    func: str | None = None
    args: list[Any] | None = None
    kwargs: dict[str, Any] | None = None
    return_type: str | None = None
    # Queue reference for wait_for_result
    queue: Any | None = Field(default=None, exclude=True)


class TaskMetadata(BaseModel):
    model_config = ConfigDict()

    created_datetime: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_datetime: datetime | None = None
    caller: str | None = None
    worker: str | None = None

    # Task Retries
    retry: float = Field(default=0, ge=0)
    retry_delay: float | list[float] = Field(default=1.0)
    retry_count: int = 0
    last_retry_at: datetime | None = None
    next_retry_at: datetime | None = None


class Task(BaseModel):
    # Core task fields
    id: UUID = Field(default_factory=uuid4)
    result: Any = None
    completed: bool = False
    error: str | None = None

    # Metadata
    metadata: TaskMetadata = Field(default_factory=TaskMetadata)
    internal: TaskInternal = Field(default_factory=TaskInternal, description="Internal task execution details")

    @model_validator(mode='after')
    def _reconstruct_pydantic_result(self) -> 'Task':
        """Reconstruct result if it's pydantic model."""
        if self.result and self.internal.return_type:
            # Reconstruct return if it's pydantic model
            module_name, type_name = self.internal.return_type.rsplit('.', 1)
            module = importlib.import_module(module_name)
            return_type = getattr(module, type_name)
            self.result = TypeAdapter(return_type).validate_python(self.result)

        return self

    async def refresh(self) -> bool:
        if not self.internal.queue:
            raise RuntimeError("Task is not associated with a queue. Add it to a queue first.")

        old_state = self.model_copy()

        current_task = await self.internal.queue.get_task(self.id)

        if current_task:
            self.completed = current_task.completed
            self.error = current_task.error
            self.result = current_task.result
            self.metadata = current_task.metadata

            old_state.internal.queue = self.internal.queue
            return old_state != self

        return False

    async def wait_for_result(
        self,
        timeout: float = 0,
        poll_interval: float = 0.1
    ) -> Any:
        start_time = asyncio.get_event_loop().time()

        while True:
            await self.refresh()

            # Check if task is finished (either completed successfully or failed)
            if self.metadata.finished_datetime is not None:
                if self.error:
                    raise ValueError(f"Task failed: {self.error}")
                return self.result

            if timeout > 0:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(f"Task {self.id} did not complete within {timeout} seconds")

            # ! FIXME - remove polling
            await asyncio.sleep(poll_interval)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        func_name = self.internal.func or "None"

        parts = {
            "id": repr(self.id),
            "func": repr(func_name),
            "args": repr(self.internal.args),
            "kwargs": repr(self.internal.kwargs),
            "completed": repr(self.completed),
            "error": repr(self.error)
        }

        if self.metadata.retry_count > 0:
            parts["retries"] = str(self.metadata.retry_count)

        return f"Task({', '.join([f'{k}={v}' for k, v in parts.items()])})"


# Overload for @task() or @task(retry=..., retry_delay=...)
@overload
def task(
    *,
    retry: float = 0,
    retry_delay: float | list[float] | None = None
) -> Callable[[Callable[P, R]], Callable[P, Task]]:
    ...

# Overload for @task without parentheses
@overload
def task(func: Callable[P, R], /) -> Callable[P, Task]:
    ...

def task(
    func: Callable[P, R] | None = None,
    *,
    retry: float = 0,
    retry_delay: float | list[float] | None = None
) -> Callable[[Callable[P, R]], Callable[P, Task]] | Callable[P, Task]:
    def decorator(func: Callable[P, R]) -> Callable[P, Task]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Task:
            # Process and validate arguments
            processed_args, processed_kwargs = process_task_arguments(func, args, kwargs)

            # Store return type to later reconstruct the result
            try:
                return_type = get_type_hints(func).get('return')
                if return_type and hasattr(return_type, '__module__') and hasattr(return_type, '__qualname__'):
                    return_type = f"{return_type.__module__}.{return_type.__qualname__}"
            except TypeError:
                return_type = None

            # Create task with internal data directly set
            _task = Task(
                internal=TaskInternal(
                    func=f"{func.__module__}:{func.__name__}",
                    args=processed_args,
                    kwargs=processed_kwargs,
                    return_type=return_type
                ),
                metadata=TaskMetadata(retry=retry)
            )
            if retry_delay:
                _task.metadata.retry_delay = retry_delay

            return _task

        return wrapper

    # If called without parentheses (@task), func will be the decorated function
    if func is not None:
        return decorator(func)

    # If called with parentheses (@task() or @task(name="...")), return the decorator
    return decorator
