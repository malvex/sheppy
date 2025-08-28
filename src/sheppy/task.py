import importlib
import sys
import os
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
    model_config = ConfigDict(frozen=True)

    func: str | None = None
    args: list[Any] | None = None
    kwargs: dict[str, Any] | None = None
    return_type: str | None = None


class TaskMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

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
    model_config = ConfigDict(frozen=True)

    # Core task fields
    id: UUID = Field(default_factory=uuid4)
    completed: bool = False
    error: str | None = None
    result: Any = None

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
            self.__dict__["result"] = TypeAdapter(return_type).validate_python(self.result)

        return self

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

            task_metadata = {
                "retry": retry
            }
            if retry_delay:
                task_metadata["retry_delay"] = retry_delay

            _module = func.__module__
            # special case if the task is in the main python file that is executed
            if _module == "__main__":
                # this handles "python -m app.main" because with "-m" sys.argv[0] is absolute path
                _main_path = os.path.relpath(sys.argv[0])[:-3]
                # replace handles situations when user runs "python app/main.py"
                _module = _main_path.replace(os.sep, ".")

            _task = Task(
                internal=TaskInternal(
                    func=f"{_module}:{func.__name__}",
                    args=processed_args,
                    kwargs=processed_kwargs,
                    return_type=return_type
                ),
                metadata=TaskMetadata(**task_metadata)
            )

            return _task

        return wrapper

    # If called without parentheses (@task), func will be the decorated function
    if func is not None:
        return decorator(func)

    # If called with parentheses (@task() or @task(name="...")), return the decorator
    return decorator
