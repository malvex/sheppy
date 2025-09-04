import importlib
import sys
import os
from collections.abc import Callable
from datetime import datetime, timezone, UTC
from croniter import croniter
from functools import wraps
from base64 import b64encode
from typing import (
    Any,
    ParamSpec,
    TypeVar,
    get_type_hints,
    overload,
)
from uuid import UUID, uuid3, uuid4

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

P = ParamSpec('P')
R = TypeVar('R')


TASK_CRON_NS = UUID('7005b432-c135-4131-b19e-d3dc89703a9a')


class TaskCron(BaseModel):
    id: str | None = None
    func: str = None
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    cron: str

    def next_run(self, start: datetime | None = None) -> datetime | None:
        if not start:
            start = datetime.now(UTC)
        return croniter(self.cron, start).get_next(datetime)

    def create_task(self, start: datetime) -> "Task":
        task_id = str(uuid3(TASK_CRON_NS, self.id + str(start.timestamp())))

        return Task(
            id=task_id,
            internal=TaskInternal(
                func=self.func,
                args=self.args,
                kwargs=self.kwargs,
                #return_type=return_type  # fixme?
            ),
            #metadata=TaskMetadata(**task_metadata)
        )



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

    def copy(self) -> "Task":
        print("KINDA BUGGY")
        return self.model_validate(self.model_dump(exclude_unset=True))

    def _create_task_cron(self, cron: str, queue_name: str = None) -> TaskCron:
        # create deterministic ID if not provided
        s = self.internal.model_dump_json(include=["func", "args", "kwargs"])
        s += b64encode(cron.encode()).decode()
        s += (queue_name if queue_name else "")
        cron_id = str(uuid3(TASK_CRON_NS, s))

        return TaskCron(
            id=cron_id,
            func=self.internal.func,
            args=self.internal.args,
            kwargs=self.internal.kwargs,
            cron=cron
        )

class TaskFactory:

    def __init__(self):
        pass

    @staticmethod
    def _get_return_type(func):
        """Get function return type"""
        try:
            return_type = get_type_hints(func).get('return')
            if return_type and hasattr(return_type, '__module__') and hasattr(return_type, '__qualname__'):
                return_type = f"{return_type.__module__}.{return_type.__qualname__}"
        except TypeError:
            return_type = None

        return return_type

    @staticmethod
    def _stringify_function(func):
        _module = func.__module__
        # special case if the task is in the main python file that is executed
        if _module == "__main__":
            # this handles "python -m app.main" because with "-m" sys.argv[0] is absolute path
            _main_path = os.path.relpath(sys.argv[0])[:-3]
            # replace handles situations when user runs "python app/main.py"
            _module = _main_path.replace(os.sep, ".")

        return f"{_module}:{func.__name__}"

    @staticmethod
    def validate_input(func: Callable[..., Any], args: list[Any], kwargs: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
        ## ! FIXME
        from .utils.validation import validate_input
        return validate_input(func, args, kwargs)

    @staticmethod
    def create_task(func, args, kwargs, retry, retry_delay) -> Task:
        # Store return type to later reconstruct the result
        return_type = __class__._get_return_type(func)

        task_metadata = {
            "retry": retry
        }
        if retry_delay:
            task_metadata["retry_delay"] = retry_delay

        func_string = __class__._stringify_function(func)

        args, kwargs = __class__.validate_input(func, list(args or []), dict(kwargs or {}))

        _task = Task(
            internal=TaskInternal(
                func=func_string,
                args=args,
                kwargs=kwargs,
                return_type=return_type
            ),
            metadata=TaskMetadata(**task_metadata)
        )

        return _task


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

            return TaskFactory.create_task(func, args, kwargs, retry, retry_delay)

        return wrapper

    # If called without parentheses (@task), func will be the decorated function
    if func is not None:
        return decorator(func)

    # If called with parentheses (@task() or @task(name="...")), return the decorator
    return decorator
