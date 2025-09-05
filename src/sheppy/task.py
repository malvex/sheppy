import importlib
import os
import sys
from base64 import b64encode
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
from uuid import UUID, uuid3, uuid4

from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

P = ParamSpec('P')
R = TypeVar('R')


TASK_CRON_NS = UUID('7005b432-c135-4131-b19e-d3dc89703a9a')


class Spec(BaseModel):
    model_config = ConfigDict(frozen=True)

    func: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    return_type: str | None = None
    middleware: list[str] | None = None


class Config(BaseModel):
    model_config = ConfigDict(frozen=True)

    # Task Retries
    retry: float = Field(default=0, ge=0)
    retry_delay: float | list[float] = Field(default=1.0)
    retry_count: int = 0
    last_retry_at: datetime | None = None
    next_retry_at: datetime | None = None

    # timeout: float | None = None  # seconds
    # tags: dict[str, str] = Field(default_factory=dict)
    # extra: dict[str, Any] = Field(default_factory=dict)

    # status stuff...
    # caller: str | None = None
    # worker: str | None = None


class Task(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    completed: bool = False
    error: str | None = None
    result: Any = None

    spec: Spec
    config: Config = Field(default_factory=Config)

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    scheduled_at: datetime | None = None


    @model_validator(mode='after')
    def _reconstruct_pydantic_result(self) -> 'Task':
        """Reconstruct result if it's pydantic model."""

        if self.result and self.spec.return_type:
            # Reconstruct return if it's pydantic model
            module_name, type_name = self.spec.return_type.rsplit('.', 1)
            module = importlib.import_module(module_name)
            return_type = getattr(module, type_name)
            self.__dict__["result"] = TypeAdapter(return_type).validate_python(self.result)

        return self

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        parts = {
            "id": repr(self.id),
            "func": repr(self.spec.func),
            "args": repr(self.spec.args),
            "kwargs": repr(self.spec.kwargs),
            "completed": repr(self.completed),
            "error": repr(self.error)
        }

        if self.config.retry_count > 0:
            parts["retries"] = str(self.config.retry_count)

        return f"Task({', '.join([f'{k}={v}' for k, v in parts.items()])})"


class TaskCron(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    expression: str

    spec: Spec
    config: Config

    # enabled: bool = True
    # last_run: datetime | None = None
    # next_run: datetime | None = None

    #@computed_field  # use this instead of id for the dedup thing

    def next_run(self, start: datetime | None = None) -> datetime:
        if not start:
            start = datetime.now(timezone.utc)
        return croniter(self.expression, start).get_next(datetime)

    def create_task(self, start: datetime) -> Task:
        return Task(
            id=uuid3(TASK_CRON_NS, str(self.id) + str(start.timestamp())),
            spec=self.spec.model_copy(deep=True),
            config=self.config.model_copy(deep=True)
        )


class TaskFactory:

    def __init__(self) -> None:
        pass

    @staticmethod
    def _get_return_type(func: Callable[..., Any]) -> str | None:
        """Get function return type"""
        try:
            return_type = get_type_hints(func).get('return')
            if return_type and hasattr(return_type, '__module__') and hasattr(return_type, '__qualname__'):
                return_type = f"{return_type.__module__}.{return_type.__qualname__}"
        except TypeError:
            return_type = None

        return return_type

    @staticmethod
    def _stringify_function(func: Callable[..., Any]) -> str:
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
    def create_task(func: Callable[..., Any],
                    args: list[Any],
                    kwargs: dict[str, Any],
                    retry: float,
                    retry_delay: float | list[float] | None,
                    middleware: list[Callable[..., Any]] | None
                    ) -> Task:
        # Store return type to later reconstruct the result
        return_type = TaskFactory._get_return_type(func)

        task_config: dict[str, Any] = {
            "retry": retry
        }
        if retry_delay:
            task_config["retry_delay"] = retry_delay

        func_string = TaskFactory._stringify_function(func)

        args, kwargs = TaskFactory.validate_input(func, list(args or []), dict(kwargs or {}))

        stringified_middlewares = []
        if middleware:
            for m in middleware:
                # todo: should probably also validate them here
                stringified_middlewares.append(TaskFactory._stringify_function(m))

        _task = Task(
            spec=Spec(
                func=func_string,
                args=args,
                kwargs=kwargs,
                return_type=return_type,
                middleware=stringified_middlewares
            ),
            config=Config(**task_config)
        )

        return _task

    @staticmethod
    def create_cron_from_task(task: Task, cron_expression: str, queue_name: str | None = None) -> TaskCron:
        # create deterministic ID if not provided
        s = task.spec.model_dump_json(include={"func", "args", "kwargs"})
        s += b64encode(cron_expression.encode()).decode()
        s += (queue_name if queue_name else "")
        cron_id = str(uuid3(TASK_CRON_NS, s))

        return TaskCron(
            id=UUID(cron_id),
            expression=cron_expression,
            spec=task.spec.model_copy(deep=True),
            config=task.config.model_copy(deep=True),
        )


# Overload for @task() or @task(retry=..., retry_delay=...)
@overload
def task(
    *,
    retry: float = 0,
    retry_delay: float | list[float] | None = None,
    middleware: list[Callable[..., Any]] | None = None
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
    retry_delay: float | list[float] | None = None,
    middleware: list[Callable[..., Any]] | None = None
) -> Callable[[Callable[P, R]], Callable[P, Task]] | Callable[P, Task]:
    def decorator(func: Callable[P, R]) -> Callable[P, Task]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Task:

            return TaskFactory.create_task(func, list(args), kwargs, retry, retry_delay, middleware)

        return wrapper

    # If called without parentheses (@task), func will be the decorated function
    if func is not None:
        return decorator(func)

    # If called with parentheses (@task() or @task(name="...")), return the decorator
    return decorator
