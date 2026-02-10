import os
import sys
from collections.abc import Callable
from functools import wraps
from typing import (
    Any,
    ParamSpec,
    TypeVar,
    overload,
)

from ._utils.functions import stringify_function
from ._utils.validation import validate_input
from ._workflow import get_workflow_context
from .models import Task, TaskConfig, TaskCron, TaskSpec

P = ParamSpec('P')
R = TypeVar('R')

cache_main_module: str | None = None
cache_return_type: dict[Callable[..., Any], str | None] = {}


class TaskFactory:

    def __init__(self) -> None:
        pass

    @staticmethod
    def create_task(func: Callable[..., Any],
                    args: tuple[Any, ...],
                    kwargs: dict[str, Any],
                    retry: int,
                    retry_delay: float | list[float] | None,
                    middleware: list[Callable[..., Any]] | None,
                    timeout: float | None,
                    retry_on_timeout: bool | None,
                    ) -> Task:

        task_config: dict[str, Any] = {
            "retry": retry
        }
        if retry_delay is not None:
            task_config["retry_delay"] = retry_delay

        if timeout is not None:
            task_config["timeout"] = timeout
        if retry_on_timeout is not None:
            task_config["retry_on_timeout"] = retry_on_timeout

        func_string = stringify_function(func)

        args, kwargs = validate_input(func, tuple(args or ()), dict(kwargs or {}))

        stringified_middlewares = []
        if middleware:
            for m in middleware:
                # todo: should probably also validate them here
                stringified_middlewares.append(stringify_function(m))

        task_kwargs: dict[str, Any] = {}

        ctx = get_workflow_context()
        if ctx is not None:
            task_kwargs["id"] = ctx.next_task_id()
            task_kwargs["workflow_id"] = ctx.workflow_id

        _task = Task(
            **task_kwargs,
            spec=TaskSpec(
                func=func_string,
                args=args,
                kwargs=kwargs,
                middleware=stringified_middlewares
            ),
            config=TaskConfig(**task_config)
        )

        return _task

    @staticmethod
    def create_cron_from_task(task: Task, cron_expression: str) -> TaskCron:
        return TaskCron(
            expression=cron_expression,
            spec=task.spec.model_copy(deep=True),
            config=task.config.model_copy(deep=True),
        )


# Overload for @task() or @task(retry=..., retry_delay=...)
@overload
def task(
    *,
    retry: int = 0,
    retry_delay: float | list[float] | None = None,
    middleware: list[Callable[..., Any]] | None = None,
    timeout: float | None = None,
    retry_on_timeout: bool | None = None,
) -> Callable[[Callable[P, R]], Callable[P, Task]]:
    ...

# Overload for @task without parentheses
@overload
def task(func: Callable[P, R], /) -> Callable[P, Task]:
    ...

def task(
    func: Callable[P, R] | None = None,
    *,
    retry: int = 0,
    retry_delay: float | list[float] | None = None,
    middleware: list[Callable[..., Any]] | None = None,
    timeout: float | None = None,
    retry_on_timeout: bool | None = None,
) -> Callable[[Callable[P, R]], Callable[P, Task]] | Callable[P, Task]:
    def decorator(func: Callable[P, R]) -> Callable[P, Task]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Task:

            return TaskFactory.create_task(func, tuple(args), kwargs, retry, retry_delay, middleware, timeout, retry_on_timeout)

        return wrapper

    # If called without parentheses (@task), func will be the decorated function
    if func is not None:
        return decorator(func)

    # If called with parentheses (@task() or @task(name="...")), return the decorator
    return decorator
