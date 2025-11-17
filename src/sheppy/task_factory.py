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

from .models import Task, TaskConfig, TaskCron, TaskSpec
from .utils.validation import validate_input

P = ParamSpec('P')
R = TypeVar('R')

cache_main_module: str | None = None
cache_return_type: dict[Callable[..., Any], str | None] = {}


class TaskFactory:

    def __init__(self) -> None:
        pass

    @staticmethod
    def _stringify_function(func: Callable[..., Any]) -> str:
        _module = func.__module__
        # special case if the task is in the main python file that is executed
        if _module == "__main__":
            global cache_main_module
            if not cache_main_module:
                # this handles "python -m app.main" because with "-m" sys.argv[0] is absolute path
                _main_path = os.path.relpath(sys.argv[0])[:-3]
                # replace handles situations when user runs "python app/main.py"
                cache_main_module = _main_path.replace(os.sep, ".")

            _module = cache_main_module

        return f"{_module}:{func.__name__}"

    @staticmethod
    def create_task(func: Callable[..., Any],
                    args: tuple[Any, ...],
                    kwargs: dict[str, Any],
                    retry: int,
                    retry_delay: float | list[float] | None,
                    middleware: list[Callable[..., Any]] | None
                    ) -> Task:

        task_config: dict[str, Any] = {
            "retry": retry
        }
        if retry_delay is not None:
            task_config["retry_delay"] = retry_delay

        func_string = TaskFactory._stringify_function(func)

        args, kwargs = validate_input(func, tuple(args or ()), dict(kwargs or {}))

        stringified_middlewares = []
        if middleware:
            for m in middleware:
                # todo: should probably also validate them here
                stringified_middlewares.append(TaskFactory._stringify_function(m))

        _task = Task(
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
    retry: int = 0,
    retry_delay: float | list[float] | None = None,
    middleware: list[Callable[..., Any]] | None = None
) -> Callable[[Callable[P, R]], Callable[P, Task]] | Callable[P, Task]:
    def decorator(func: Callable[P, R]) -> Callable[P, Task]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Task:

            return TaskFactory.create_task(func, tuple(args), kwargs, retry, retry_delay, middleware)

        return wrapper

    # If called without parentheses (@task), func will be the decorated function
    if func is not None:
        return decorator(func)

    # If called with parentheses (@task() or @task(name="...")), return the decorator
    return decorator
