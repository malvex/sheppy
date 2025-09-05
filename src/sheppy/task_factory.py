import os
import sys
from base64 import b64encode
from collections.abc import Callable
from functools import wraps
from typing import (
    Any,
    ParamSpec,
    TypeVar,
    get_type_hints,
    overload,
)
from uuid import UUID, uuid3

from .models import TASK_CRON_NS, Config, Spec, Task, TaskCron
from .utils.validation import validate_input

P = ParamSpec('P')
R = TypeVar('R')


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

        args, kwargs = validate_input(func, list(args or []), dict(kwargs or {}))

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
