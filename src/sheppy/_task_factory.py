from collections.abc import Callable
from functools import wraps
from typing import (
    Any,
    Literal,
    ParamSpec,
    TypeVar,
    overload,
)

from ._utils.functions import stringify_function
from ._utils.validation import validate_input
from .experimental._workflow import get_workflow_context
from .models import RateLimit, Task, TaskConfig, TaskCron, TaskSpec, TTLValue

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
                    timeout: float | None,
                    retry_on_timeout: bool | None,
                    retry_on_crash: bool | None,
                    rate_limit: RateLimit | None = None,
                    ttl: TTLValue = "inherit",
                    error_ttl: TTLValue = "inherit",
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
        if retry_on_crash is not None:
            task_config["retry_on_crash"] = retry_on_crash

        if rate_limit is not None:
            task_config["rate_limit"] = dict(rate_limit)

        if ttl != "inherit":
            task_config["ttl"] = ttl
        if error_ttl != "inherit":
            task_config["error_ttl"] = error_ttl

        func_string = stringify_function(func)

        args, kwargs = validate_input(func, tuple(args or ()), dict(kwargs or {}))

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
            ),
            config=TaskConfig(**task_config)
        )

        return _task

    @staticmethod
    def create_cron_from_task(task: Task, cron_expression: str, managed_by: str | None = None) -> TaskCron:
        return TaskCron(
            expression=cron_expression,
            spec=task.spec.model_copy(deep=True),
            config=task.config.model_copy(deep=True),
            managed_by=managed_by,
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
    retry_on_crash: bool | None = None,
    rate_limit: RateLimit | None = None,
    ttl: int | None | Literal["inherit"] = "inherit",
    error_ttl: int | None | Literal["inherit"] = "inherit",
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
    retry_on_crash: bool | None = None,
    rate_limit: RateLimit | None = None,
    ttl: int | None | Literal["inherit"] = "inherit",
    error_ttl: int | None | Literal["inherit"] = "inherit",
) -> Callable[[Callable[P, R]], Callable[P, Task]] | Callable[P, Task]:
    """Turn a function into a task factory.

    Works with and without parentheses (`@task` or `@task(retry=3)`) and with
    both sync and async functions. Calling the decorated function does not
    execute it. It validates the arguments and returns a Task instance ready
    to be queued.

    Args:
        retry: Number of times to retry the task if it fails. Default is 0 (no retries).
        retry_delay: Delay between retries in seconds. A single value applies to
            every attempt; a list sets the delay per attempt. Defaults to 1.0.
        middleware: Middleware applied to this task only.
        timeout: Maximum execution time in seconds. Default is None (no timeout).
        retry_on_timeout: If True, a task that times out is retried. Default is False.
        retry_on_crash: If True, a task whose worker crashed is retried
            (RedisBackend only). Default is False.
        rate_limit: RateLimit dict, e.g. `{"max_rate": 2, "rate_period": 5}`.
        ttl: Expiry of the task's stored metadata once it finishes, in seconds.
            None disables expiry; "inherit" (default) uses the backend's `ttl`.
        error_ttl: Expiry applied when the task fails, in seconds. None disables
            expiry; "inherit" (default) falls back to `ttl`.

    Returns:
        The decorated function. Calling it returns a Task instance.

    Example:
        ```python
        from sheppy import task

        @task(retry=3, retry_delay=[1, 10, 60])
        async def send_email(to: str) -> str:
            ...

        t = send_email("alice@example.com")  # returns a Task model, nothing runs yet
        ```
    """
    def decorator(func: Callable[P, R]) -> Callable[P, Task]:
        func.__sheppy_task__ = True  # type: ignore[attr-defined]

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Task:

            return TaskFactory.create_task(
                func,
                tuple(args),
                kwargs,
                retry,
                retry_delay,
                timeout,
                retry_on_timeout,
                retry_on_crash,
                rate_limit,
                ttl,
                error_ttl
            )

        wrapper.__sheppy_middleware__ = list(middleware if middleware else [])  # type: ignore[attr-defined]

        return wrapper

    # If called without parentheses (@task), func will be the decorated function
    if func is not None:
        return decorator(func)

    # If called with parentheses (@task() or @task(retry=3)), return the decorator
    return decorator
