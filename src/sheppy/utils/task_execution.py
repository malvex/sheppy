"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import importlib
import inspect
import socket
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin
from uuid import uuid4

import anyio
from pydantic import PydanticSchemaGenerationError, TypeAdapter

from .fastapi import Depends

if TYPE_CHECKING:
    from ..task import Task


def generate_unique_worker_id(prefix: str) -> str:
    return f"{prefix}-{socket.gethostname()}-{str(uuid4())[:8]}"


class TaskStatus(str, Enum):
    """Temporary, to make it easier to refactor."""

    SUCCESS = "success"
    FAILED_NO_RETRY = "failed_no_retry"
    FAILED_SHOULD_RETRY = "failed_should_retry"
    FAILED_OUT_OF_RETRY = "failed_ouf_of_retry"


class TaskProcessor:

    @staticmethod
    async def _actually_execute_task(task: "Task") -> Any:
        # resolve the function from its string representation
        func = __class__.resolve_function(task.spec.func)
        args = task.spec.args or []
        kwargs = task.spec.kwargs or {}

        # validate all parameters, inject DI and Task
        final_args, final_kwargs = await __class__.process_function_parameters(func, args, kwargs, task)

        # async task
        if inspect.iscoroutinefunction(func):
            return await func(*final_args, **final_kwargs)

        # sync task
        return await anyio.to_thread.run_sync(lambda: func(*final_args, **final_kwargs))

    @staticmethod
    async def execute_task(__task: "Task", worker_id: str) -> tuple[TaskStatus, Exception | None, "Task"]:
        try:
            __task, _generators = await __class__.process_pre_task_middleware(__task)
        except Exception as e:
            raise Exception("Middleware error") from e

        try:
            result = await __class__._actually_execute_task(__task)
            task = __class__.handle_success_and_update_task_metadata(__task, result, worker_id)
            task_status = TaskStatus.SUCCESS
            exception = None

        except Exception as e:
            task_status, task = await __class__.handle_failed_task(__task, e)

            exception = e  # temporary

        try:
            task = await __class__.process_post_task_middleware(task, _generators)
        except Exception as e:
            raise Exception("Middleware error") from e

        return task_status, exception, task

    @staticmethod
    async def process_pre_task_middleware(task: "Task") -> tuple["Task", list]:
        if not task.spec.middleware:
            return task, []

        _generators = []

        for middleware_string in task.spec.middleware:
            middleware = __class__.resolve_function(middleware_string, wrapped=False)
            gen = middleware(task)
            task = next(gen) or task
            _generators.append(gen)

        return task, _generators

    @staticmethod
    async def process_post_task_middleware(task: "Task", _generators: list) -> "Task":
        if not _generators:
            return task

        for gen in _generators[::-1]:  # post task middleware goes in reverse order
            try:
                task = gen.send(task) or task
            except StopIteration as e:
                task = e.value or task

        return task

    @staticmethod
    def handle_success_and_update_task_metadata(task: "Task", result: Any, worker_id: str) -> "Task":
        # recreate an updated task, don't mutate the original  # ! FIXME
        updated_task = task.model_copy(deep=True)
        updated_task.__dict__["result"] = result
        updated_task.__dict__["completed"] = True
        updated_task.__dict__["error"] = None  # Clear any previous error on success

        #updated_task.config.__dict__["worker"] = worker_id
        updated_task.__dict__["finished_at"] = datetime.now(timezone.utc)

        return updated_task

    @staticmethod
    async def handle_failed_task(__task: "Task", exception: Exception):
        # recreate an updated task, don't mutate the original  # ! FIXME
        task = __task.model_copy(deep=True)
        task.__dict__["completed"] = False
        task.__dict__["error"] = str(exception)

        # Check if task should be retried
        if task.config.retry > 0:
            task_status, task = __class__.handle_retry(task)
        else:
            task.__dict__["finished_at"] = datetime.now(timezone.utc)
            task_status = TaskStatus.FAILED_NO_RETRY

        return task_status, task

    @staticmethod
    def handle_retry(task: "Task"):  # ! FIXME - mutates input - temp
        if task.config.retry_count < task.config.retry:
            task.config.__dict__["retry_count"] += 1
            task.config.__dict__["last_retry_at"] = datetime.now(timezone.utc)
            task.config.__dict__["next_retry_at"] = datetime.now(timezone.utc) + timedelta(seconds=__class__.calculate_retry_delay(task))
            # task will be retried  # ! FIXME
            task.__dict__["finished_at"] = None
            task_status = TaskStatus.FAILED_SHOULD_RETRY
        else:
            # final failure - no more retries  # ! FIXME
            task.__dict__["finished_at"] = datetime.now(timezone.utc)
            task_status = TaskStatus.FAILED_OUT_OF_RETRY

        return task_status, task

    @staticmethod
    def calculate_retry_delay(task: "Task") -> float:
        if isinstance(task.config.retry_delay, float):
            return task.config.retry_delay # constant delay for all retries
        if isinstance(task.config.retry_delay, list):
            if len(task.config.retry_delay) == 0:
                return 1.0  # empty list defaults to 1 second  # ! FIXME - we should probably refuse empty lists as input

            if task.config.retry_count < len(task.config.retry_delay):
                return float(task.config.retry_delay[task.config.retry_count])
            else:
                # use last delay value for remaining retries
                return float(task.config.retry_delay[-1])

        # this should never happen if the library is used correctly  # ! FIXME
        if isinstance(task.config.retry_delay, int):
            return float(task.config.retry_delay)
        # this should never happen if the library is used correctly  # ! FIXME
        raise ValueError(f"Invalid retry_delay type: {type(task.config.retry_delay).__name__}. Expected None, float, or list.")

    @staticmethod
    def resolve_function(func: str, wrapped: bool = True):
        try:
            module_name, function_name = func.split(':')
            module = importlib.import_module(module_name)
            fn = getattr(module, function_name)
            return fn.__wrapped__ if wrapped else fn

        except (ValueError, ImportError, AttributeError) as e:
            raise ValueError(f"Cannot resolve function: {func}") from e

    @staticmethod
    async def process_function_parameters(
        func: Callable[..., Any],
        args: list[Any],
        kwargs: dict[str, Any],
        task: "Task | None" = None,
    ) -> tuple[list[Any], dict[str, Any]]:

        final_args = []
        final_kwargs = kwargs.copy()
        remaining_args = args.copy()

        for param_name, param in list(inspect.signature(func).parameters.items()):
            # Task injection (self: Task)
            if task and __class__._is_task_injection(param):
                final_args.append(task)
                continue

            # validate positional args
            if remaining_args:
                final_args.append(__class__._validate(remaining_args.pop(0), param.annotation))
                continue

            # dependency injection
            if depends := __class__.get_depends_from_param(param):
                final_kwargs[param_name] = await __class__._resolve_dependency(depends.dependency)
                continue

            # validate kwargs
            if param_name in kwargs:
                final_kwargs[param_name] = __class__._validate(kwargs[param_name], param.annotation)

        return final_args, final_kwargs

    @staticmethod
    def _is_task_injection(param: inspect.Parameter) -> bool:
        if param.name != 'self':
            return False

        ann = param.annotation
        if ann == inspect.Parameter.empty:
            return False

        if isinstance(ann, str):
            return ann == 'Task'

        return (getattr(ann, '__name__', None) == 'Task' and
                getattr(ann, '__module__', None) == 'sheppy.task')

    @staticmethod
    def _validate(value: Any, annotation: Any) -> Any:
        if value is not None and annotation != inspect.Parameter.empty:
            try:
                return TypeAdapter(annotation).validate_python(value)
            except PydanticSchemaGenerationError:
                pass

        return value

    @staticmethod
    async def _resolve_dependency(func: Callable) -> Any:
        # func = resolver.dependency_overrides.get(dep_func, dep_func)  # ! FIXME

        # resolve nested dependencies
        _, kwargs = await __class__.process_function_parameters(func, [], {}, None)

        # execute dependency
        if inspect.iscoroutinefunction(func):
            return await func(**kwargs)

        if inspect.isasyncgenfunction(func):
            return await func(**kwargs).__anext__()

        if inspect.isgeneratorfunction(func):
            return await anyio.to_thread.run_sync(next, func(**kwargs))

        return await anyio.to_thread.run_sync(lambda: func(**kwargs))

    @staticmethod
    def get_depends_from_param(param: inspect.Parameter) -> Any | None:
        if param.default != inspect.Parameter.empty and isinstance(param.default, Depends):
            return param.default

        # Annotated style
        if get_origin(param.annotation) is Annotated:
            args = get_args(param.annotation)

            for arg in args[1:]:
                if isinstance(arg, Depends):
                    return arg

        return None
