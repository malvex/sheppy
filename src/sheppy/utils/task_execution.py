"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import importlib
import inspect
import socket
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Annotated, Any, cast, get_args, get_origin
from uuid import uuid4

import anyio
from pydantic import ConfigDict, PydanticSchemaGenerationError, TypeAdapter

from ..models import Config, Spec, Task
from .fastapi import Depends


class SpecInternal(Spec):
     model_config = ConfigDict(frozen=False)

class ConfigInternal(Config):
    model_config = ConfigDict(frozen=False)

class TaskInternal(Task):
    model_config = ConfigDict(frozen=False)

    spec: SpecInternal
    config: ConfigInternal

    @staticmethod
    def from_task(task: Task) -> "TaskInternal":
        return TaskInternal.model_validate(task.model_dump(mode="json"))

    def create_task(self) -> Task:
        return Task.model_validate(self.model_dump(mode="json"))


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
    async def _actually_execute_task(task: TaskInternal) -> Any:
        # resolve the function from its string representation
        func = TaskProcessor.resolve_function(task.spec.func)
        args = task.spec.args or []
        kwargs = task.spec.kwargs or {}

        # validate all parameters, inject DI and Task
        final_args, final_kwargs = await TaskProcessor.process_function_parameters(func, args, kwargs, task)

        # async task
        if inspect.iscoroutinefunction(func):
            return await func(*final_args, **final_kwargs)

        # sync task
        return await anyio.to_thread.run_sync(lambda: func(*final_args, **final_kwargs))

    @staticmethod
    async def execute_task(__task: "Task", worker_id: str) -> tuple[TaskStatus, Exception | None, "Task"]:
        task = TaskInternal.from_task(__task)

        try:
            task, _generators = await TaskProcessor.process_pre_task_middleware(task)
        except Exception as e:
            raise Exception("Middleware error") from e

        try:
            result = await TaskProcessor._actually_execute_task(task)
            task = TaskProcessor.handle_success_and_update_task_metadata(task, result, worker_id)
            task_status = TaskStatus.SUCCESS
            exception = None

        except Exception as e:
            task_status, task = await TaskProcessor.handle_failed_task(task, e)

            exception = e  # temporary

        try:
            task = await TaskProcessor.process_post_task_middleware(task, _generators)
        except Exception as e:
            raise Exception("Middleware error") from e

        return task_status, exception, task.create_task()

    @staticmethod
    async def process_pre_task_middleware(task: TaskInternal) -> tuple[TaskInternal, list[Any]]:
        if not task.spec.middleware:
            return task, []

        _generators = []

        for middleware_string in task.spec.middleware:
            middleware = TaskProcessor.resolve_function(middleware_string, wrapped=False)
            gen = middleware(task)
            task = next(gen) or task
            _generators.append(gen)

        return task, _generators

    @staticmethod
    async def process_post_task_middleware(task: TaskInternal, _generators: list[Any]) -> TaskInternal:
        if not _generators:
            return task

        for gen in _generators[::-1]:  # post task middleware goes in reverse order
            try:
                task = gen.send(task) or task
            except StopIteration as e:
                task = e.value or task

        return task

    @staticmethod
    def handle_success_and_update_task_metadata(task: TaskInternal, result: Any, worker_id: str) -> TaskInternal:
        task.result = result
        task.completed = True
        task.error = None  # Clear any previous error on success

        #task.config.worker = worker_id
        task.finished_at = datetime.now(timezone.utc)

        return task

    @staticmethod
    async def handle_failed_task(task: TaskInternal, exception: Exception) -> tuple[TaskStatus, TaskInternal]:
        task.completed = False
        task.error = str(exception)

        # Check if task should be retried
        if task.config.retry > 0:
            task_status, task = TaskProcessor.handle_retry(task)
        else:
            task.finished_at = datetime.now(timezone.utc)
            task_status = TaskStatus.FAILED_NO_RETRY

        return task_status, task

    @staticmethod
    def handle_retry(task: TaskInternal) -> tuple[TaskStatus, TaskInternal]:
        if task.config.retry_count < task.config.retry:
            task.config.retry_count += 1
            task.config.last_retry_at = datetime.now(timezone.utc)
            task.config.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=TaskProcessor.calculate_retry_delay(task))
            # task will be retried  # ! FIXME
            task.finished_at = None
            task_status = TaskStatus.FAILED_SHOULD_RETRY
        else:
            # final failure - no more retries  # ! FIXME
            task.finished_at = datetime.now(timezone.utc)
            task_status = TaskStatus.FAILED_OUT_OF_RETRY

        return task_status, task

    @staticmethod
    def calculate_retry_delay(task: TaskInternal) -> float:
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
    def resolve_function(func: str, wrapped: bool = True) -> Callable[..., Any]:
        try:
            module_name, function_name = func.split(':')
            module = importlib.import_module(module_name)
            fn = getattr(module, function_name)
            result = fn.__wrapped__ if wrapped else fn
            return cast(Callable[..., Any], result)

        except (ValueError, ImportError, AttributeError) as e:
            raise ValueError(f"Cannot resolve function: {func}") from e

    @staticmethod
    async def process_function_parameters(
        func: Callable[..., Any],
        args: list[Any],
        kwargs: dict[str, Any],
        task: TaskInternal | None = None,
    ) -> tuple[list[Any], dict[str, Any]]:

        final_args = []
        final_kwargs = kwargs.copy()
        remaining_args = args.copy()

        for param_name, param in list(inspect.signature(func).parameters.items()):
            # Task injection (self: Task)
            if task and TaskProcessor._is_task_injection(param):
                final_args.append(task)
                continue

            # validate positional args
            if remaining_args:
                final_args.append(TaskProcessor._validate(remaining_args.pop(0), param.annotation))
                continue

            # dependency injection
            if depends := TaskProcessor.get_depends_from_param(param):
                final_kwargs[param_name] = await TaskProcessor._resolve_dependency(depends.dependency)
                continue

            # validate kwargs
            if param_name in kwargs:
                final_kwargs[param_name] = TaskProcessor._validate(kwargs[param_name], param.annotation)

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
                getattr(ann, '__module__', None) == 'sheppy.models')  # ! FIXME

    @staticmethod
    def _validate(value: Any, annotation: Any) -> Any:
        if value is not None and annotation != inspect.Parameter.empty:
            try:
                return TypeAdapter(annotation).validate_python(value)
            except PydanticSchemaGenerationError:
                pass

        return value

    @staticmethod
    async def _resolve_dependency(func: Callable[..., Any]) -> Any:
        # func = resolver.dependency_overrides.get(dep_func, dep_func)  # ! FIXME

        # resolve nested dependencies
        _, kwargs = await TaskProcessor.process_function_parameters(func, [], {}, None)

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
