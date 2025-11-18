"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import importlib
import inspect
import os
import socket
import sys
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, cast, get_args, get_origin
from uuid import uuid4

import anyio
from pydantic import ConfigDict, PydanticSchemaGenerationError, TypeAdapter

from ..models import Task
from .fastapi import Depends

cache_main_module: str | None = None
cache_signature: dict[Callable[..., Any], inspect.Signature] = {}


class TaskInternal(Task):
    model_config = ConfigDict(frozen=False)

    @staticmethod
    def from_task(task: Task) -> "TaskInternal":
        return TaskInternal.model_validate(task.model_dump(mode="json"))

    def create_task(self) -> Task:
        return Task.model_validate(self.model_dump(mode="json"))


def generate_unique_worker_id(prefix: str) -> str:
    return f"{prefix}-{socket.gethostname()}-{str(uuid4())[:8]}"


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
    async def execute_task(__task: "Task", worker_id: str) -> tuple[Exception | None, "Task"]:
        task = TaskInternal.from_task(__task)

        try:
            task, _generators = await TaskProcessor.process_pre_task_middleware(task)
        except Exception as e:
            raise Exception("Middleware error") from e

        try:
            result = await TaskProcessor._actually_execute_task(task)
            task = TaskProcessor.handle_success_and_update_task_metadata(task, result, worker_id)
            exception = None

        except Exception as e:
            task = await TaskProcessor.handle_failed_task(task, e)
            exception = e  # temporary

        try:
            task = await TaskProcessor.process_post_task_middleware(task, _generators)
        except Exception as e:
            raise Exception("Middleware error") from e

        # result validation might fail
        try:
            final_task = task.create_task()
        except Exception as e:
            task = await TaskProcessor.handle_failed_task(task, e)
            exception = e
            final_task = task.create_task()

        return exception, final_task

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
        task.error = None

        #task.config.worker = worker_id
        task.finished_at = datetime.now(timezone.utc)

        return task

    @staticmethod
    async def handle_failed_task(task: TaskInternal, exception: Exception) -> TaskInternal:
        task.completed = False
        task.result = None
        task.error = f"{exception.__class__.__name__}: {exception}"

        if task.is_retriable:
            task = TaskProcessor.handle_retry(task)
        else:
            task.finished_at = datetime.now(timezone.utc)

        return task

    @staticmethod
    def handle_retry(task: TaskInternal) -> TaskInternal:
        if task.should_retry:
            task.retry_count += 1
            task.last_retry_at = datetime.now(timezone.utc)
            task.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=TaskProcessor.calculate_retry_delay(task))
            task.finished_at = None
        else:
            task.finished_at = datetime.now(timezone.utc)

        return task

    @staticmethod
    def calculate_retry_delay(task: TaskInternal) -> float:
        if isinstance(task.config.retry_delay, float):
            return task.config.retry_delay  # constant delay for all retries
        if isinstance(task.config.retry_delay, list):
            if len(task.config.retry_delay) == 0:
                return 1.0  # empty list defaults to 1 second  # todo - we should probably refuse empty lists as input

            if task.retry_count < len(task.config.retry_delay):
                return float(task.config.retry_delay[task.retry_count])
            else:
                # use last delay value for remaining retries
                return float(task.config.retry_delay[-1])

        # this should never happen if the library is used correctly
        if isinstance(task.config.retry_delay, int):
            return float(task.config.retry_delay)
        # this should never happen if the library is used correctly
        raise ValueError(f"Invalid retry_delay type: {type(task.config.retry_delay).__name__}. Expected float or list[float].")

    @staticmethod
    def resolve_function(func: str, wrapped: bool = True) -> Callable[..., Any]:
        module_name = None
        function_name = None

        try:
            module_name, function_name = func.split(':')
            module = importlib.import_module(module_name)
            fn = getattr(module, function_name)
            result = fn.__wrapped__ if wrapped else fn
            return cast(Callable[..., Any], result)

        except (ValueError, ImportError, AttributeError) as e:
            # edge case where we are trying to resolve a function from __main__ and worker is running from main
            global cache_main_module
            if not cache_main_module:
                _main_path = os.path.relpath(sys.argv[0])[:-3]  # this handles "python -m app.main" because with "-m" sys.argv[0] is absolute path
                cache_main_module = _main_path.replace(os.sep, ".")  # replace handles situations when user runs "python app/main.py"

            if module_name and function_name and module_name == cache_main_module and "__main__" in sys.modules:  # noqa: SIM102
                if fn := getattr(sys.modules["__main__"], function_name, None):
                    result = fn.__wrapped__ if wrapped else fn
                    return cast(Callable[..., Any], result)

            raise ValueError(f"Cannot resolve function: {func}") from e

    @staticmethod
    async def process_function_parameters(
        func: Callable[..., Any],
        args: list[Any],
        kwargs: dict[str, Any],
        task: TaskInternal | None = None,
    ) -> tuple[list[Any], dict[str, Any]]:

        signature = cache_signature.get(func)
        if not signature:
            signature = inspect.signature(func)
            cache_signature[func] = signature

        final_args = []
        final_kwargs = kwargs.copy()
        remaining_args = args.copy()

        for param_name, param in list(signature.parameters.items()):
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

        if param.annotation == inspect.Parameter.empty:
            return False

        return param.annotation is Task or param.annotation == 'Task'


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
