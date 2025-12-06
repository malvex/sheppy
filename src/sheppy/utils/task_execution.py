"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import inspect
import socket
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, get_args, get_origin
from uuid import uuid4

import anyio
from pydantic import PydanticSchemaGenerationError, TypeAdapter

from ..models import CURRENT_TASK, Task
from .fastapi import Depends
from .functions import reconstruct_result, resolve_function

cache_signature: dict[Callable[..., Any], inspect.Signature] = {}


def generate_unique_worker_id(prefix: str) -> str:
    return f"{prefix}-{socket.gethostname()}-{str(uuid4())[:8]}"


class TaskProcessor:
    def __init__(
            self,
            dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None,
    ):
        self.dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] = dependency_overrides or {}

    async def process_task(self, task: Task, worker_id: str) -> tuple[Exception | None, "Task"]:
        task, _generators = await TaskProcessor.process_pre_task_middleware(task)

        try:
            result = await self._execute_task_function(task)
            exception = None
            task = TaskProcessor.mark_completed(task, result, worker_id)

        except Exception as e:
            task = TaskProcessor.mark_failed(task, e)
            exception = e  # temporary

            if task.is_retriable:
                task = TaskProcessor.handle_retry(task)

        task = await TaskProcessor.process_post_task_middleware(task, _generators)

        return exception, task

    async def _execute_task_function(self, task: Task) -> Any:
        # resolve the function from its string representation
        func = resolve_function(task.spec.func)
        args = task.spec.args or ()
        kwargs = task.spec.kwargs or {}

        # validate all parameters, inject DI and Task
        final_args, final_kwargs = await self.process_function_parameters(func, args, kwargs, task)

        # async task
        if inspect.iscoroutinefunction(func):
            return await func(*final_args, **final_kwargs)

        # sync task
        return await anyio.to_thread.run_sync(lambda: func(*final_args, **final_kwargs))

    @staticmethod
    async def process_pre_task_middleware(task: Task) -> tuple[Task, list[Any]]:
        if not task.spec.middleware:
            return task, []

        _generators = []

        try:
            for middleware_string in task.spec.middleware:
                middleware = resolve_function(middleware_string, wrapped=False)
                gen = middleware(task)
                task = next(gen) or task
                _generators.append(gen)
        except Exception as e:
            raise Exception("Middleware error") from e

        return task, _generators

    @staticmethod
    async def process_post_task_middleware(task: Task, _generators: list[Any]) -> Task:
        if not _generators:
            return task

        try:
            for gen in _generators[::-1]:  # post task middleware goes in reverse order
                try:
                    task = gen.send(task) or task
                except StopIteration as e:
                    task = e.value or task
        except Exception as e:
            raise Exception("Middleware error") from e

        return task

    @staticmethod
    def mark_completed(task: Task, result: Any, worker_id: str) -> Task:
        return task.model_copy(
            deep=True,
            update={
                "completed": True,
                # we reconstruct result here only to trigger result validation
                # before we store task as completed
                "result": reconstruct_result(task.spec.func, result),
                "error": None,
                "finished_at": datetime.now(timezone.utc),
            },
        )

    @staticmethod
    def mark_failed(task: Task, exception: Exception) -> Task:
        return task.model_copy(
            deep=True,
            update={
                "completed": False,
                "result": None,
                "error": f"{exception.__class__.__name__}: {exception}",
                "finished_at": datetime.now(timezone.utc),
            },
        )

    @staticmethod
    def handle_retry(task: Task) -> Task:
        if not task.should_retry:
            return task

        return task.model_copy(
            deep=True,
            update={
                "retry_count": task.retry_count + 1,
                "last_retry_at": datetime.now(timezone.utc),
                "next_retry_at": datetime.now(timezone.utc) + timedelta(seconds=TaskProcessor.calculate_retry_delay(task)),
                "finished_at": None
            },
        )

    @staticmethod
    def calculate_retry_delay(task: Task) -> float:
        if isinstance(task.config.retry_delay, float):
            return task.config.retry_delay  # constant delay for all retries
        if isinstance(task.config.retry_delay, list):
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

    async def process_function_parameters(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task: Task | None = None,
    ) -> tuple[list[Any], dict[str, Any]]:

        signature = cache_signature.get(func)
        if not signature:
            signature = inspect.signature(func)
            cache_signature[func] = signature

        final_args = []
        final_kwargs = kwargs.copy()
        remaining_args = list(args)

        for param_name, param in list(signature.parameters.items()):
            # current Task injection (current: Task = CURRENT_TASK)
            if task and TaskProcessor._is_task_injection(param):
                # inject positionally for positional params to maintain correct order
                if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    final_args.append(task)
                else:
                    final_kwargs[param_name] = task
                continue

            # validate positional args
            if remaining_args:
                final_args.append(TaskProcessor._validate(remaining_args.pop(0), param.annotation))
                continue

            # dependency injection
            if depends := TaskProcessor.get_depends_from_param(param):
                final_kwargs[param_name] = await self._resolve_dependency(depends.dependency)
                continue

            # validate kwargs
            if param_name in kwargs:
                final_kwargs[param_name] = TaskProcessor._validate(kwargs[param_name], param.annotation)

        return final_args, final_kwargs

    @staticmethod
    def _is_task_injection(param: inspect.Parameter) -> bool:
        return param.default is CURRENT_TASK


    @staticmethod
    def _validate(value: Any, annotation: Any) -> Any:
        if value is not None and annotation != inspect.Parameter.empty:
            try:
                return TypeAdapter(annotation).validate_python(value)
            except PydanticSchemaGenerationError:
                pass

        return value

    async def _resolve_dependency(self, func: Callable[..., Any]) -> Any:
        func = self.dependency_overrides.get(func, func)

        # resolve nested dependencies
        _, kwargs = await self.process_function_parameters(func, (), {}, None)

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
