"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""
import asyncio
import inspect
import logging
import socket
from collections.abc import AsyncGenerator, Callable, Generator
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, get_args, get_origin
from uuid import uuid4

import anyio
from pydantic import PydanticSchemaGenerationError, TypeAdapter

from ..exceptions import MiddlewareError, TaskTimeoutError
from ..models import CURRENT_TASK, Task, TaskStatus
from ..protocols import (
    AsyncMiddlewareProtocol,
    MiddlewareProtocol,
    TaskProcessorProtocol,
)
from ..queue import Queue
from .fastapi import Depends
from .functions import reconstruct_result, resolve_function

cache_signature: dict[Callable[..., Any], inspect.Signature] = {}


def generate_unique_worker_id(prefix: str) -> str:
    return f"{prefix}-{socket.gethostname()}-{str(uuid4())[:8]}"


class TaskChainingMiddleware(AsyncMiddlewareProtocol):
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    async def __call__(self, task: Task, queue: "Queue") -> AsyncGenerator[None, tuple[Task, Exception | None]]:
        task, exception = yield

        # quick skip on error
        if exception:
            return

        if isinstance(task.result, Task):
            self.logger.info(f"Adding task {task.id} into Queue (Chained Task)")
            await queue.add(task.result)

        elif isinstance(task.result, list) and isinstance(task.result[0], Task):
            _tasks = [item for item in task.result if isinstance(item, Task)]
            for _task in _tasks:
                self.logger.info(f"Adding task {_task.id} into Queue (Chained Task)")
            await queue.add(_tasks)


class LoggingMiddleware(MiddlewareProtocol):
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.prefix = "<Worker> "

    def __call__(self, task: Task, queue: "Queue") -> Generator[None, tuple[Task, Exception | None], None]:
        self.logger.info(self.prefix + f"Processing task {task.id} ({task.spec.func})")
        task, exception = yield

        match task.status:
            case 'completed':
                self.logger.info(self.prefix + f"Task {task.id} completed successfully")

            case 'retrying':
                # retriable task - reschedule
                self.logger.warning(self.prefix + f"Task {task.id} failed (attempt {task.retry_count}/{task.config.retry}), scheduling retry at {task.next_retry_at}")
            case 'failed':
                if task.is_retriable and not task.should_retry:
                    # retriable task - final failure
                    self.logger.error(self.prefix + f"Task {task.id} failed after {task.retry_count} retries: {exception}")
                else:
                    # non retriable task
                    self.logger.error(self.prefix + f"Task {task.id} failed: {exception}")


class TaskProcessor(TaskProcessorProtocol):
    def __init__(
            self,
            /,
            middleware: list[AsyncMiddlewareProtocol | MiddlewareProtocol] | None = None,
            dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None,
    ):
        self.middleware = middleware or []
        self.dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] = dependency_overrides or {}

    async def process_task(self, task: Task, queue: Queue, worker_id: str) -> tuple[Exception | None, Task]:
        task, _generators = await self._preprocess(task, queue)
        result = None
        exception = None

        try:
            result = await self._execute_task_function(task)
            task = self.mark_completed(task, result)
        except Exception as e:
            task = self.mark_failed(task, e)
            exception = e

            if task.is_retriable:
                task = TaskProcessor.handle_retry(task, exception)

        task = await self._postprocess(task, exception, _generators)

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
            coro = func(*final_args, **final_kwargs)
        else:
            # sync task
            coro = anyio.to_thread.run_sync(lambda: func(*final_args, **final_kwargs))

        if task.config.timeout is not None:
            try:
                return await asyncio.wait_for(coro, timeout=task.config.timeout)
            except asyncio.TimeoutError:
                raise TaskTimeoutError(f"Task exceeded timeout of {task.config.timeout} seconds") from None

        return await coro

    async def _preprocess(self, task: Task, queue: Queue) -> tuple[Task, list[Any]]:
        middlewares = self.middleware + (task.spec.middleware or [])

        if not middlewares:
            return task, []

        _generators = []

        try:
            for middleware_string in middlewares:
                middleware = resolve_function(middleware_string, wrapped=False) if isinstance(middleware_string, str) else middleware_string
                gen = middleware(task, queue)
                if inspect.isasyncgen(gen):
                    task = await anext(gen) or task
                else:
                    task = next(gen) or task  # type: ignore
                _generators.append(gen)
        except Exception as e:
            raise MiddlewareError("Pre-middleware error") from e

        return task, _generators

    async def _postprocess(self, task: Task, exception: Exception | None, _generators: list[Any]) -> Task:
        if not _generators:
            return task

        try:
            for gen in _generators[::-1]:  # post task middleware goes in reverse order
                try:
                    if inspect.isasyncgen(gen):
                        task = await gen.asend((task, exception)) or task
                    else:
                        task = gen.send((task, exception)) or task
                except StopIteration as e:
                    task = e.value or task
                except StopAsyncIteration:
                    pass
        except Exception as e:
            raise MiddlewareError("Post-middleware error") from e

        return task

    @staticmethod
    def update_task_status(task: Task, status: TaskStatus) -> Task:
        task.__dict__['status'] = status
        return task

    @staticmethod
    def mark_completed(task: Task, result: Any, status: TaskStatus = 'completed') -> Task:
        task.__dict__["status"] = status
        # we reconstruct result here only to trigger result validation
        # before we store task as completed
        task.__dict__["result"] = reconstruct_result(task.spec.func, result)
        task.__dict__["error"] = None
        task.__dict__["finished_at"] = datetime.now(timezone.utc)

        return task

    @staticmethod
    def mark_failed(task: Task, exception: Exception, status: TaskStatus = 'failed') -> Task:
        task.__dict__["status"] = status
        task.__dict__["result"] = None
        task.__dict__["error"] = f"{exception.__class__.__name__}: {exception}"
        task.__dict__["finished_at"] = datetime.now(timezone.utc)

        return task

    @staticmethod
    def handle_retry(task: Task, exception: Exception) -> Task:
        if not task.should_retry:
            return task

        if isinstance(exception, TaskTimeoutError) and not task.config.retry_on_timeout:
            return task

        task.__dict__["status"] = "retrying"
        task.__dict__["retry_count"] = task.retry_count + 1
        task.__dict__["last_retry_at"] = datetime.now(timezone.utc)
        task.__dict__["next_retry_at"] = datetime.now(timezone.utc) + timedelta(seconds=TaskProcessor.calculate_retry_delay(task))
        task.__dict__["finished_at"] = None

        return task

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
            if task and param.default is CURRENT_TASK:
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
