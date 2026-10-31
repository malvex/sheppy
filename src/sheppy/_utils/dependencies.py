"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import inspect
import sys
from collections.abc import Callable
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from types import TracebackType
from typing import Any, cast

if sys.version_info >= (3, 13):
    from inspect import iscoroutinefunction
else:
    from asyncio import iscoroutinefunction

from ..exceptions import DependencyScopeError
from ..models import Task
from .parameters import process_parameters
from .worker_thread import _run_context_exit_in_worker_thread, _run_in_worker_thread


def _is_gen_callable(call: Callable[..., Any]) -> bool:
    if inspect.isgeneratorfunction(call):
        return True
    dunder_call = getattr(call, "__call__", None)  # noqa: B004
    return inspect.isgeneratorfunction(dunder_call)


def _is_async_gen_callable(call: Callable[..., Any]) -> bool:
    if inspect.isasyncgenfunction(call):
        return True
    dunder_call = getattr(call, "__call__", None)  # noqa: B004
    return inspect.isasyncgenfunction(dunder_call)


def _is_coroutine_callable(call: Callable[..., Any]) -> bool:
    if iscoroutinefunction(call):
        return True
    if inspect.isclass(call):
        return False
    dunder_call = getattr(call, "__call__", None)  # noqa: B004
    return iscoroutinefunction(dunder_call)


class DependencyContext:
    def __init__(self) -> None:
        self.request_stack = AsyncExitStack()
        self.function_stack: AsyncExitStack | None = None
        self.cache: dict[tuple[Callable[..., Any], str], Any] = {}

    async def __aenter__(self) -> "DependencyContext":
        await self.request_stack.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return bool(await self.request_stack.__aexit__(exc_type, exc, tb))

    def function_scope(self) -> "_FunctionScope":
        return _FunctionScope(self)


class _FunctionScope:
    def __init__(self, context: DependencyContext):
        self.context = context
        self.stack: AsyncExitStack | None = None

    async def __aenter__(self) -> None:
        self.stack = AsyncExitStack()
        await self.stack.__aenter__()
        self.context.function_stack = self.stack

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        assert self.stack is not None
        try:
            return bool(await self.stack.__aexit__(exc_type, exc, tb))
        finally:
            self.context.function_stack = None


class DependencyResolver:
    def __init__(
        self,
        dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None,
    ):
        self.dependency_overrides = dependency_overrides if dependency_overrides is not None else {}

    def request_scope(self) -> DependencyContext:
        return DependencyContext()

    async def process_function_parameters(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task: Task | None,
        dependency_context: DependencyContext,
        parent_scope: str | None = None,
        include_defaults: bool = False,
    ) -> tuple[list[Any], dict[str, Any]]:
        async def dependency_handler(parent_func: Callable[..., Any], depends: Any) -> Any:
            if parent_scope == "request" and depends.scope == "function":
                dependency_name = getattr(parent_func, "__name__", "<unnamed_callable>")
                raise DependencyScopeError(
                    f'The dependency "{dependency_name}" has a scope of "request", '
                    'it cannot depend on dependencies with scope "function".'
                )
            return await self._resolve_dependency(depends, dependency_context, task)

        return await process_parameters(
            func, args, kwargs, task, dependency_handler, include_defaults=include_defaults
        )

    async def _resolve_dependency(self, depends: Any, context: DependencyContext, task: Task | None) -> Any:
        original_func = depends.dependency
        if original_func is None:
            raise TypeError("Dependency callable cannot be None")

        func = self.dependency_overrides.get(original_func, original_func)
        parent_scope = (depends.scope or "request") if self._is_generator(func) else None

        # resolve nested dependencies
        args, kwargs = await self.process_function_parameters(
            func, (), {}, task, context, parent_scope, include_defaults=True
        )

        cache_key = (original_func, self._get_cache_scope(original_func, depends))
        if depends.use_cache and cache_key in context.cache:
            return context.cache[cache_key]

        value = await self._execute_dependency(func, args, kwargs, depends, context)
        if cache_key not in context.cache:
            context.cache[cache_key] = value
        return value

    async def _execute_dependency(
        self,
        func: Callable[..., Any],
        args: list[Any],
        kwargs: dict[str, Any],
        depends: Any,
        context: DependencyContext,
    ) -> Any:
        # execute dependency
        if _is_async_gen_callable(func):
            stack = self._get_dependency_stack(depends, context)
            return await stack.enter_async_context(asynccontextmanager(func)(*args, **kwargs))

        if _is_gen_callable(func):
            stack = self._get_dependency_stack(depends, context)
            cm = contextmanager(func)(*args, **kwargs)
            return await self._enter_sync_context(cm, stack)

        if _is_coroutine_callable(func):
            return await func(*args, **kwargs)

        return await _run_in_worker_thread(lambda: func(*args, **kwargs))

    @staticmethod
    async def _enter_sync_context(cm: Any, stack: AsyncExitStack) -> Any:
        value = await _run_in_worker_thread(cm.__enter__)

        async def exit_context(
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> bool:
            return bool(await _run_context_exit_in_worker_thread(lambda: cm.__exit__(exc_type, exc, tb)))

        stack.push_async_exit(exit_context)
        return value

    @staticmethod
    def _get_dependency_stack(depends: Any, context: DependencyContext) -> AsyncExitStack:
        if depends.scope == "function":
            if context.function_stack is None:
                raise RuntimeError("Function-scoped dependency used outside a task execution context")
            return context.function_stack
        return context.request_stack

    @staticmethod
    def _get_cache_scope(func: Callable[..., Any], depends: Any) -> str:
        return cast(str, depends.scope or ("request" if DependencyResolver._is_generator(func) else ""))

    @staticmethod
    def _is_generator(func: Callable[..., Any]) -> bool:
        return _is_async_gen_callable(func) or _is_gen_callable(func)
