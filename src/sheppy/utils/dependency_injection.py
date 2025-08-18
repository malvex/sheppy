"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

__all__ = ["Depends", "DependencyResolver"]

import inspect
from collections.abc import Callable
from typing import Annotated, Any, get_args, get_origin

import anyio

try:
    from fastapi.params import Depends  # type: ignore
except ImportError:
    # fallback to support FastAPI style of handling dependencies

    class Depends:  # type: ignore[no-redef]
        def __init__(
            self, dependency: Callable[..., Any] | None = None, *, use_cache: bool = True
        ):
            self.dependency = dependency
            self.use_cache = use_cache

        def __repr__(self) -> str:
            attr = getattr(self.dependency, "__name__", type(self.dependency).__name__)
            cache = "" if self.use_cache else ", use_cache=False"
            return f"{self.__class__.__name__}({attr}{cache})"



def is_task_injection_param(param: inspect.Parameter) -> bool:
    """Check if a parameter is for Task injection (self: Task pattern)"""

    # Task injection uses 'self' parameter name by convention
    if param.name != 'self':
        return False

    # Must have a type annotation
    if param.annotation == inspect.Parameter.empty:
        return False

    annotation = param.annotation

    # Handle string annotations (forward references)
    if isinstance(annotation, str):
        return annotation == 'Task'

    # Handle actual class - check by name and module to avoid importing Task
    return bool(annotation.__name__ == 'Task' and annotation.__module__ == 'sheppy.task')


def get_depends_from_param(param: inspect.Parameter) -> Any | None:
    """Extract Depends() object from a function parameter"""
    # Check default value style: param = Depends(func) - Most common pattern in FastAPI
    if param.default != inspect.Parameter.empty and isinstance(param.default, Depends):
        return param.default

    # Check for Annotated style: param: Annotated[Type, Depends(...)]
    # Newer pattern that keeps type separate from dependency
    if get_origin(param.annotation) is Annotated:
        # get_args returns tuple: (Type, metadata1, metadata2, ...)
        args = get_args(param.annotation)
        # Skip first element (the type), check metadata elements for Depends
        for arg in args[1:]:  # Skip the type, check metadata
            if isinstance(arg, Depends):
                return arg

    return None


def is_depends_param(param: inspect.Parameter) -> bool:
    """Check if a parameter uses FastAPI dependency injection"""
    return get_depends_from_param(param) is not None


class DependencyResolver:
    """Handles FastAPI-style dependency injection for tasks"""

    def __init__(
        self,
        dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None
    ):
        # Optional overrides for tests
        self.dependency_overrides = dependency_overrides or {}

    async def _execute_dependency(self, func: Callable[..., Any], kwargs: dict[str, Any]) -> Any:
        """Execute a dependency function (sync or async, regular or generator)"""

        # Check if it's an async function (async def func())
        if inspect.iscoroutinefunction(func):
            return await func(**kwargs)

        # Check if it's an async generator (async def func() with yield)
        if inspect.isasyncgenfunction(func):
            return await func(**kwargs).__anext__()

        # Sync functions must run in thread pool to avoid blocking event loop
        if inspect.isgeneratorfunction(func):
            # Sync generator - create and get first value
            gen = await anyio.to_thread.run_sync(lambda: func(**kwargs))
            # Get first yielded value in thread pool
            return await anyio.to_thread.run_sync(next, gen)

        # Regular sync function - execute in thread pool to avoid blocking
        return await anyio.to_thread.run_sync(lambda: func(**kwargs))

    def _map_positional_args(self, sig: inspect.Signature, args: tuple[Any, ...]) -> dict[str, Any]:
        """Map positional arguments to parameter names"""
        resolved = {}
        arg_index = 0

        # Process parameters in definition order (important for positional mapping)
        for param_name, param in sig.parameters.items():
            # Skip special parameters that don't consume user-provided arguments
            # These are handled separately (dependencies resolved, Task injected)
            if is_task_injection_param(param) or is_depends_param(param):
                continue

            # Map positional argument to this parameter
            if arg_index < len(args):
                resolved[param_name] = args[arg_index]
                arg_index += 1
            else:
                # No more positional args to map
                break

        return resolved

    async def solve_dependencies(
        self,
        call: Callable[..., Any],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        dependency_cache: dict[tuple[Callable[..., Any], tuple[Any, ...]], Any] | None = None,
    ) -> dict[str, Any]:
        """Resolve all dependencies for a function call"""

        # Initialize defaults if not provided
        kwargs = kwargs or {}
        dependency_cache = dependency_cache or {}
        # Get function signature to analyze parameters
        sig = inspect.signature(call)

        # Start with positional args mapped to parameter names
        resolved = self._map_positional_args(sig, args)
        # Add provided kwargs (may override positional args)
        resolved.update(kwargs)

        # Resolve dependencies for parameters that don't have values yet
        for param_name, param in sig.parameters.items():
            # Skip if we already have a value for this parameter
            if param_name in resolved:
                continue

            # Check if this parameter has a Depends() declaration
            depends = get_depends_from_param(param)
            if not depends:
                # Not a dependency - use default value if available
                if param.default != inspect.Parameter.empty:
                    resolved[param_name] = param.default
                continue

            # Extract the dependency function from Depends()
            dep_func = depends.dependency
            if dep_func is None:
                continue

            # Check for test overrides (replace dep_func if overridden)
            actual_func = self.dependency_overrides.get(dep_func, dep_func)
            # Create cache key (function + args, but dependencies have no args)
            cache_key = (actual_func, ())

            # Check cache first if caching is enabled for this dependency
            if depends.use_cache and cache_key in dependency_cache:
                resolved[param_name] = dependency_cache[cache_key]
                continue

            # Recursively resolve nested dependencies
            # The dependency function might itself have dependencies
            nested_deps = await self.solve_dependencies(
                actual_func,
                dependency_cache=dependency_cache  # Share cache across nested calls
            )

            # Execute the dependency function with its resolved dependencies
            result = await self._execute_dependency(actual_func, nested_deps)

            # Cache result if caching is enabled
            if depends.use_cache:
                dependency_cache[cache_key] = result

            # Store resolved value for this parameter
            resolved[param_name] = result

        return resolved
