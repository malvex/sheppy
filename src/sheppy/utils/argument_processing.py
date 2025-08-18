"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pydantic import PydanticSchemaGenerationError, TypeAdapter, ValidationError

from .dependency_injection import is_depends_param, is_task_injection_param

if TYPE_CHECKING:
    from ..task import Task


def _should_skip_param(param: inspect.Parameter) -> bool:
    """Check if a parameter should be skipped when processing task arguments"""
    return is_depends_param(param) or is_task_injection_param(param)


def _validate_and_dump(value: Any, param_type: type) -> Any:
    """Validate value against type annotation"""

    # If no type annotation, can't validate - return as-is
    if param_type == inspect.Parameter.empty:
        return value

    try:
        # TypeAdapter is Pydantic's way to validate arbitrary types (not just BaseModel)
        # It handles Optional, List, Dict, Union, Pydantic models, and standard types
        adapter: TypeAdapter[Any] = TypeAdapter(param_type)
        validated = adapter.validate_python(value)

        return adapter.dump_python(validated)
    except ValidationError:
        # Let Pydantic validation errors bubble up so caller knows what went wrong
        raise
    except Exception:
        # Not a type that Pydantic can handle (e.g., custom classes without validation)
        # Return the value unchanged
        return value


def process_task_arguments(func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    """Process and validate arguments when creating a task, converting to serializable format"""

    # Get function signature to understand expected parameters
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    # Filter out params that don't consume user-provided arguments
    # This excludes Depends() params and Task injection (self: Task) params
    # These will be handled separately during task execution
    consumable_params = [p for p in params if not _should_skip_param(p)]

    # Process positional arguments
    processed_args = []
    for i, arg in enumerate(args):
        # Match positional args to consumable parameters for validation
        if i < len(consumable_params):
            param = consumable_params[i]
            # Validate and convert to serializable format based on type annotation
            arg = _validate_and_dump(arg, param.annotation)
        processed_args.append(arg)

    # Process keyword arguments
    processed_kwargs = {}
    # Check if function accepts **kwargs (VAR_KEYWORD parameter)
    has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)

    for key, value in kwargs.items():
        if key in sig.parameters:
            param = sig.parameters[key]
            # Skip parameters that are handled by dependency injection or task injection
            if _should_skip_param(param):
                continue

            # Validate and convert to serializable format
            value = _validate_and_dump(value, param.annotation)
            processed_kwargs[key] = value
        elif has_var_keyword:
            # Function accepts **kwargs, so pass through extra keyword arguments
            # These won't be validated since we don't know their expected types
            processed_kwargs[key] = value

    return processed_args, processed_kwargs


def prepare_task_arguments(
    task: "Task",
    resolved_values: dict[str, Any],
    func: Callable[..., Any]
) -> tuple[list[Any], dict[str, Any]]:
    """Prepare arguments for task execution, reconstructing objects from serialized data"""

    # Get function signature to understand parameter positions and types
    sig = inspect.signature(func)
    final_args: list[Any] = []
    final_kwargs: dict[str, Any] = {}

    # Track positional arg consumption to correctly map stored args to parameters
    num_positional = len(task.internal.args) if task.internal.args else 0
    positional_consumed = 0

    # Process each parameter in order to build final args/kwargs for execution
    for param_name, param in sig.parameters.items():
        # Handle task injection first (doesn't use resolved value)
        if is_task_injection_param(param):
            # Task injection (self: Task) - inject the task instance itself
            # Maintain positional vs keyword based on original call pattern
            if positional_consumed < num_positional:
                final_args.append(task)
            else:
                final_kwargs[param_name] = task
            continue

        # Skip if parameter wasn't provided at all
        if param_name not in resolved_values:
            # But still track positional consumption
            if positional_consumed < num_positional and not is_depends_param(param):
                positional_consumed += 1
            continue

        # Get the resolved value (could be None if explicitly set)
        value = resolved_values[param_name]

        # Reconstruct Pydantic models
        if value is not None and param.annotation != inspect.Parameter.empty:
            try:
                value = TypeAdapter(param.annotation).validate_python(value)
            except PydanticSchemaGenerationError:
                pass  # Not a Pydantic type or already correct type, use as-is

        # Handle positional arguments
        if positional_consumed < num_positional and not is_depends_param(param):
            positional_consumed += 1
            final_args.append(value)
            continue

        # Dependencies and keyword arguments are always passed as kwargs
        final_kwargs[param_name] = value

    return final_args, final_kwargs
