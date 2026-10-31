"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import inspect
from collections.abc import Callable
from typing import Any

from pydantic import TypeAdapter, ValidationError

from sheppy.models import CURRENT_TASK

from .parameters import get_cached_signature, get_depends_from_param


def validate_input(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any]
) -> tuple[tuple[Any, ...], dict[str, Any]]:

    signature = get_cached_signature(func)

    final_args = []
    final_kwargs = {}
    remaining_args = list(args)
    remaining_kwargs = dict(kwargs)

    for param_name, param in signature.parameters.items():

        # task self injection
        if _is_task_injection(param):
            if param_name in remaining_kwargs:
                raise ValidationError.from_exception_data(
                    f"Cannot provide value for Task injection parameter '{param_name}'", line_errors=[]
                )
            continue

        # skip Depends unless value is provided
        if _is_depends_parameter(param) and not (
            (remaining_args and param.kind != inspect.Parameter.KEYWORD_ONLY)
            or param_name in remaining_kwargs
        ):
            continue

        # handle *args
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            final_args.extend(remaining_args)
            remaining_args.clear()
            continue

        # handle **kwargs
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            final_kwargs.update(remaining_kwargs)
            remaining_kwargs.clear()
            continue

        # positional args or kwargs
        if remaining_args and param.kind != inspect.Parameter.KEYWORD_ONLY:
            validated = _validate_value(remaining_args.pop(0), param.annotation)
            final_args.append(validated)
            continue

        if param_name in remaining_kwargs:
            # edge case - positional-only functions
            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                raise ValidationError.from_exception_data(
                    f"Cannot pass '{param_name}' as keyword argument (positional-only)", line_errors=[]
                )
            validated = _validate_value(remaining_kwargs.pop(param_name), param.annotation)
            final_kwargs[param_name] = validated
            continue

        if param.default != inspect.Parameter.empty:
            continue

        raise ValidationError.from_exception_data(
            f"No value provided for parameter '{param_name}'", line_errors=[]
        )

    has_var_positional = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in signature.parameters.values())
    has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values())

    if remaining_args and not has_var_positional:
        raise ValidationError.from_exception_data(
            f"Too many positional arguments: expected {len(args) - len(remaining_args)}, got {len(args)}", line_errors=[]
        )

    if remaining_kwargs and not has_var_keyword:
        raise ValidationError.from_exception_data(
            f"Unexpected keyword arguments: {', '.join(remaining_kwargs.keys())}", line_errors=[]
        )

    return tuple(final_args), final_kwargs


def _is_task_injection(param: inspect.Parameter) -> bool:
    return param.default is CURRENT_TASK


def _is_depends_parameter(param: inspect.Parameter) -> bool:
    return get_depends_from_param(param) is not None


def _validate_value(value: Any, annotation: Any) -> Any:
    if annotation == inspect.Parameter.empty:
        return value

    return TypeAdapter(annotation).validate_python(value)
