"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import inspect
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import replace
from typing import Annotated, Any, get_args, get_origin

from pydantic import PydanticSchemaGenerationError, TypeAdapter

from ..models import CURRENT_TASK, Task
from .fastapi import DependsType, SecurityType

DependencyHandler = Callable[[Callable[..., Any], Any], Awaitable[Any]]

cache_signature: dict[Callable[..., Any], inspect.Signature] = {}


def get_typed_signature(call: Callable[..., Any]) -> inspect.Signature:
    try:
        return inspect.signature(call, eval_str=True)
    except NameError:
        return inspect.signature(call)


def get_cached_signature(call: Callable[..., Any]) -> inspect.Signature:
    signature = cache_signature.get(call)
    if signature is None:
        signature = get_typed_signature(call)
        cache_signature[call] = signature
    return signature


async def process_parameters(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    task: Task | None,
    dependency_handler: DependencyHandler,
    include_defaults: bool = False,
) -> tuple[list[Any], dict[str, Any]]:
    signature = get_cached_signature(func)
    missing = object()
    values: dict[str, Any] = {}
    positional_parameters: list[inspect.Parameter] = []
    variadic_args: list[Any] = []
    positional_cutoff = 0
    remaining_args = list(args)

    for param_name, param in signature.parameters.items():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            continue

         # preserve variadic arguments (my_task(*values: int))
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            if remaining_args:
                variadic_args = [_validate(value, param.annotation) for value in remaining_args]
                positional_cutoff = len(positional_parameters)
                remaining_args.clear()
            continue

        if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            positional_parameters.append(param)

        value: Any = missing
        # current Task injection (current: Task = CURRENT_TASK)
        if task is not None and param.default is CURRENT_TASK:
            value = task

        # validate positional args
        elif remaining_args:
            value = _validate(remaining_args.pop(0), param.annotation)
            if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                positional_cutoff = len(positional_parameters)

        # validate kwargs
        elif param_name in kwargs:
            value = _validate(kwargs[param_name], param.annotation)

        # dependency injection
        elif depends := get_depends_from_param(param):
            value = await dependency_handler(func, depends)
        elif include_defaults and param.default is not inspect.Parameter.empty:
            value = deepcopy(param.default)

        if value is not missing:
            values[param_name] = value
            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                positional_cutoff = len(positional_parameters)

    # inject positionally for positional params to maintain correct order
    final_args = []
    final_kwargs = kwargs.copy()
    for index, param in enumerate(positional_parameters):
        if index >= positional_cutoff:
            if param.name in values:
                final_kwargs[param.name] = values[param.name]
            continue

        if param.name in values:
            final_args.append(values[param.name])
        elif param.default is not inspect.Parameter.empty:
            final_args.append(deepcopy(param.default) if include_defaults else param.default)
        else:
            raise TypeError(f"No value provided for parameter {param.name!r}")

    final_args.extend(variadic_args)
    for param_name, param in signature.parameters.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY and param_name in values:
            final_kwargs[param_name] = values[param_name]

    return final_args, final_kwargs


def _validate(value: Any, annotation: Any) -> Any:
    if value is not None and annotation != inspect.Parameter.empty:
        try:
            return TypeAdapter(annotation).validate_python(value)
        except PydanticSchemaGenerationError:
            pass

    return value


def get_depends_from_param(param: inspect.Parameter) -> Any | None:
    annotation = param.annotation
    type_annotation = annotation
    depends = None

    # Annotated style
    if get_origin(annotation) is Annotated:
        args = get_args(annotation)
        type_annotation = args[0]
        for arg in args[1:]:
            if isinstance(arg, SecurityType):
                raise TypeError("`Security` is not supported by Sheppy; use `Depends` instead")
            if isinstance(arg, DependsType):
                depends = arg

    if isinstance(param.default, SecurityType):
        raise TypeError("`Security` is not supported by Sheppy; use `Depends` instead")
    if isinstance(param.default, DependsType):
        assert depends is None, (
            "Cannot specify `Depends` in `Annotated` and default value"
            f" together for {param.name!r}"
        )
        depends = param.default

    if depends is not None:
        return _infer_dependency(depends, type_annotation)
    return None


def _infer_dependency(depends: Any, annotation: Any) -> Any:
    if depends.dependency is None and annotation is not inspect.Parameter.empty:
        return replace(depends, dependency=annotation)
    return depends
