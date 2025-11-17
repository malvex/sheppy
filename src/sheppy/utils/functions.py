"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import importlib
import os
import sys
from collections.abc import Callable
from typing import Any, cast, get_type_hints

from pydantic import PydanticSchemaGenerationError, TypeAdapter

cache_main_module: str | None = None


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


def reconstruct_result(func_s: str, result: Any) -> Any:
    if result is None:
        return result

    try:
        func = resolve_function(func_s)

        if return_type := get_type_hints(func).get("return"):
            return TypeAdapter(return_type).validate_python(result)
    except PydanticSchemaGenerationError:
        # possible exceptions here:
        # - TypeError: get_type_hints fails
        # - ValueError: resolve_function fails
        # - PydanticSchemaGenerationError: TypeAdapter fails
        pass

    return result
