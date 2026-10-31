"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

__all__ = ["Depends", "DependsType", "SecurityType"]

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

try:
    from fastapi.params import Depends as DependsType  # type: ignore[import-not-found,unused-ignore]
    from fastapi.params import Security as SecurityType  # type: ignore[import-not-found,unused-ignore]
except ImportError:
    # fallback implementation if FastAPI is not installed
    @dataclass(frozen=True)
    class DependsType:  # type: ignore[no-redef]
        dependency: Callable[..., Any] | None = None
        use_cache: bool = True
        scope: Literal["function", "request"] | None = None

    class SecurityType:  # type: ignore[no-redef]
        pass


def Depends(
    dependency: Callable[..., Any] | None = None,
    *,
    use_cache: bool = True,
    scope: Literal["function", "request"] | None = None,
) -> DependsType:
    return DependsType(dependency=dependency, use_cache=use_cache, scope=scope)
