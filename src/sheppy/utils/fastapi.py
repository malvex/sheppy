"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

__all__ = ["Depends"]

from collections.abc import Callable
from typing import Any

try:
    from fastapi.params import Depends  # type: ignore
except ImportError:
    # fallback implementation if FastAPI is not installed
    class Depends:  # type: ignore[no-redef]
        def __init__(
            self, dependency: Callable[..., Any] | None = None, /
        ):
            self.dependency = dependency
