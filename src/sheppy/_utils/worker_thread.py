"""
This file contains utility functions meant for internal use only. Expect breaking changes if you use them directly.
"""

import asyncio
import importlib
from collections.abc import Callable
from types import ModuleType
from typing import Any

anyio: ModuleType | None
try:
    anyio = importlib.import_module("anyio")
except ImportError:
    anyio = None


def _run_in_worker_thread(fn: Callable[[], Any]) -> Any:
    # use anyio when available (asyncer compatibility)
    if anyio is not None:
        return anyio.to_thread.run_sync(fn)
    return asyncio.to_thread(fn)


def _run_context_exit_in_worker_thread(fn: Callable[[], Any]) -> Any:
    if anyio is not None:
        return anyio.to_thread.run_sync(fn, limiter=anyio.CapacityLimiter(1))
    return asyncio.to_thread(fn)
