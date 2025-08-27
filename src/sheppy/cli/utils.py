import logging
from enum import Enum

from rich.console import Console

from sheppy import Backend, MemoryBackend, RedisBackend

console = Console()


class LogLevel(str, Enum):
    """Log level enum for CLI arguments."""
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"

    def to_logging_level(self) -> int:
        """Convert to Python logging level."""
        return getattr(logging, self.value.upper())  # type: ignore[no-any-return]


class BackendType(str, Enum):
    """Backend type enum."""
    redis = "redis"


def get_backend(backend_type: BackendType, redis_url: str) -> Backend:
    """Create backend instance based on type."""
    if backend_type == BackendType.redis:
        return RedisBackend(redis_url)

    elif backend_type == BackendType.memory:
        return MemoryBackend()

    else:
        raise ValueError(f"Unknown backend: {backend_type}")
