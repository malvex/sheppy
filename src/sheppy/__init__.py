try:
    from fastapi import Depends  # type: ignore
except ImportError:
    from .utils.dependency_injection import Depends

from .backend import Backend, BackendError, MemoryBackend, RedisBackend
from .queue import Queue
from .task import Task, task
from .testqueue import TestQueue
from .worker import Worker

__version__ = "0.1.0"

__all__ = [
    # fastapi
    "Depends",
    # task
    "task", "Task",
    # queue
    "Queue",
    # testqueue
    "TestQueue",
    # worker
    "Worker",
    # backend/
    "Backend", "MemoryBackend", "RedisBackend", "BackendError",
]
