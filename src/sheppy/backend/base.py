from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any


class BackendError(Exception):
    pass


class Backend(ABC):

    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        pass

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        pass

    @abstractmethod
    async def append(self, queue_name: str, tasks: list[dict[str, Any]], unique: bool = True) -> list[bool]:
        pass

    @abstractmethod
    async def pop(self, queue_name: str, limit: int = 1, timeout: float | None = None) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def get_tasks(self, queue_name: str, task_ids: list[str]) -> dict[str, dict[str, Any]]:
        pass

    @abstractmethod
    async def get_all_tasks(self, queue_name: str) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def get_pending(self, queue_name: str, count: int = 1) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def schedule(self, queue_name: str, task_data: dict[str, Any], at: datetime, unique: bool = True) -> bool:
        pass

    @abstractmethod
    async def get_scheduled(self, queue_name: str) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def pop_scheduled(self, queue_name: str, now: datetime | None = None) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def store_result(self, queue_name: str, task_data: dict[str, Any]) -> bool:
        pass

    @abstractmethod
    async def get_results(self, queue_name: str, task_ids: list[str], timeout: float | None = None) -> dict[str, dict[str, Any]]:
        pass

    @abstractmethod
    async def size(self, queue_name: str) -> int:
        pass

    @abstractmethod
    async def clear(self, queue_name: str) -> int:
        pass

    @abstractmethod
    async def get_stats(self, queue_name: str) -> dict[str, int]:
        pass

    @abstractmethod
    async def list_queues(self) -> dict[str, int]:
        pass

    @abstractmethod
    async def add_cron(self, queue_name: str, deterministic_id: str, task_cron: dict[str, Any]) -> bool:
        pass

    @abstractmethod
    async def delete_cron(self, queue_name: str, deterministic_id: str) -> bool:
        pass

    @abstractmethod
    async def get_crons(self, queue_name: str) -> list[dict[str, Any]]:
        pass
