import asyncio
import contextlib
import json
from datetime import datetime, timezone
from typing import Any

from .._localkv.client import KVClient
from .._localkv.server import handle_client
from .base import Backend, BackendError


class LocalBackend(Backend):

    def __init__(self, host: str = "127.0.0.1", port: int = 17420, *, embedded: bool = False) -> None:
        self._host = host
        self._port = port
        self._embedded = embedded
        self._server: asyncio.Server | None = None
        self._server_task: asyncio.Task[None] | None = None
        self._client = KVClient(host, port)

    async def connect(self) -> None:
        if self._embedded:
            try:
                self._server = await asyncio.start_server(handle_client, self._host, self._port)
                self._server_task = asyncio.create_task(self._server.serve_forever())
            except OSError as e:
                raise BackendError(f"Failed to start embedded KV server on {self._host}:{self._port}: {e}") from e

            # give the server a moment to start
            await asyncio.sleep(0.01)

        try:
            await self._client.connect()
            await self._client.ping()
        except Exception as e:
            raise BackendError(f"Failed to connect to local server: {e}") from e

    async def disconnect(self) -> None:
        await self._client.close()

        if self._embedded:
            if self._server:
                self._server.close()
                await self._server.wait_closed()
                self._server = None

            if self._server_task:
                self._server_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._server_task
                self._server_task = None

    @property
    def is_connected(self) -> bool:
        return self._client.is_connected

    @property
    def client(self) -> KVClient:
        if not self.is_connected:
            raise BackendError("Not connected to local server")
        return self._client

    def _task_key(self, queue_name: str, task_id: str) -> str:
        return f"sheppy:{queue_name}:task:{task_id}"

    def _task_prefix(self, queue_name: str) -> str:
        return f"sheppy:{queue_name}:task:"

    def _queue_key(self, queue_name: str) -> str:
        return f"sheppy:{queue_name}:pending"

    def _scheduled_key(self, queue_name: str) -> str:
        return f"sheppy:{queue_name}:scheduled"

    def _cron_key(self, queue_name: str, cron_id: str) -> str:
        return f"sheppy:{queue_name}:cron:{cron_id}"

    def _cron_prefix(self, queue_name: str) -> str:
        return f"sheppy:{queue_name}:cron:"

    def _queue_prefix(self, queue_name: str) -> str:
        return f"sheppy:{queue_name}:"

    async def append(self, queue_name: str, tasks: list[dict[str, Any]], unique: bool = True) -> list[bool]:
        success = []

        for task in tasks:
            task_json = json.dumps(task)
            key = self._task_key(queue_name, task["id"])

            if unique:
                created = await self.client.create(key, task_json)
                success.append(created)
                if not created:
                    continue
            else:
                await self.client.set({key: task_json})
                success.append(True)

            await self.client.list_push(self._queue_key(queue_name), task["id"])

        return success

    async def pop(self, queue_name: str, limit: int = 1, timeout: float | None = None) -> list[dict[str, Any]]:
        queue_key = self._queue_key(queue_name)
        start_time = asyncio.get_event_loop().time()

        while True:
            tasks = []
            for _ in range(limit):
                task_id = await self.client.list_pop(queue_key)
                if not task_id:
                    break

                values = await self.client.get([self._task_key(queue_name, task_id)])
                task_json = values.get(self._task_key(queue_name, task_id))
                if task_json:
                    tasks.append(json.loads(task_json))

            if tasks:
                return tasks

            if timeout is None or timeout <= 0:
                return []

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                return []

            await asyncio.sleep(min(0.05, timeout - elapsed))

    async def get_pending(self, queue_name: str, count: int = 1) -> list[dict[str, Any]]:
        task_ids = await self.client.list_get(self._queue_key(queue_name), count)

        if not task_ids:
            return []

        keys = [self._task_key(queue_name, tid) for tid in task_ids]
        values = await self.client.get(keys)

        return [json.loads(v) for v in values.values() if v]

    async def size(self, queue_name: str) -> int:
        return await self.client.list_len(self._queue_key(queue_name))

    async def clear(self, queue_name: str) -> int:
        prefix = self._queue_prefix(queue_name)
        count = await self.client.len(self._task_prefix(queue_name))
        await self.client.clear(prefix)
        return count

    async def get_tasks(self, queue_name: str, task_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not task_ids:
            return {}

        keys = [self._task_key(queue_name, tid) for tid in task_ids]
        values = await self.client.get(keys)

        return {tid: json.loads(v) for tid, k in zip(task_ids, keys, strict=True) if (v := values.get(k)) is not None}

    async def schedule(self, queue_name: str, task_data: dict[str, Any], at: datetime, unique: bool = True) -> bool:
        task_json = json.dumps(task_data)
        key = self._task_key(queue_name, task_data["id"])

        if unique:
            created = await self.client.create(key, task_json)
            if not created:
                return False
        else:
            await self.client.set({key: task_json})

        await self.client.sorted_push(self._scheduled_key(queue_name), at.timestamp(), task_data["id"])
        return True

    async def pop_scheduled(self, queue_name: str, now: datetime | None = None) -> list[dict[str, Any]]:
        if now is None:
            now = datetime.now(timezone.utc)

        items = await self.client.sorted_pop(self._scheduled_key(queue_name), now.timestamp())

        if not items:
            return []

        keys = [self._task_key(queue_name, task_id) for _, task_id in items]
        values = await self.client.get(keys)

        return [json.loads(v) for v in values.values() if v]

    async def store_result(self, queue_name: str, task_data: dict[str, Any]) -> bool:
        key = self._task_key(queue_name, task_data["id"])
        await self.client.set({key: json.dumps(task_data)})
        return True

    async def get_results(self, queue_name: str, task_ids: list[str], timeout: float | None = None) -> dict[str, dict[str, Any]]:
        if not task_ids:
            return {}

        results: dict[str, dict[str, Any]] = {}
        remaining_ids = task_ids[:]
        start_time = asyncio.get_event_loop().time()

        while True:
            keys = [self._task_key(queue_name, tid) for tid in remaining_ids]
            values = await self.client.get(keys)

            for tid, key in zip(remaining_ids[:], keys, strict=True):
                task_json = values.get(key)
                if task_json:
                    task = json.loads(task_json)
                    if task.get("finished_at"):
                        results[tid] = task
                        remaining_ids.remove(tid)

            if not remaining_ids:
                return results

            if timeout is None or timeout < 0:
                return results

            if timeout == 0:
                await asyncio.sleep(0.05)
                continue

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                raise TimeoutError(f"Did not complete within {timeout} seconds")

            await asyncio.sleep(min(0.05, timeout - elapsed))

    async def get_stats(self, queue_name: str) -> dict[str, int]:
        pending = await self.client.list_len(self._queue_key(queue_name))
        scheduled = await self.client.sorted_len(self._scheduled_key(queue_name))

        completed = 0
        task_keys = await self.client.keys(self._task_prefix(queue_name))
        if task_keys:
            values = await self.client.get(task_keys)
            for v in values.values():
                if v:
                    task = json.loads(v)
                    if task.get("finished_at"):
                        completed += 1

        return {"pending": pending, "completed": completed, "scheduled": scheduled}

    async def get_all_tasks(self, queue_name: str) -> list[dict[str, Any]]:
        task_keys = await self.client.keys(self._task_prefix(queue_name))
        if not task_keys:
            return []

        values = await self.client.get(task_keys)
        return [json.loads(v) for v in values.values() if v]

    async def list_queues(self) -> dict[str, int]:
        all_keys = await self.client.keys("sheppy:")
        queue_names: set[str] = set()

        for key in all_keys:
            parts = key.split(":")
            if len(parts) >= 2:
                queue_names.add(parts[1])

        queues = {}
        for queue_name in sorted(queue_names):
            queues[queue_name] = await self.client.list_len(self._queue_key(queue_name))

        return queues

    async def get_scheduled(self, queue_name: str) -> list[dict[str, Any]]:
        items = await self.client.sorted_get(self._scheduled_key(queue_name))

        if not items:
            return []

        keys = [self._task_key(queue_name, task_id) for _, task_id in items]
        values = await self.client.get(keys)

        return [json.loads(v) for v in values.values() if v]

    async def add_cron(self, queue_name: str, deterministic_id: str, task_cron: dict[str, Any]) -> bool:
        return await self.client.create(self._cron_key(queue_name, deterministic_id), json.dumps(task_cron))

    async def delete_cron(self, queue_name: str, deterministic_id: str) -> bool:
        count = await self.client.delete([self._cron_key(queue_name, deterministic_id)])
        return count > 0

    async def get_crons(self, queue_name: str) -> list[dict[str, Any]]:
        cron_keys = await self.client.keys(self._cron_prefix(queue_name))
        if not cron_keys:
            return []

        values = await self.client.get(cron_keys)
        return [json.loads(v) for v in values.values() if v]
