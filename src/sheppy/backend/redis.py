import asyncio
import contextlib
import json
from datetime import datetime
from time import time
from typing import Any

try:
    import redis.asyncio as redis
except ImportError as e:
    raise ImportError(
        "Redis backend requires redis package. "
        "Install it with: pip install redis"
    ) from e

from ..utils.task_execution import generate_unique_worker_id
from .base import Backend, BackendError


class RedisBackend(Backend):

    def __init__(
        self,
        url: str = "redis://127.0.0.1:6379",
        consumer_group: str = "workers",
        ttl: int | None = 24 * 60 * 60,  # 24 hours
        **kwargs: Any
    ):
        self.url = url
        self.consumer_group = consumer_group
        self.consumer_name = generate_unique_worker_id("consumer")
        self.ttl = ttl
        self.redis_kwargs = kwargs

        self._client: redis.Redis | None = None
        self._pool: redis.ConnectionPool | None = None
        self._pending_messages: dict[str, tuple[str, str]] = {}  # task_id -> (queue_name, message_id)
        self._initialized_groups: set[str] = set()
        self._results_stream_ttl = 60

    async def connect(self) -> None:
        try:
            self._pool = redis.ConnectionPool.from_url(
                self.url,
                #decode_responses=self.decode_responses,
                #max_connections=self.max_connections,
                #protocol=3,  # enable RESP version 3
                **self.redis_kwargs
            )
            self._client = redis.Redis.from_pool(self._pool)
            await self._client.ping()
        except Exception as e:
            self._client = None
            self._pool = None
            raise BackendError(f"Failed to connect to Redis: {e}") from e

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
            self._pool = None
            self._pending_messages.clear()
            self._initialized_groups.clear()

    @property
    def is_connected(self) -> bool:
        return self._client is not None and self._pool is not None

    def _tasks_metadata_key(self, queue_name: str) -> str:
        """Task Metadata (hset)"""
        return f"sheppy:tasks:{queue_name}"

    def _scheduled_tasks_key(self, queue_name: str) -> str:
        """Scheduled tasks (sorted set)"""
        return f"sheppy:scheduled:{queue_name}"

    def _cron_tasks_key(self, queue_name: str) -> str:
        """Cron tasks (hset)"""
        return f"sheppy:cron:{queue_name}"

    def _pending_tasks_key(self, queue_name: str) -> str:
        """Queued tasks to be processed (stream)"""
        return f"sheppy:pending:{queue_name}"

    def _finished_tasks_key(self, queue_name: str) -> str:
        """Notifications about finished tasks (stream)"""
        return f"sheppy:finished:{queue_name}"

    def _worker_metadata_key(self, queue_name: str) -> str:
        """Worker Metadata (hset)"""
        return f"sheppy:workers:{queue_name}"

    @property
    def client(self) -> redis.Redis:
        if not self._client:
            raise BackendError("Not connected to Redis")
        return self._client

    async def create_tasks(self, queue_name: str, tasks: list[dict[str, Any]]) -> list[bool]:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)

        try:
            async with self.client.pipeline() as pipe:
                for task in tasks:
                    pipe.hsetnx(tasks_metadata_key, task["id"], json.dumps(task))
                    # pipe.expire()  #! FIXME - todo
                res = await pipe.execute()
        except Exception as e:
            raise BackendError(f"Failed to create tasks: {e}") from e

        return [bool(r) for r in res]

    async def append(self, queue_name: str, tasks: list[dict[str, Any]]) -> bool:
        """Add new tasks to be processed."""
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        try:
            async with self.client.pipeline(transaction=False) as pipe:
                for _task_data in tasks:
                    # add to pending stream
                    pipe.xadd(pending_tasks_key, {"data": json.dumps(_task_data)})

                await pipe.execute()
        except Exception as e:
            raise BackendError(f"Failed to enqueue task: {e}") from e

        return True

    async def pop(self, queue_name: str, limit: int = 1, timeout: float | None = None) -> list[dict[str, Any]]:
        """Get next tasks to process. Used primarily by workers."""
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        try:
            result = await self.client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={pending_tasks_key: ">"},  # ">" means only new messages (not delivered to other consumers)
                count=limit,
                block=None if timeout is None or timeout == 0 else int(timeout * 1000)
            )

            if not result:
                return []

            messages = result[0][1]  # [['stream-name', [(message_id, dict_data)]]]

            if not messages:
                return []

            tasks = []
            for message_id, fields in messages:
                task_data = json.loads(fields[b"data"])

                # store message_id for acknowledge()
                self._pending_messages[task_data["id"]] = (queue_name, message_id.decode())
                tasks.append(task_data)

            return tasks

        except Exception as e:
            raise BackendError(f"Failed to dequeue task: {e}") from e

    async def list_pending(self, queue_name: str, count: int = 1) -> list[dict[str, Any]]:
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        messages = await self.client.xrange(pending_tasks_key, count=count)

        return [json.loads(fields[b"data"]) for _message_id, fields in messages]

    async def size(self, queue_name: str) -> int:
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        return int(await self.client.xlen(pending_tasks_key))

    async def clear(self, queue_name: str) -> int:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        pending_tasks_key = self._pending_tasks_key(queue_name)
        scheduled_key = self._scheduled_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        count = await self.client.hlen(tasks_metadata_key)  # type: ignore[misc]

        await self.client.xtrim(pending_tasks_key, maxlen=0)
        await self.client.delete(scheduled_key)
        await self.client.delete(tasks_metadata_key)

        return int(count)

    async def get_task(self, queue_name: str, task_id: str) -> dict[str, Any] | None:
        task_metadata_key = self._tasks_metadata_key(queue_name)

        task_json = await self.client.hget(task_metadata_key, task_id)  # type: ignore[misc]

        return json.loads(task_json) if task_json else None

    async def schedule(self, queue_name: str, task_data: dict[str, Any], at: datetime) -> bool:
        scheduled_key = self._scheduled_tasks_key(queue_name)

        try:
            # add to sorted set with timestamp as score
            score = at.timestamp()
            await self.client.zadd(scheduled_key, {json.dumps(task_data): score})

            return True
        except Exception as e:
            raise BackendError(f"Failed to schedule task: {e}") from e

    async def get_scheduled(self, queue_name: str, now: datetime | None = None) -> list[dict[str, Any]]:
        scheduled_key = self._scheduled_tasks_key(queue_name)

        score = now.timestamp() if now else time()

        task_jsons = await self.client.zrangebyscore(scheduled_key, 0, score)

        tasks = []
        for task_json in task_jsons:
            removed = await self.client.zrem(scheduled_key, task_json)

            if removed <= 0:
                # some other worker already got this task at the same time, skip
                continue

            tasks.append(json.loads(task_json))

        return tasks

    async def store_result(self, queue_name: str, task_data: dict[str, Any]) -> bool:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        finished_tasks_key = self._finished_tasks_key(queue_name)
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(finished_tasks_key)

        message_id = None
        if task_data["id"] in self._pending_messages:
            stored_queue, message_id = self._pending_messages[task_data["id"]]

            if queue_name != stored_queue:  # this should never happen
                raise BackendError("queue name mismatch")

        try:
            # trim older messages to keep the stream small
            min_id = f"{int((time() - self._results_stream_ttl) * 1000)}-0"

            async with self.client.pipeline(transaction=True) as pipe:
                # update task metadata with the results
                pipe.hsetex(tasks_metadata_key, task_data["id"], json.dumps(task_data), ex=self.ttl)
                # add to finished stream for get_result notifications
                pipe.xadd(finished_tasks_key, {"task_id": task_data["id"]}, minid=min_id)
                # ack and delete the task from the stream (cleanup)
                if message_id:
                    pipe.xackdel(pending_tasks_key, self.consumer_group, message_id)

                await (pipe.execute())

            return True
        except Exception as e:
            raise BackendError(f"Failed to store task result: {e}") from e

    async def stats(self, queue_name: str) -> dict[str, int]:
        scheduled_tasks_key = self._scheduled_tasks_key(queue_name)
        pending_tasks_key = self._pending_tasks_key(queue_name)
        finished_tasks_key = self._finished_tasks_key(queue_name)

        pending = await self.client.xlen(pending_tasks_key)
        completed = await self.client.xlen(finished_tasks_key)

        return {
            "pending": pending,
            "completed": completed,
            "scheduled": await self.client.zcard(scheduled_tasks_key),
        }

    async def get_all_tasks(self, queue_name: str) -> list[dict[str, Any]]:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)

        all_tasks_data = await self.client.hgetall(tasks_metadata_key)  # type: ignore[misc]

        return [json.loads(task_json) for task_json in all_tasks_data.values()]

    async def get_result(self, queue_name: str, task_id: str, timeout: float | None = None) -> dict[str, Any] | None:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        finished_tasks_key = self._finished_tasks_key(queue_name)

        last_id = "0-0"
        if timeout is not None and timeout >= 0:
            with contextlib.suppress(redis.ResponseError):
                last_id = (await self.client.xinfo_stream(finished_tasks_key))["last-generated-id"]

        task_data_json = await self.client.hget(tasks_metadata_key, task_id)  # type: ignore[misc]
        if task_data_json:
            task_data = json.loads(task_data_json)
            if task_data.get("finished_at"):
                return task_data  # type: ignore[no-any-return]

        if timeout is None or timeout < 0:
            return None

        # endless wait if timeout == 0
        deadline = None if timeout == 0 else asyncio.get_event_loop().time() + timeout

        while True:
            if deadline:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")
            else:
                remaining = 0

            messages = await self.client.xread(
                {finished_tasks_key: last_id},
                block=int(remaining * 1000),
                count=100
            )

            if not messages:
                continue

            for _, stream_messages in messages:
                for msg_id, data in stream_messages:
                    last_id = msg_id

                    if data.get(b"task_id").decode() == task_id:
                        task_data_json = await self.client.hget(tasks_metadata_key, task_id)  # type: ignore[misc]
                        return json.loads(task_data_json) if task_data_json else None


    async def _ensure_consumer_group(self, stream_key: str) -> None:
        if stream_key in self._initialized_groups:
            return

        try:
            self._initialized_groups.add(stream_key)
            # id="0" = start from beginning to include existing messages
            await self.client.xgroup_create(stream_key, self.consumer_group, id="0", mkstream=True)
        except redis.ResponseError:
            # group already exists, ignore
            pass

    async def list_queues(self) -> dict[str, int]:

        queue_names = set()

        for key in await self.client.keys("sheppy:*:*"):
            queue_names.add(key.decode().split(":")[2])

        queues = {}
        for queue_name in sorted(queue_names):
            try:
                pending_count = await self.client.xlen(self._pending_tasks_key(queue_name))
                queues[queue_name] = int(pending_count)
            except redis.ResponseError:
                queues[queue_name] = 0

        return queues

    async def list_scheduled(self, queue_name: str) -> list[dict[str, Any]]:
        scheduled_key = self._scheduled_tasks_key(queue_name)

        task_jsons = await self.client.zrange(scheduled_key, 0, -1, withscores=True)

        tasks = []
        for task_json, _score in task_jsons:
            tasks.append(json.loads(task_json))

        return tasks

    async def add_cron(self, queue_name: str, deterministic_id: str, task_cron: dict[str, Any]) -> bool:
        cron_tasks_key = self._cron_tasks_key(queue_name)
        return bool(await self.client.hsetnx(cron_tasks_key, deterministic_id, json.dumps(task_cron)))  # type: ignore[misc]

    async def delete_cron(self, queue_name: str, deterministic_id: str) -> bool:
        cron_tasks_key = self._cron_tasks_key(queue_name)
        return bool(await self.client.hdel(cron_tasks_key, deterministic_id))  # type: ignore[misc]

    async def list_crons(self, queue_name: str) -> list[dict[str, Any]]:
        cron_tasks_key = self._cron_tasks_key(queue_name)
        return [json.loads(d) for _, d in (await self.client.hgetall(cron_tasks_key)).items()]  # type: ignore[misc]
