import asyncio
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
        url: str = "redis://localhost:6379",
        consumer_group: str = "workers",
        decode_responses: bool = False,
        max_connections: int = 10,
        ttl: int | None = 24 * 60 * 60,  # 24 hours
        **kwargs: Any
    ):
        self.url = url
        self.consumer_group = consumer_group
        self.consumer_name = generate_unique_worker_id("consumer")
        self.decode_responses = decode_responses
        self.max_connections = max_connections
        self.ttl = ttl
        self.redis_kwargs = kwargs

        self._client: redis.Redis | None = None
        self._pool: redis.ConnectionPool | None = None
        self._pending_messages: dict[str, tuple[str, str]] = {}  # task_id -> (queue_name, message_id)
        self._initialized_groups: set[str] = set()
        self._results_stream_ttl = 60

    async def connect(self) -> None:
        try:
            # Create connection pool
            self._pool = redis.ConnectionPool.from_url(
                self.url,
                decode_responses=self.decode_responses,
                max_connections=self.max_connections,
                #protocol=3,  # enable RESP version 3  # ! FIXME
                **self.redis_kwargs
            )

            # Let Redis client own the pool for proper cleanup
            self._client = redis.Redis.from_pool(self._pool)

            # Test connection
            await self._client.ping()
        except Exception as e:
            # Clean up on failure
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

    async def append(self, queue_name: str, task_data: dict[str, Any]) -> bool:  # ! fixme: maybe add (task_id: str) arg?
        """Add new tasks to be processed."""
        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        try:
            async with self.client.pipeline(transaction=True) as pipe:
                # create task metadata
                pipe.hset(tasks_metadata_key, task_data["id"], json.dumps(task_data))
                # add to pending stream
                pipe.xadd(pending_tasks_key, {"data": json.dumps(task_data)})

                await (pipe.execute())

            return True
        except Exception as e:
            raise BackendError(f"Failed to enqueue task: {e}") from e

    async def pop(self, queue_name: str, timeout: float | None = None) -> dict[str, Any] | None:
        """Get next tasks to process. Used primarily by workers."""
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        try:
            result = await self.client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={pending_tasks_key: ">"},  # ">" means only new messages (not delivered to other consumers)
                count=1,
                block=None if timeout is None or timeout == 0 else int(timeout * 1000)
            )

            if not result:
                return None

            messages = result[0][1]  # [['stream-name', [(message_id, dict_data)]]]

            if not messages:
                return None

            message_id, fields = messages[0]
            task_data = json.loads(fields[b"data"])

            # store message_id for acknowledge()
            self._pending_messages[task_data["id"]] = (queue_name, message_id.decode())

            return task_data  # type: ignore[no-any-return]

        except Exception as e:
            raise BackendError(f"Failed to dequeue task: {e}") from e

    async def acknowledge(self, queue_name: str, task_id: str) -> bool:
        if task_id not in self._pending_messages:
            # cannot acknowledge unknown task_id
            return False

        stored_queue, message_id = self._pending_messages[task_id]

        pending_tasks_key = self._pending_tasks_key(stored_queue)

        await self._ensure_consumer_group(pending_tasks_key)

        try:
            acked = await self.client.xack(pending_tasks_key, self.consumer_group, message_id)

            if acked > 0:
                del self._pending_messages[task_id]
                return True

            return False
        except Exception:
            return False

    async def peek(self, queue_name: str, count: int = 1) -> list[dict[str, Any]]:
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        # Get messages from stream
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

        count = 0

        # Clear stream (XTRIM to 0 entries)
        stream_info = await self.client.xinfo_stream(pending_tasks_key)
        count += stream_info.get("length", 0)  #  ! fixme?
        await self.client.xtrim(pending_tasks_key, maxlen=0)

        count += await self.client.zcard(scheduled_key)

        await self.client.delete(scheduled_key)
        await self.client.delete(tasks_metadata_key)

        return int(count)

    async def get_task(self, queue_name: str, task_id: str) -> dict[str, Any] | None:
        task_metadata_key = self._tasks_metadata_key(queue_name)

        task_json = await self.client.hget(task_metadata_key, task_id)  # type: ignore[misc]

        return json.loads(task_json) if task_json else None

    async def schedule(self, queue_name: str, task_data: dict[str, Any], at: datetime) -> bool:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        scheduled_key = self._scheduled_tasks_key(queue_name)

        try:
            # create task metadata
            await self.client.hset(tasks_metadata_key, task_data["id"], json.dumps(task_data))  # type: ignore[misc]

            # add to sorted set with timestamp as score
            score = at.timestamp()
            await self.client.zadd(scheduled_key, {json.dumps(task_data): score})  # ! FIXME? only store task_id?

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

        await self._ensure_consumer_group(finished_tasks_key)

        try:
            async with self.client.pipeline(transaction=True) as pipe:
                # update task metadata with the results
                pipe.hsetex(tasks_metadata_key, task_data["id"], json.dumps(task_data), ex=self.ttl)
                # add to finished stream for get_result notifications
                pipe.xadd(finished_tasks_key, {"task_id": task_data["id"]})
                # Trim messages older than stream_ttl_seconds
                min_id = f"{int((time() - self._results_stream_ttl) * 1000)}-0"
                pipe.xtrim(finished_tasks_key, minid=min_id, approximate=True)

                await (pipe.execute())

            return True
        except Exception as e:
            raise BackendError(f"Failed to store task result: {e}") from e

    async def get_result(self, queue_name: str, task_id: str, timeout: float | None = None) -> dict[str, Any] | None:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        finished_tasks_key = self._finished_tasks_key(queue_name)

        last_id = "0-0"
        if timeout is not None and timeout > 0:
            try:
                last_id = (await self.client.xinfo_stream(finished_tasks_key))["last-generated-id"]
            except redis.ResponseError:
                pass

        task_data_json = await self.client.hget(tasks_metadata_key, task_id)  # type: ignore[misc]
        if task_data_json:
            task_data = json.loads(task_data_json)
            if task_data.get("metadata", {}).get("finished_datetime"):
                return task_data  # type: ignore[no-any-return]

        if timeout is None or timeout <= 0:
            return None

        deadline = asyncio.get_event_loop().time() + timeout

        while asyncio.get_event_loop().time() < deadline:
            remaining = deadline - asyncio.get_event_loop().time()

            messages = await self.client.xread(
                {finished_tasks_key: last_id},
                block=max(1, int(remaining * 1000)),
                count=100
            )

            if not messages:
                break  # timeout

            for _, stream_messages in messages:
                for msg_id, data in stream_messages:
                    last_id = msg_id

                    if data.get(b"task_id").decode() == task_id:
                        task_data_json = await self.client.hget(tasks_metadata_key, task_id)  # type: ignore[misc]
                        return json.loads(task_data_json) if task_data_json else None

        raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")

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
