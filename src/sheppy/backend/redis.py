import json
from datetime import datetime, timezone
from typing import Any

try:
    import redis.asyncio as redis
except ImportError:
    raise ImportError(
        "Redis backend requires redis package. "
        "Install it with: pip install redis"
    )

from ..utils.task_execution import generate_unique_worker_id
from .base import Backend, BackendError


class RedisBackend(Backend):

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        consumer_group: str = "workers",
        decode_responses: bool = False,
        max_connections: int = 10,
        results_ttl: int | None = 24 * 60 * 60,  # 24 hours
        **kwargs: Any
    ):
        self.url = url
        self.consumer_group = consumer_group
        self.consumer_name = generate_unique_worker_id("consumer")
        self.decode_responses = decode_responses
        self.max_connections = max_connections
        self.results_ttl = results_ttl
        self.redis_kwargs = kwargs

        self._client: redis.Redis | None = None
        self._pool: redis.ConnectionPool | None = None
        self._pending_messages: dict[str, tuple[str, str]] = {}  # task_id -> (queue_name, message_id)
        self._initialized_groups: set[str] = set()

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
            raise BackendError(f"Failed to connect to Redis: {e}")

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

    def _results_key(self, queue_name: str) -> str:
        """Task Results (hset)"""
        return f"sheppy:results:{queue_name}"

    def _pending_tasks_key(self, queue_name: str) -> str:
        """Queued tasks to be processed (stream)"""
        return f"sheppy:pending:{queue_name}"

    def _finished_tasks_key(self, queue_name: str) -> str:
        """Notifications about finished tasks (stream)"""
        return f"sheppy:finished:{queue_name}"

    def _worker_metadata_key(self, queue_name: str) -> str:
        """Worker Metadata (hset)"""
        return f"sheppy:workers:{queue_name}"

    def _ensure_connected(self):
        if not self._client:
            raise BackendError("Not connected to Redis")

    async def append(self, queue_name: str, task_data: dict[str, Any]) -> bool:  # ! fixme: maybe add (task_id: str) arg?
        """Add new tasks to be processed."""
        self._ensure_connected()

        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        try:
            async with self._client.pipeline(transaction=True) as pipe:
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
        self._ensure_connected()

        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        try:
            result = await self._client.xreadgroup(
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

            return task_data

        except Exception as e:
            raise BackendError(f"Failed to dequeue task: {e}") from e

    async def acknowledge(self, queue_name: str, task_id: str) -> bool:
        self._ensure_connected()

        if task_id not in self._pending_messages:
            # cannot acknowledge unknown task_id
            return False

        stored_queue, message_id = self._pending_messages[task_id]

        pending_tasks_key = self._pending_tasks_key(stored_queue)

        await self._ensure_consumer_group(pending_tasks_key)

        try:
            acked = await self._client.xack(pending_tasks_key, self.consumer_group, message_id)

            if acked > 0:
                del self._pending_messages[task_id]
                return True

            return False
        except Exception:
            return False

    async def peek(self, queue_name: str, count: int = 1) -> list[dict[str, Any]]:
        self._ensure_connected()

        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        # Get messages from stream
        messages = await self._client.xrange(pending_tasks_key, count=count)

        return [json.loads(fields[b"data"]) for _message_id, fields in messages]

    async def size(self, queue_name: str) -> int:
        self._ensure_connected()

        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        return await self._client.xlen(pending_tasks_key)

    async def clear(self, queue_name: str) -> int:
        self._ensure_connected()

        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        pending_tasks_key = self._pending_tasks_key(queue_name)
        scheduled_key = self._scheduled_tasks_key(queue_name)
        results_key = self._results_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        count = 0

        # Clear stream (XTRIM to 0 entries)
        stream_info = await self._client.xinfo_stream(pending_tasks_key)
        count += stream_info.get("length", 0)  #  ! fixme?
        await self._client.xtrim(pending_tasks_key, maxlen=0)

        count += await self._client.zcard(scheduled_key)

        await self._client.delete(scheduled_key)
        await self._client.delete(results_key)
        await self._client.delete(tasks_metadata_key)

        return count

    async def get_task(self, queue_name: str, task_id: str) -> dict[str, Any] | None:
        self._ensure_connected()

        task_metadata_key = self._tasks_metadata_key(queue_name)
        results_key = self._results_key(queue_name)

        # check in results first (most likely place for completed tasks)  # ! FIXME
        task_json = await self._client.hget(results_key, task_id)

        # look into metadata if not found in results
        if not task_json:
            task_json = await self._client.hget(task_metadata_key, task_id)

        return json.loads(task_json) if task_json else None

    async def schedule(self, queue_name: str, task_data: dict[str, Any], at: datetime) -> bool:
        self._ensure_connected()

        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        scheduled_key = self._scheduled_tasks_key(queue_name)

        try:
            # create task metadata
            await self._client.hset(tasks_metadata_key, task_data["id"], json.dumps(task_data))

            # add to sorted set with timestamp as score
            score = at.timestamp()
            await self._client.zadd(scheduled_key, {json.dumps(task_data): score})  # ! FIXME? only store task_id?

            return True
        except Exception as e:
            raise BackendError(f"Failed to schedule task: {e}") from e

    async def get_scheduled(self, queue_name: str, now: datetime | None = None) -> list[dict[str, Any]]:
        self._ensure_connected()

        scheduled_key = self._scheduled_tasks_key(queue_name)

        if not now:
            now = datetime.now(timezone.utc)

        task_jsons = await self._client.zrange(scheduled_key, "-inf", now.timestamp(), byscore=True)

        tasks = []
        for task_json in task_jsons:
            removed = await self._client.zrem(scheduled_key, task_json)

            if removed <= 0:
                # some other worker already got this task at the same time, skip
                continue

            tasks.append(json.loads(task_json))

        return tasks

    async def store_result(self, queue_name: str, task_data: dict[str, Any]) -> bool:
        self._ensure_connected()

        results_key = self._results_key(queue_name)

        try:
            # ! fixme - metadata task update?
            await self._client.hsetex(results_key, task_data["id"], json.dumps(task_data), ex=self.results_ttl)  # ! FIXME - should we only store result?
            await self.acknowledge(task_data["id"])
            # await self._client.xadd(finished_tasks_key, {"data": json.dumps(task_data)})  # ! fixme
        except Exception as e:
            raise BackendError(f"Failed to store task result: {e}")

    async def get_result(self, queue_name: str, task_data: dict[str, Any], timeout: float | None = None) -> bool:
        raise NotImplementedError()

    async def _ensure_consumer_group(self, stream_key: str) -> None:
        if stream_key in self._initialized_groups:
            return

        try:
            self._initialized_groups.add(stream_key)
            # id="0" = start from beginning to include existing messages
            await self._client.xgroup_create(stream_key, self.consumer_group, id="0", mkstream=True)
        except redis.ResponseError:
            # group already exists, ignore
            pass
