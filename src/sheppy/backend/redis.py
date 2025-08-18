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
from .base import Backend, BackendError, ConnectionError


class RedisBackend(Backend):

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        consumer_group: str = "workers",
        decode_responses: bool = False,
        max_connections: int = 10,
        **kwargs: Any
    ):
        self.url = url
        self.consumer_group = consumer_group
        self.consumer_name = generate_unique_worker_id("consumer")
        self.decode_responses = decode_responses
        self.max_connections = max_connections
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
            raise ConnectionError(f"Failed to connect to Redis: {e}")

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

    def _stream_key(self, queue_name: str) -> str:
        return f"stream:{queue_name}"

    def _scheduled_key(self, queue_name: str) -> str:
        return f"scheduled:{queue_name}"

    def _results_key(self, queue_name: str) -> str:
        return f"results:{queue_name}"

    async def _ensure_consumer_group(self, stream_key: str) -> None:
        """Ensure consumer group exists. Only checks once per connection."""

        # Skip if we've already initialized this group
        if stream_key in self._initialized_groups:
            return

        # Check if consumer group exists
        try:
            groups = await self._client.xinfo_groups(stream_key)  # type: ignore[union-attr]
            for group in groups:
                if group['name'] == self.consumer_group:
                    self._initialized_groups.add(stream_key)
                    return  # Group already exists
        except redis.ResponseError:
            # Stream doesn't exist yet, will be created with mkstream=True
            pass

        # Create the consumer group
        try:
            await self._client.xgroup_create(  # type: ignore[union-attr]
                name=stream_key,
                groupname=self.consumer_group,
                id="0",  # Start from beginning to include existing messages
                mkstream=True  # Create stream if it doesn't exist
            )
            self._initialized_groups.add(stream_key)
        except redis.ResponseError as e:
            # BUSYGROUP error means the group already exists
            if "BUSYGROUP" in str(e):
                self._initialized_groups.add(stream_key)
                return
            raise BackendError(f"Failed to create consumer group: {e}")

    async def append(self, queue_name: str, task_data: dict[str, Any]) -> bool:
        if not self._client:
            raise BackendError("Not connected to Redis")

        try:
            # Store task data as fields in the stream entry
            fields = {
                "task_id": task_data["id"],
                "data": json.dumps(task_data)
            }

            # Add to stream
            stream_key = self._stream_key(queue_name)
            await self._client.xadd(stream_key, fields)  # type: ignore[arg-type]

            # Ensure consumer group exists
            await self._ensure_consumer_group(stream_key)

            # Track queue in metadata
            await self._client.sadd("queues", queue_name)  # type: ignore[misc]

            return True
        except Exception as e:
            raise BackendError(f"Failed to enqueue task: {e}")

    async def pop(self, queue_name: str, timeout: float | None = None) -> dict[str, Any] | None:
        if not self._client:
            raise BackendError("Not connected to Redis")

        stream_key = self._stream_key(queue_name)

        # Ensure consumer group exists
        await self._ensure_consumer_group(stream_key)

        try:
            # Read from stream using consumer group
            # ">" means only new messages (not delivered to other consumers)
            result = await self._client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={stream_key: ">"},
                count=1,
                block=None if timeout is None or timeout == 0 else int(timeout * 1000)
            )

            if not result:
                return None

            # Extract message
            stream_data = result[0]  # First (and only) stream
            messages = stream_data[1]  # Messages from the stream

            if not messages:
                return None

            message_id, fields = messages[0]

            # Deserialize task
            task_data = json.loads(fields[b"data"])

            # Store message ID for acknowledgment
            message_id_str = message_id.decode() if isinstance(message_id, bytes) else message_id
            self._pending_messages[task_data["id"]] = (queue_name, message_id_str)

            return task_data  # type: ignore[no-any-return]

        except Exception as e:
            raise BackendError(f"Failed to dequeue task: {e}")

    async def peek(self, queue_name: str, count: int = 1) -> list[dict[str, Any]]:
        if not self._client:
            raise BackendError("Not connected to Redis")

        stream_key = self._stream_key(queue_name)

        # Get messages from stream
        messages = await self._client.xrange(stream_key, count=count)

        tasks = []
        for _message_id, fields in messages:
            try:
                task_data = json.loads(fields[b"data"])
                tasks.append(task_data)
            except Exception:
                # Skip malformed tasks
                continue

        return tasks

    async def size(self, queue_name: str) -> int:
        if not self._client:
            raise BackendError("Not connected to Redis")

        stream_key = self._stream_key(queue_name)

        # Count undelivered messages in stream
        try:
            await self._ensure_consumer_group(stream_key)
            return await self._client.xlen(stream_key)  # type: ignore[no-any-return]
        except redis.ResponseError:
            # Stream doesn't exist yet
            return 0

    async def clear(self, queue_name: str) -> int:
        if not self._client:
            raise BackendError("Not connected to Redis")

        count = 0

        # Clear stream (XTRIM to 0 entries)
        stream_key = self._stream_key(queue_name)
        try:
            stream_info = await self._client.xinfo_stream(stream_key)
            count += stream_info.get("length", 0)
            await self._client.xtrim(stream_key, maxlen=0)
        except redis.ResponseError:
            # Stream doesn't exist yet
            pass

        # Clear scheduled tasks
        scheduled_key = self._scheduled_key(queue_name)
        count += await self._client.zcard(scheduled_key)
        await self._client.delete(scheduled_key)

        # Clear results
        results_key = self._results_key(queue_name)
        await self._client.delete(results_key)

        # Remove from queue list if empty
        await self._client.srem("queues", queue_name)  # type: ignore[misc]

        return count  # type: ignore[no-any-return]

    async def schedule(self, queue_name: str, task_data: dict[str, Any], at: datetime) -> bool:
        if not self._client:
            raise BackendError("Not connected to Redis")

        try:
            task_json = json.dumps(task_data)

            # Add to sorted set with timestamp as score
            scheduled_key = self._scheduled_key(queue_name)
            score = at.timestamp()
            await self._client.zadd(scheduled_key, {task_json: score})

            # Track queue
            await self._client.sadd("queues", queue_name)  # type: ignore[misc]

            return True
        except Exception as e:
            raise BackendError(f"Failed to schedule task: {e}")

    async def get_scheduled(self, queue_name: str, now: datetime | None = None) -> list[dict[str, Any]]:
        if not self._client:
            raise BackendError("Not connected to Redis")

        if now is None:
            now = datetime.now(timezone.utc)

        # Get tasks with score <= now
        scheduled_key = self._scheduled_key(queue_name)
        task_jsons = await self._client.zrangebyscore(
            scheduled_key,
            "-inf",
            now.timestamp()
        )

        tasks = []
        for task_json in task_jsons:
            try:
                task_data = json.loads(task_json)
                tasks.append(task_data)
                # Remove from scheduled set
                await self._client.zrem(scheduled_key, task_json)
            except Exception:
                continue

        return tasks

    async def acknowledge(self, queue_name: str, task_id: str) -> bool:
        if not self._client:
            raise BackendError("Not connected to Redis")

        # We only acknowledge messages we've dequeued and tracked
        if task_id not in self._pending_messages:
            return False

        stored_queue, message_id = self._pending_messages[task_id]
        stream_key = self._stream_key(stored_queue)

        try:
            acked = await self._client.xack(
                stream_key,
                self.consumer_group,
                message_id
            )

            if acked > 0:
                # Remove from pending messages tracking
                del self._pending_messages[task_id]
                return True

            return False
        except Exception:
            return False

    async def list_queues(self) -> list[str]:
        if not self._client:
            raise BackendError("Not connected to Redis")

        queue_names = await self._client.smembers("queues")  # type: ignore[misc]

        # Handle binary data
        result = []
        for name in queue_names:
            if isinstance(name, bytes):
                result.append(name.decode('utf-8'))
            else:
                result.append(str(name))

        return sorted(result)

    async def store_result(self, queue_name: str, task_data: dict[str, Any]) -> bool:
        if not self._client:
            raise BackendError("Not connected to Redis")

        try:
            task_json = json.dumps(task_data)

            # Store in results hash
            results_key = self._results_key(queue_name)
            await self._client.hset(results_key, task_data["id"], task_json)  # type: ignore[misc]

            # Set TTL (24 hours)
            await self._client.expire(results_key, 86400)

            # Acknowledge the message if we have it tracked
            task_id_str = task_data["id"]
            if task_id_str in self._pending_messages:
                stored_queue, message_id = self._pending_messages[task_id_str]
                stream_key = self._stream_key(stored_queue)
                await self._client.xack(
                    stream_key,
                    self.consumer_group,
                    message_id
                )
                del self._pending_messages[task_id_str]

            return True
        except Exception as e:
            raise BackendError(f"Failed to store task result: {e}")

    async def get_task(self, queue_name: str, task_id: str) -> dict[str, Any] | None:
        if not self._client:
            raise BackendError("Not connected to Redis")

        # Check in results first (most likely place for completed tasks)
        results_key = self._results_key(queue_name)
        task_json = await self._client.hget(results_key, task_id)  # type: ignore[misc]
        if task_json:
            try:
                task_data = json.loads(task_json)
                return task_data  # type: ignore[no-any-return]
            except Exception:
                pass

        # Check if it's a task we're currently tracking (dequeued but not completed)
        if task_id in self._pending_messages:
            stored_queue, message_id = self._pending_messages[task_id]
            stream_key = self._stream_key(stored_queue)

            # Read the specific message
            messages = await self._client.xrange(stream_key, min=message_id, max=message_id, count=1)
            if messages:
                _, fields = messages[0]
                try:
                    task_data = json.loads(fields[b"data"])
                    return task_data  # type: ignore[no-any-return]
                except Exception:
                    pass

        # Check scheduled tasks
        scheduled_key = self._scheduled_key(queue_name)
        task_jsons = await self._client.zrange(scheduled_key, 0, -1)
        for task_json in task_jsons:
            try:
                task_data = json.loads(task_json)
                if task_data["id"] == task_id:
                    return task_data  # type: ignore[no-any-return]
            except Exception:
                continue

        return None
