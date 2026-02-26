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

from .._utils.task_execution import generate_unique_worker_id
from .base import Backend, BackendError


class RedisBackend(Backend):

    def __init__(
        self,
        url: str = "redis://127.0.0.1:6379",
        consumer_group: str = "workers",
        ttl: int | None = 30 * 24 * 60 * 60,  # 30 days
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
            await self._client.ping()  # type: ignore[misc]
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
        """Task Metadata (key prefix)"""
        return f"sheppy:tasks:{queue_name}"

    def _scheduled_tasks_key(self, queue_name: str) -> str:
        """Scheduled tasks (sorted set)"""
        return f"sheppy:scheduled:{queue_name}"

    def _cron_tasks_key(self, queue_name: str) -> str:
        """Cron tasks (hset)"""
        return f"sheppy:cron:{queue_name}"

    def _workflows_key(self, queue_name: str) -> str:
        """Workflow metadata (hset)"""
        return f"sheppy:workflows:{queue_name}"

    def _workflow_pending_key(self, queue_name: str, workflow_id: str) -> str:
        """Pending task IDs for a workflow (set)"""
        return f"sheppy:workflows:{queue_name}:{workflow_id}:pending"

    def _workflow_pending_index_key(self, queue_name: str) -> str:
        """Index of pending workflow IDs (set)"""
        return f"sheppy:workflows:{queue_name}:_pending"

    def _queues_registry_key(self) -> str:
        """Registry of all queue names and their metadata (hset)"""
        return "sheppy:_queues"

    def _pending_tasks_key(self, queue_name: str) -> str:
        """Queued tasks to be processed (stream)"""
        return f"sheppy:pending:{queue_name}"

    def _finished_tasks_key(self, queue_name: str) -> str:
        """Notifications about finished tasks (stream)"""
        return f"sheppy:finished:{queue_name}"

    def _completed_counter_key(self, queue_name: str) -> str:
        """Cumulative count of completed tasks (string/integer)"""
        return f"sheppy:completed:{queue_name}"

    def _worker_metadata_key(self, queue_name: str) -> str:
        """Worker Metadata (key prefix)"""
        return f"sheppy:workers:{queue_name}"

    def _rate_limit_key(self, queue_name: str) -> str:
        return f"sheppy:ratelimit:{queue_name}"

    def _sliding_window_key(self, queue_name: str, key: str) -> str:
        return f"sheppy:ratelimit:{queue_name}:sw:{key}"

    @property
    def client(self) -> redis.Redis:
        if not self._client:
            raise BackendError("Not connected to Redis")
        return self._client

    async def _create_tasks(self, queue_name: str, tasks: list[dict[str, Any]]) -> list[bool]:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)

        try:
            async with self.client.pipeline() as pipe:
                for task in tasks:
                    pipe.set(f"{tasks_metadata_key}:{task['id']}", json.dumps(task), ex=self.ttl, nx=True)
                res = await pipe.execute()
        except Exception as e:
            raise BackendError(f"Failed to create tasks: {e}") from e

        return [bool(r) for r in res]

    async def append(self, queue_name: str, tasks: list[dict[str, Any]], unique: bool = True) -> list[bool]:
        """Add new tasks to be processed."""
        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        pending_tasks_key = self._pending_tasks_key(queue_name)

        await self._ensure_consumer_group(pending_tasks_key)

        if unique:
            success = await self._create_tasks(queue_name, tasks)
            to_queue = [t for i, t in enumerate(tasks) if success[i]]
        else:
            success = [True] * len(tasks)
            to_queue = tasks

        try:
            async with self.client.pipeline(transaction=False) as pipe:
                pipe.hsetnx(self._queues_registry_key(), queue_name, "{}")

                for t in to_queue:
                    _task_data = json.dumps(t)

                    if not unique:
                        pipe.set(f"{tasks_metadata_key}:{t['id']}", _task_data)

                    # add to pending stream
                    pipe.xadd(pending_tasks_key, {"data": _task_data})

                await pipe.execute()
        except Exception as e:
            raise BackendError(f"Failed to enqueue task: {e}") from e

        return success

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

    async def get_pending(self, queue_name: str, count: int = 1) -> list[dict[str, Any]]:
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

        count = 0
        async for key in self.client.scan_iter(match=f"{tasks_metadata_key}:*", count=10000):
            await self.client.delete(key)
            count += 1

        await self.client.xtrim(pending_tasks_key, maxlen=0)
        await self.client.delete(scheduled_key)
        await self.client.hdel(self._queues_registry_key(), queue_name)  # type: ignore[misc]
        await self.client.delete(self._rate_limit_key(queue_name))
        sw_keys = [key async for key in self.client.scan_iter(match=self._sliding_window_key(queue_name, '*'), count=10000)]
        if sw_keys:
            await self.client.delete(*sw_keys)

        return count

    async def get_tasks(self, queue_name: str, task_ids: list[str]) -> dict[str,dict[str, Any]]:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)

        if not task_ids:
            return {}

        task_json = await self.client.mget([f"{tasks_metadata_key}:{t}" for t in task_ids])
        tasks = [json.loads(d) for d in task_json if d]

        return {t['id']: t for t in tasks}

    async def schedule(self, queue_name: str, task_data: dict[str, Any], at: datetime, unique: bool = True) -> bool:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        scheduled_key = self._scheduled_tasks_key(queue_name)

        if unique:
            success = await self._create_tasks(queue_name, [task_data])
            if not success[0]:
                return False

        try:
            if not unique:
                await self.client.set(f"{tasks_metadata_key}:{task_data['id']}", json.dumps(task_data))

            score = at.timestamp()
            async with self.client.pipeline(transaction=False) as pipe:
                pipe.zadd(scheduled_key, {task_data['id']: score})
                pipe.hsetnx(self._queues_registry_key(), queue_name, "{}")
                await pipe.execute()

            return True
        except Exception as e:
            raise BackendError(f"Failed to schedule task: {e}") from e

    async def pop_scheduled(self, queue_name: str, now: datetime | None = None) -> list[dict[str, Any]]:
        scheduled_key = self._scheduled_tasks_key(queue_name)
        tasks_metadata_key = self._tasks_metadata_key(queue_name)

        score = now.timestamp() if now else time()

        task_id_entries = await self.client.zrangebyscore(scheduled_key, 0, score)

        claimed_ids = []
        for entry in task_id_entries:
            removed = await self.client.zrem(scheduled_key, entry)

            if removed <= 0:
                # some other worker already got this task at the same time, skip
                continue

            task_id = entry.decode() if isinstance(entry, bytes) else entry
            claimed_ids.append(task_id)

        if not claimed_ids:
            return []

        task_jsons = await self.client.mget([f"{tasks_metadata_key}:{tid}" for tid in claimed_ids])
        return [json.loads(tj) for tj in task_jsons if tj]

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
                pipe.set(f"{tasks_metadata_key}:{task_data['id']}", json.dumps(task_data), ex=self.ttl)
                # add to finished stream for get_result notifications
                if task_data["finished_at"] is not None:  # only send notification on finished task (for retriable tasks we continue to wait)
                    pipe.xadd(finished_tasks_key, {"task_id": task_data["id"]}, minid=min_id)
                    pipe.incr(self._completed_counter_key(queue_name))
                # ack and delete the task from the stream (cleanup)
                if message_id:
                    pipe.xack(pending_tasks_key, self.consumer_group, message_id)
                    pipe.xdel(pending_tasks_key, message_id)

                await pipe.execute()

            self._pending_messages.pop(task_data["id"], None)
            return True
        except Exception as e:
            raise BackendError(f"Failed to store task result: {e}") from e

    async def get_stats(self, queue_name: str) -> dict[str, int]:
        scheduled_tasks_key = self._scheduled_tasks_key(queue_name)
        pending_tasks_key = self._pending_tasks_key(queue_name)

        pending = await self.client.xlen(pending_tasks_key)
        completed = await self.client.get(self._completed_counter_key(queue_name))

        return {
            "pending": pending,
            "completed": int(completed) if completed else 0,
            "scheduled": await self.client.zcard(scheduled_tasks_key),
        }

    async def get_all_tasks(self, queue_name: str) -> list[dict[str, Any]]:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)

        keys = []
        async for key in self.client.scan_iter(match=f"{tasks_metadata_key}:*", count=10000):
            keys.append(key)

        if not keys:
            return []

        all_tasks_data = await self.client.mget(keys)
        return [json.loads(task_json) for task_json in all_tasks_data if task_json]

    async def get_results(self, queue_name: str, task_ids: list[str], timeout: float | None = None) -> dict[str,dict[str, Any]]:
        tasks_metadata_key = self._tasks_metadata_key(queue_name)
        finished_tasks_key = self._finished_tasks_key(queue_name)

        if not task_ids:
            return {}

        results = {}
        remaining_ids = task_ids[:]

        last_id = "0-0"
        if timeout is not None and timeout >= 0:
            with contextlib.suppress(redis.ResponseError):
                last_id = (await self.client.xinfo_stream(finished_tasks_key))["last-generated-id"]

        tasks = await self.client.mget([f"{tasks_metadata_key}:{t}" for t in task_ids])
        for task_json in tasks:
            if not task_json:
                continue
            t = json.loads(task_json)

            if t.get("finished_at"):
                 results[t["id"]] = t
                 remaining_ids.remove(t["id"])

        if not remaining_ids:
            return results

        if timeout is None or timeout < 0:
            return results

        # endless wait if timeout == 0
        deadline = None if timeout == 0 else asyncio.get_running_loop().time() + timeout

        while True:
            if deadline:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError(f"Did not complete within {timeout} seconds")
            else:
                remaining = 0

            messages = await self.client.xread(
                {finished_tasks_key: last_id},
                block=int(remaining * 1000),
                count=1000
            )

            if not messages:
                continue

            for _, stream_messages in messages:
                for msg_id, data in stream_messages:
                    last_id = msg_id
                    task_id = data.get(b"task_id").decode()

                    if task_id in remaining_ids:
                        task_json = await self.client.get(f"{tasks_metadata_key}:{task_id}")
                        if not task_json:
                            continue
                        t = json.loads(task_json)

                        if t.get("finished_at"):  # should be always true because we only get notifications for finished tasks
                            results[t["id"]] = t
                            remaining_ids.remove(t["id"])

                        if not remaining_ids:
                            return results

    async def acquire_rate_limit(self, queue_name: str, key: str, max_rate: int, rate_period: float, task_id: str, strategy: str = "sliding_window") -> float | None:
        if strategy == "fixed_window":
            return await self._acquire_fixed_window(queue_name, key, max_rate, rate_period)

        return await self._acquire_sliding_window(queue_name, key, max_rate, rate_period, task_id)

    async def _acquire_fixed_window(self, queue_name: str, key: str, max_rate: int, rate_period: float) -> float | None:
        rl_key = self._rate_limit_key(queue_name)
        ttl_ms = int(rate_period * 1000)

        async with self.client.pipeline(transaction=False) as pipe:
            pipe.hincrby(rl_key, key, 1)
            pipe.hpexpire(rl_key, ttl_ms, key, nx=True)
            pipe.hpttl(rl_key, key)
            results = await pipe.execute()

        count = results[0]

        if count <= max_rate:
            return None

        # over limit - undo increment, return remaining TTL
        await self.client.hincrby(rl_key, key, -1)  # type: ignore[misc]
        pttl_result = results[2]
        remaining_ms = pttl_result[0] if pttl_result and pttl_result[0] > 0 else ttl_ms
        return remaining_ms / 1000.0

    async def _acquire_sliding_window(self, queue_name: str, key: str, max_rate: int, rate_period: float, task_id: str) -> float | None:
        rl_key = self._sliding_window_key(queue_name, key)
        now = time()
        window_start = now - rate_period

        async with self.client.pipeline(transaction=False) as pipe:
            pipe.zremrangebyscore(rl_key, 0, window_start)
            pipe.zcard(rl_key)
            pipe.zadd(rl_key, {task_id: now})
            pipe.expire(rl_key, int(rate_period) + 1)
            results = await pipe.execute()

        current_count = results[1]

        if current_count < max_rate:
            return None

        # over limit - remove the entry we just added
        await self.client.zrem(rl_key, task_id)

        # calculate wait time from oldest entry in the window
        oldest = await self.client.zrange(rl_key, 0, 0, withscores=True)
        if oldest:
            wait = float(oldest[0][1]) + rate_period - now
            return max(wait, 0.01)

        return rate_period

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
        queue_names = await self.client.hkeys(self._queues_registry_key())  # type: ignore[misc]

        queues = {}
        for raw_name in sorted(queue_names):
            queue_name = raw_name.decode() if isinstance(raw_name, bytes) else raw_name
            try:
                pending_count = await self.client.xlen(self._pending_tasks_key(queue_name))
                queues[queue_name] = int(pending_count)
            except redis.ResponseError:
                queues[queue_name] = 0

        return queues

    async def get_scheduled(self, queue_name: str) -> list[dict[str, Any]]:
        scheduled_key = self._scheduled_tasks_key(queue_name)
        tasks_metadata_key = self._tasks_metadata_key(queue_name)

        task_ids = await self.client.zrange(scheduled_key, 0, -1)

        if not task_ids:
            return []

        keys = [
            f"{tasks_metadata_key}:{(tid.decode() if isinstance(tid, bytes) else tid)}"
            for tid in task_ids
        ]
        task_jsons = await self.client.mget(keys)
        return [json.loads(tj) for tj in task_jsons if tj]

    async def add_cron(self, queue_name: str, deterministic_id: str, task_cron: dict[str, Any]) -> bool:
        cron_key = self._cron_tasks_key(queue_name)
        return bool(await self.client.hsetnx(cron_key, deterministic_id, json.dumps(task_cron)))  # type: ignore[misc]

    async def delete_cron(self, queue_name: str, deterministic_id: str) -> bool:
        cron_key = self._cron_tasks_key(queue_name)
        return bool(await self.client.hdel(cron_key, deterministic_id))  # type: ignore[misc]

    async def get_crons(self, queue_name: str) -> list[dict[str, Any]]:
        cron_key = self._cron_tasks_key(queue_name)
        cron_data = await self.client.hvals(cron_key)  # type: ignore[misc]
        return [json.loads(d) for d in cron_data]

    async def store_workflow(self, queue_name: str, workflow_data: dict[str, Any]) -> bool:
        workflows_key = self._workflows_key(queue_name)
        workflow_id = workflow_data['id']
        pending_key = self._workflow_pending_key(queue_name, workflow_id)
        pending_index_key = self._workflow_pending_index_key(queue_name)
        pending_ids = workflow_data.get('pending_task_ids', [])

        try:
            async with self.client.pipeline(transaction=True) as pipe:
                pipe.hset(workflows_key, workflow_id, json.dumps(workflow_data))
                if self.ttl:
                    pipe.hexpire(workflows_key, self.ttl, workflow_id)

                if workflow_data.get('completed') or workflow_data.get('error'):
                    pipe.delete(pending_key)
                    pipe.srem(pending_index_key, workflow_id)
                elif pending_ids:
                    pipe.sadd(pending_key, *pending_ids)
                    pipe.sadd(pending_index_key, workflow_id)
                    if self.ttl:
                        pipe.expire(pending_key, self.ttl)

                await pipe.execute()
            return True
        except Exception as e:
            raise BackendError(f"Failed to store workflow: {e}") from e

    async def get_workflows(self, queue_name: str, workflow_ids: list[str]) -> dict[str, dict[str, Any]]:
        workflows_key = self._workflows_key(queue_name)

        if not workflow_ids:
            return {}

        try:
            data = await self.client.hmget(workflows_key, workflow_ids)  # type: ignore[misc]
            result = {}
            for wf_json in data:
                if wf_json:
                    wf = json.loads(wf_json)
                    result[wf["id"]] = wf
            return result
        except Exception as e:
            raise BackendError(f"Failed to get workflows: {e}") from e

    async def get_all_workflows(self, queue_name: str) -> list[dict[str, Any]]:
        workflows_key = self._workflows_key(queue_name)

        try:
            all_data = await self.client.hvals(workflows_key)  # type: ignore[misc]
            return [json.loads(wf_json) for wf_json in all_data if wf_json]
        except Exception as e:
            raise BackendError(f"Failed to get all workflows: {e}") from e

    async def get_pending_workflows(self, queue_name: str) -> list[dict[str, Any]]:
        workflows_key = self._workflows_key(queue_name)
        pending_index_key = self._workflow_pending_index_key(queue_name)

        try:
            workflow_ids = await self.client.smembers(pending_index_key)  # type: ignore[misc]
            if not workflow_ids:
                return []

            ids = [wid.decode() if isinstance(wid, bytes) else wid for wid in workflow_ids]
            data = await self.client.hmget(workflows_key, ids)  # type: ignore[misc]
            return [json.loads(wf_json) for wf_json in data if wf_json]
        except Exception as e:
            raise BackendError(f"Failed to get pending workflows: {e}") from e

    async def delete_workflow(self, queue_name: str, workflow_id: str) -> bool:
        workflows_key = self._workflows_key(queue_name)
        pending_key = self._workflow_pending_key(queue_name, workflow_id)
        pending_index_key = self._workflow_pending_index_key(queue_name)
        try:
            async with self.client.pipeline(transaction=True) as pipe:
                pipe.hdel(workflows_key, workflow_id)
                pipe.delete(pending_key)
                pipe.srem(pending_index_key, workflow_id)
                results = await pipe.execute()
            return int(results[0]) > 0
        except Exception as e:
            raise BackendError(f"Failed to delete workflow: {e}") from e

    async def mark_workflow_task_complete(self, queue_name: str, workflow_id: str, task_id: str) -> int:
        pending_key = self._workflow_pending_key(queue_name, workflow_id)

        try:
            async with self.client.pipeline() as pipe:
                pipe.srem(pending_key, task_id)
                pipe.scard(pending_key)
                results = await pipe.execute()

            removed_count = results[0]  # 1 if removed, 0 if not found
            remaining_count = results[1]

            if removed_count == 0:
                return -1  # task not in pending set

            return int(remaining_count)
        except Exception as e:
            raise BackendError(f"Failed to mark workflow task complete: {e}") from e
