import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic.type_adapter import TypeAdapter

from sheppy import Backend, BackendError, MemoryBackend
from sheppy.task_factory import TaskFactory
from sheppy.utils.task_execution import TaskInternal
from tests.dependencies import add_retriable, simple_async_task

Q = "pytest"
TASK_ID = UUID("47d88514-a5bb-488f-bd6e-9a442c732da5")
TASK_CRON_ID = UUID("2ee47ad3-0de2-4d54-854e-20259c45e7ca")


@pytest.fixture
def task_dict():
    return simple_async_task(1, 2).model_dump(mode="json")


@pytest.fixture
def tasks(datetime_now: datetime) -> dict[str, dict]:
    def _set_completed(task: TaskInternal) -> None:
        task.completed = True
        task.error = None
        task.result = 3
        task.finished_at = datetime_now

    def _set_failed(task: TaskInternal) -> None:
        task.completed = False
        task.error = "some error happened"
        task.result = None
        task.finished_at = datetime_now

    def _set_failed_should_retry(task: TaskInternal) -> None:
        task.completed = False
        task.error = "some error happened"
        task.result = None
        task.retry_count += 1
        task.last_retry_at = datetime_now - timedelta(seconds=6)
        task.next_retry_at = datetime_now - timedelta(seconds=5)
        task.scheduled_at = datetime_now - timedelta(seconds=5)
        task.finished_at = None

    # standard tasks
    task_new = TaskInternal.from_task(simple_async_task(1, 2))
    # task_new.id = TASK_ID
    task_new.created_at = datetime_now - timedelta(seconds=10)

    task_completed = task_new.model_copy(deep=True)
    _set_completed(task_completed)

    task_failed = task_new.model_copy(deep=True)
    _set_failed(task_failed)

    # scheduled tasks
    task_scheduled = task_new.model_copy(deep=True)
    task_scheduled.scheduled_at = datetime_now - timedelta(seconds=5)

    task_scheduled_completed = task_scheduled.model_copy(deep=True)
    _set_completed(task_scheduled_completed)

    task_scheduled_failed = task_scheduled.model_copy(deep=True)
    _set_failed(task_scheduled_failed)

    # cron tasks
    _task_cron = TaskFactory.create_cron_from_task(task_new.create_task(), "* * * * *")
    _next_run = _task_cron.next_run(datetime_now - timedelta(minutes=1))

    task_cron_new = TaskInternal.from_task(_task_cron.create_task(_next_run))
    # task_cron_new.id = TASK_ID
    task_cron_new.scheduled_at = _next_run

    task_cron_completed = task_cron_new.model_copy(deep=True)
    _set_completed(task_cron_completed)

    task_cron_failed = task_cron_new.model_copy(deep=True)
    _set_failed(task_cron_failed)

    # retriable tasks
    task_retriable_new = TaskInternal.from_task(add_retriable(1, 2))
    # task_retriable_new.id = TASK_ID
    task_retriable_new.created_at = datetime_now - timedelta(seconds=10)

    task_retriable_completed = task_retriable_new.model_copy(deep=True)
    _set_completed(task_retriable_completed)

    task_retriable_failed_should_retry = task_retriable_new.model_copy(deep=True)
    _set_failed_should_retry(task_retriable_failed_should_retry)

    task_retriable_retried_completed = task_retriable_failed_should_retry.model_copy(deep=True)
    _set_completed(task_retriable_retried_completed)

    task_retriable_retried_failed = task_retriable_failed_should_retry.model_copy(deep=True)
    _set_failed(task_retriable_retried_failed)

    # scheduled retriable tasks
    task_retriable_scheduled = task_retriable_new.model_copy(deep=True)
    task_retriable_scheduled.scheduled_at = datetime_now - timedelta(seconds=5)

    task_retriable_scheduled_completed = task_retriable_scheduled.model_copy(deep=True)
    _set_completed(task_retriable_scheduled_completed)

    task_retriable_scheduled_failed_should_retry = task_retriable_scheduled.model_copy(deep=True)
    _set_failed_should_retry(task_retriable_scheduled_failed_should_retry)

    task_retriable_scheduled_retried_completed = task_retriable_scheduled_failed_should_retry.model_copy(deep=True)
    _set_completed(task_retriable_scheduled_retried_completed)

    task_retriable_scheduled_retried_failed = task_retriable_scheduled_failed_should_retry.model_copy(deep=True)
    _set_failed(task_retriable_scheduled_retried_failed)

    # cron tasks
    _task_retriable_cron = TaskFactory.create_cron_from_task(task_retriable_new.create_task(), "* * * * *")
    _next_run = _task_retriable_cron.next_run(datetime_now - timedelta(minutes=1))

    task_retriable_cron_new = TaskInternal.from_task(_task_retriable_cron.create_task(_next_run))
    # task_retriable_cron_new.id = TASK_ID
    task_retriable_cron_new.scheduled_at = _next_run

    task_retriable_cron_completed = task_retriable_cron_new.model_copy(deep=True)
    _set_completed(task_retriable_cron_completed)

    task_retriable_cron_failed_should_retry = task_retriable_cron_new.model_copy(deep=True)
    _set_failed_should_retry(task_retriable_cron_failed_should_retry)

    task_retriable_cron_retried_completed = task_retriable_cron_failed_should_retry.model_copy(deep=True)
    _set_completed(task_retriable_cron_retried_completed)

    task_retriable_cron_retried_failed = task_retriable_cron_failed_should_retry.model_copy(deep=True)
    _set_failed(task_retriable_scheduled_retried_failed)

    tasks = {
        "new": task_new,
        "completed": task_completed,
        "failed": task_failed,
        "scheduled": task_scheduled,
        "scheduled_completed": task_scheduled_completed,
        "scheduled_failed": task_scheduled_failed,
        "cron_new": task_cron_new,
        "cron_completed": task_cron_completed,
        "cron_failed": task_cron_failed,
        "retriable_new": task_retriable_new,
        "retriable_completed": task_retriable_completed,
        "retriable_failed_should_retry": task_retriable_failed_should_retry,
        "retriable_retried_completed": task_retriable_retried_completed,
        "retriable_retried_failed": task_retriable_retried_failed,
        "retriable_scheduled": task_retriable_scheduled,
        "retriable_scheduled_completed": task_retriable_scheduled_completed,
        "retriable_scheduled_failed_should_retry": task_retriable_scheduled_failed_should_retry,
        "retriable_scheduled_retried_completed": task_retriable_scheduled_retried_completed,
        "retriable_scheduled_retried_failed": task_retriable_scheduled_retried_failed,
        "retriable_cron_new": task_retriable_cron_new,
        "retriable_cron_completed": task_retriable_cron_completed,
        "retriable_cron_failed_should_retry": task_retriable_cron_failed_should_retry,
        "retriable_cron_retried_completed": task_retriable_cron_retried_completed,
        "retriable_cron_retried_failed": task_retriable_cron_retried_failed,
    }

    return {k: v.create_task().model_dump(mode="json") for k, v in tasks.items()}


async def test_require_manual_connect(backend: Backend):
    if isinstance(backend, MemoryBackend):
        pytest.xfail("MemoryBackend is broken for this test")

    assert backend.is_connected is False

    with pytest.raises(BackendError):
        await backend.append(Q, {})


async def test_connect_and_disconnect(backend: Backend):
    assert backend.is_connected is False
    await backend.connect()
    assert backend.is_connected is True
    await backend.disconnect()
    assert backend.is_connected is False


async def test_append(task_dict: dict, backend: Backend):
    await backend.connect()

    assert await backend.size(Q) == 0

    success = await backend.append(Q, task_dict)
    assert success

    assert await backend.size(Q) == 1
    assert await backend.size("different_queue") == 0


async def test_append_bulk(backend: Backend):
    t1 = simple_async_task(1, 2).model_dump(mode="json")
    t2 = simple_async_task(1, 2).model_dump(mode="json")

    await backend.connect()
    success = await backend.append(Q, [t1, t2])
    assert success

    assert await backend.size(Q) == 2
    assert await backend.size("different_queue") == 0


@pytest.mark.skip("BUG")
async def test_append_prevents_same_task(task_dict: dict, backend: Backend):
    await backend.connect()

    assert await backend.size(Q) == 0

    success = await backend.append(Q, task_dict)
    assert success

    success = await backend.append(Q, task_dict)
    assert not success

    assert await backend.size(Q) == 1


async def test_pop(backend: Backend):
    t1 = simple_async_task(1, 2).model_dump(mode="json")
    t2 = simple_async_task(3, 4).model_dump(mode="json")
    t3 = simple_async_task(5, 6).model_dump(mode="json")
    t4 = simple_async_task(7, 8).model_dump(mode="json")

    await backend.connect()
    success = await backend.append(Q, [t1, t2, t3, t4])
    assert success

    assert await backend.size(Q) == 4
    assert await backend.size("different_queue") == 0

    assert await backend.pop("different-queue") == []
    assert await backend.pop("different-queue", limit=10) == []
    assert await backend.pop("different-queue", limit=10, timeout=0.01) == []

    assert await backend.pop(Q) == [t1]
    assert await backend.pop(Q, limit=2) == [t2, t3]
    assert await backend.pop(Q, limit=3, timeout=0.01) == [t4]


async def test_peek(backend: Backend):
    t1 = simple_async_task(1, 2).model_dump(mode="json")
    t2 = simple_async_task(3, 4).model_dump(mode="json")
    t3 = simple_async_task(5, 6).model_dump(mode="json")
    t4 = simple_async_task(7, 8).model_dump(mode="json")

    await backend.connect()
    success = await backend.append(Q, [t1, t2, t3, t4])
    assert success

    assert await backend.peek(Q) == [t1]
    assert await backend.peek(Q, count=2) == [t1, t2]
    assert await backend.peek(Q, count=50) == [t1, t2, t3, t4]

    assert await backend.peek("different-queue") == []
    assert await backend.peek("different-queue", count=2) == []
    assert await backend.peek("different-queue", count=50) == []


async def test_clear(backend: Backend):
    t1 = simple_async_task(1, 2).model_dump(mode="json")
    t2 = simple_async_task(3, 4).model_dump(mode="json")
    t3 = simple_async_task(5, 6).model_dump(mode="json")
    t4 = simple_async_task(7, 8).model_dump(mode="json")

    await backend.connect()
    success = await backend.append(Q, [t1, t2, t3, t4])
    assert success

    assert await backend.size(Q) == 4
    assert await backend.size("different-queue") == 0

    assert await backend.clear(Q) == 4
    assert await backend.clear("different-queue") == 0

    assert await backend.clear(Q) == 0
    assert await backend.clear("different-queue") == 0

    assert await backend.size(Q) == 0
    assert await backend.size("different-queue") == 0

    assert await backend.pop(Q) == []
    assert await backend.pop("different-queue") == []


async def test_get_task(datetime_now: datetime, tasks: dict[str, dict], backend: Backend):
    t1 = simple_async_task(1, 2).model_dump(mode="json")
    t2 = simple_async_task(3, 4).model_dump(mode="json")
    t3 = simple_async_task(5, 6).model_dump(mode="json")
    t4 = simple_async_task(7, 8).model_dump(mode="json")

    await backend.connect()
    assert await backend.append(Q, tasks["new"])
    assert await backend.pop(Q) == [tasks["new"]]
    assert await backend.store_result(Q, tasks["completed"])

    assert await backend.append(Q, [t1, t2])
    assert await backend.append("different-queue", [t4])

    assert await backend.schedule(Q, t3, at=datetime_now)

    assert await backend.size(Q) == 2
    assert await backend.size("different-queue") == 1

    assert await backend.get_task(Q, t1["id"]) == t1
    assert await backend.get_task(Q, t2["id"]) == t2
    assert await backend.get_task(Q, t3["id"]) == t3
    assert await backend.get_task(Q, str(t4["id"])) is None
    assert await backend.get_task(Q, str(tasks["completed"]["id"])) == tasks["completed"]

    assert await backend.get_task("different-queue", t1["id"]) is None
    assert await backend.get_task("different-queue", t2["id"]) is None
    assert await backend.get_task("different-queue", t3["id"]) is None
    assert await backend.get_task("different-queue", str(t4["id"])) == t4
    assert await backend.get_task("different-queue", str(tasks["completed"]["id"])) is None

    assert await backend.get_task("x", t1["id"]) is None
    assert await backend.get_task("x", t2["id"]) is None
    assert await backend.get_task("x", t3["id"]) is None
    assert await backend.get_task("x", str(t4["id"])) is None
    assert await backend.get_task("x", str(tasks["completed"]["id"])) is None


async def test_get_all_tasks(datetime_now: datetime, tasks: dict[str, dict], backend: Backend):
    t1 = simple_async_task(1, 2).model_dump(mode="json")
    t2 = simple_async_task(3, 4).model_dump(mode="json")
    t3 = simple_async_task(5, 6).model_dump(mode="json")
    t4 = simple_async_task(7, 8).model_dump(mode="json")

    await backend.connect()
    assert await backend.append(Q, tasks["new"])
    assert await backend.pop(Q) == [tasks["new"]]
    assert await backend.store_result(Q, tasks["completed"])

    assert await backend.schedule(Q, t3, at=datetime_now)

    assert await backend.append(Q, [t1, t2])
    assert await backend.append("different-queue", [t4])

    assert await backend.size(Q) == 2
    assert await backend.size("different-queue") == 1

    _tasks = await backend.get_all_tasks(Q)
    assert t1 in _tasks
    assert t2 in _tasks
    assert t3 in _tasks
    assert tasks["completed"] in _tasks

    assert await backend.get_all_tasks("different-queue") == [t4]

    assert await backend.stats(Q) == {"completed": 1, "pending": 2, "scheduled": 1}


async def test_list_queues(datetime_now: datetime, tasks: dict[str, dict], backend: Backend):
    t1 = simple_async_task(1, 2).model_dump(mode="json")
    t2 = simple_async_task(3, 4).model_dump(mode="json")
    t3 = simple_async_task(5, 6).model_dump(mode="json")
    t4 = simple_async_task(7, 8).model_dump(mode="json")

    await backend.connect()
    assert await backend.append(Q, tasks["new"])
    assert await backend.pop(Q) == [tasks["new"]]
    assert await backend.store_result(Q, tasks["completed"])

    assert await backend.schedule(Q, t3, at=datetime_now)

    assert await backend.append(Q, [t1, t2])
    assert await backend.append("different-queue", [t4])

    assert await backend.size(Q) == 2
    assert await backend.size("different-queue") == 1

    assert await backend.list_queues() == {Q: 2, "different-queue": 1}


async def test_list_scheduled(datetime_now: datetime, backend: Backend):
    t1 = simple_async_task(1, 2).model_dump(mode="json")
    t2 = simple_async_task(3, 4).model_dump(mode="json")

    await backend.connect()
    assert await backend.schedule(Q, t1, at=datetime_now)
    assert await backend.schedule(Q, t2, at=datetime_now)

    tasks = await backend.list_scheduled(Q)
    assert len(tasks) == 2
    assert t1 in tasks
    assert t2 in tasks
    assert await backend.list_scheduled("different-queue") == []


class TestGetResult:
    async def _setup_append(self, task_data: dict, backend: Backend):
        await backend.connect()
        assert await backend.size(Q) == 0
        success = await backend.append(Q, task_data)
        assert success
        assert await backend.size(Q) == 1

    async def _setup_schedule(self, task_data: dict, backend: Backend):
        await backend.connect()
        assert await backend.size(Q) == 0
        success = await backend.schedule(Q, task_data, at=TypeAdapter(datetime).validate_python(task_data["scheduled_at"]))
        assert success
        assert await backend.size(Q) == 0
        ret = await backend.get_scheduled(Q, datetime.now(timezone.utc))
        assert await backend.size(Q) == 0
        success = await backend.append(Q, ret[0])
        assert success
        assert await backend.size(Q) == 1

    async def _setup_processing(self, task_data: dict, backend: Backend):
        await self._setup_append(task_data, backend)
        assert await backend.size(Q) == 1
        _tasks = await backend.pop(Q)
        assert _tasks and _tasks[0]
        # assert await backend.size(Q) == 0  # ! FIXME (redis)
        return _tasks[0]

    async def _setup_schedule_processing(self, task_data: dict, backend: Backend):
        await self._setup_schedule(task_data, backend)
        assert await backend.size(Q) == 1
        _tasks = await backend.pop(Q)
        assert _tasks and _tasks[0]
        # assert await backend.size(Q) == 0  # ! FIXME (redis)
        return _tasks[0]

    def _simulate_processing(self, task_data: dict, backend: Backend):
        async def processing():
            await asyncio.sleep(0.02)
            await backend.store_result(Q, task_data)

        return asyncio.create_task(processing())

    async def test_new(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_append(tasks["new"], backend)

        task = await backend.get_result(Q, str(tasks["new"]["id"]))
        assert task is None

    async def test_new_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_append(tasks["new"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["new"]["id"]), timeout=.01)

    async def test_completed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["new"], backend)
        await backend.store_result(Q, tasks["completed"])

        task = await backend.get_result(Q, str(tasks["new"]["id"]))
        assert task is not None
        assert task == tasks["completed"]

    async def test_completed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["new"], backend)
        self._simulate_processing(tasks["completed"], backend)

        # timeout sooner than processing, must fail
        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["new"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["completed"]

    async def test_failed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["new"], backend)
        await backend.store_result(Q, tasks["failed"])

        task = await backend.get_result(Q, str(tasks["new"]["id"]))
        assert task is not None
        assert task == tasks["failed"]

    async def test_failed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["new"], backend)
        self._simulate_processing(tasks["failed"], backend)

        # timeout sooner than processing, must fail
        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["new"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["failed"]

    async def test_scheduled(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule(tasks["scheduled"], backend)

        task = await backend.get_result(Q, str(tasks["scheduled"]["id"]))
        assert task is None

    async def test_scheduled_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule(tasks["scheduled"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["scheduled"]["id"]), timeout=.01)

    async def test_scheduled_completed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["scheduled"], backend)
        await backend.store_result(Q, tasks["scheduled_completed"])

        task = await backend.get_result(Q, str(tasks["scheduled"]["id"]))
        assert task is not None
        assert task == tasks["scheduled_completed"]

    async def test_scheduled_completed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["scheduled"], backend)
        self._simulate_processing(tasks["scheduled_completed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["scheduled"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["scheduled"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["scheduled_completed"]

    async def test_scheduled_failed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["scheduled"], backend)
        await backend.store_result(Q, tasks["scheduled_failed"])

        task = await backend.get_result(Q, str(tasks["scheduled"]["id"]))
        assert task is not None
        assert task == tasks["scheduled_failed"]

    async def test_scheduled_failed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["scheduled"], backend)
        self._simulate_processing(tasks["scheduled_failed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["scheduled"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["scheduled"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["scheduled_failed"]

    async def test_cron_new(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule(tasks["cron_new"], backend)

        task = await backend.get_result(Q, str(tasks["cron_new"]["id"]))
        assert task is None

    async def test_cron_new_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule(tasks["cron_new"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["cron_new"]["id"]), timeout=.01)

    async def test_cron_completed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["cron_new"], backend)
        await backend.store_result(Q, tasks["cron_completed"])

        task = await backend.get_result(Q, str(tasks["cron_new"]["id"]))
        assert task is not None
        assert task == tasks["cron_completed"]

    async def test_cron_completed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["cron_new"], backend)
        self._simulate_processing(tasks["cron_completed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["cron_new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["cron_new"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["cron_completed"]

    async def test_cron_failed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["cron_new"], backend)
        await backend.store_result(Q, tasks["cron_failed"])

        task = await backend.get_result(Q, str(tasks["cron_new"]["id"]))
        assert task is not None
        assert task == tasks["cron_failed"]

    async def test_cron_failed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["cron_new"], backend)
        self._simulate_processing(tasks["cron_failed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["cron_new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["cron_new"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["cron_failed"]

    async def test_retriable_new(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_append(tasks["retriable_new"], backend)

        task = await backend.get_result(Q, str(tasks["retriable_new"]["id"]))
        assert task is None

    async def test_retriable_new_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_append(tasks["retriable_new"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_new"]["id"]), timeout=.01)

    async def test_retriable_completed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["retriable_new"], backend)
        await backend.store_result(Q, tasks["retriable_completed"])

        task = await backend.get_result(Q, str(tasks["retriable_new"]["id"]))
        assert task is not None
        assert task == tasks["retriable_completed"]

    async def test_retriable_completed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["retriable_new"], backend)
        self._simulate_processing(tasks["retriable_completed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["retriable_new"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["retriable_completed"]

    async def test_retriable_failed_should_retry(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["retriable_new"], backend)
        await backend.store_result(Q, tasks["retriable_failed_should_retry"])
        await self._setup_schedule(tasks["retriable_failed_should_retry"], backend)

        task = await backend.get_result(Q, str(tasks["retriable_new"]["id"]))
        assert task is None

    async def test_retriable_failed_should_retry_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["retriable_new"], backend)
        await backend.store_result(Q, tasks["retriable_failed_should_retry"])
        await self._setup_schedule(tasks["retriable_failed_should_retry"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_new"]["id"]), timeout=.01)

    async def test_retriable_retried_completed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["retriable_new"], backend)
        await backend.store_result(Q, tasks["retriable_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_failed_should_retry"], backend)
        await backend.store_result(Q, tasks["retriable_retried_completed"])

        task = await backend.get_result(Q, str(tasks["retriable_new"]["id"]))
        assert task is not None
        assert task == tasks["retriable_retried_completed"]

    async def test_retriable_retried_completed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["retriable_new"], backend)
        await backend.store_result(Q, tasks["retriable_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_failed_should_retry"], backend)
        self._simulate_processing(tasks["retriable_retried_completed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["retriable_new"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["retriable_retried_completed"]

    async def test_retriable_retried_failed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["retriable_new"], backend)
        await backend.store_result(Q, tasks["retriable_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_failed_should_retry"], backend)
        await backend.store_result(Q, tasks["retriable_retried_failed"])

        task = await backend.get_result(Q, str(tasks["retriable_new"]["id"]))
        assert task is not None
        assert task == tasks["retriable_retried_failed"]

    async def test_retriable_retried_failed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_processing(tasks["retriable_new"], backend)
        await backend.store_result(Q, tasks["retriable_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_failed_should_retry"], backend)
        self._simulate_processing(tasks["retriable_retried_failed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["retriable_new"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["retriable_retried_failed"]

    async def test_retriable_scheduled(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule(tasks["retriable_scheduled"], backend)

        task = await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]))
        assert task is None

    async def test_retriable_scheduled_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule(tasks["retriable_scheduled"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]), timeout=.01)

    async def test_retriable_scheduled_completed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_scheduled"], backend)
        await backend.store_result(Q, tasks["retriable_scheduled_completed"])

        task = await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]))
        assert task is not None
        assert task == tasks["retriable_scheduled_completed"]

    async def test_retriable_scheduled_completed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_scheduled"], backend)
        self._simulate_processing(tasks["retriable_scheduled_completed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["retriable_scheduled_completed"]

    async def test_retriable_scheduled_failed_should_retry(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_scheduled"], backend)
        await backend.store_result(Q, tasks["retriable_scheduled_failed_should_retry"])
        await self._setup_schedule(tasks["retriable_scheduled_failed_should_retry"], backend)

        task = await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]))
        assert task is None

    async def test_retriable_scheduled_failed_should_retry_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_scheduled"], backend)
        await backend.store_result(Q, tasks["retriable_scheduled_failed_should_retry"])
        await self._setup_schedule(tasks["retriable_scheduled_failed_should_retry"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]), timeout=.01)

    async def test_retriable_scheduled_retried_completed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_scheduled"], backend)
        await backend.store_result(Q, tasks["retriable_scheduled_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_scheduled_failed_should_retry"], backend)
        await backend.store_result(Q, tasks["retriable_scheduled_retried_completed"])

        task = await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]))
        assert task is not None
        assert task == tasks["retriable_scheduled_retried_completed"]

    async def test_retriable_scheduled_retried_completed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_scheduled"], backend)
        await backend.store_result(Q, tasks["retriable_scheduled_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_scheduled_failed_should_retry"], backend)
        self._simulate_processing(tasks["retriable_scheduled_retried_completed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]), timeout=.1)
        assert task is not None
        assert task == tasks["retriable_scheduled_retried_completed"]

    async def test_retriable_scheduled_retried_failed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_scheduled"], backend)
        await backend.store_result(Q, tasks["retriable_scheduled_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_scheduled_failed_should_retry"], backend)
        await backend.store_result(Q, tasks["retriable_scheduled_retried_failed"])

        task = await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]))
        assert task is not None
        assert task == tasks["retriable_scheduled_retried_failed"]

    async def test_retriable_scheduled_retried_failed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_scheduled"], backend)
        await backend.store_result(Q, tasks["retriable_scheduled_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_scheduled_failed_should_retry"], backend)
        self._simulate_processing(tasks["retriable_scheduled_retried_failed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["retriable_scheduled"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["retriable_scheduled_retried_failed"]

    async def test_retriable_cron_new(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule(tasks["retriable_cron_new"], backend)

        task = await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]))
        assert task is None

    async def test_retriable_cron_new_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule(tasks["retriable_cron_new"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]), timeout=.01)

    async def test_retriable_cron_completed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_cron_new"], backend)
        await backend.store_result(Q, tasks["retriable_cron_completed"])

        task = await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]))
        assert task is not None
        assert task == tasks["retriable_cron_completed"]

    async def test_retriable_cron_completed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_cron_new"], backend)
        self._simulate_processing(tasks["retriable_cron_completed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["retriable_cron_completed"]

    async def test_retriable_cron_failed_should_retry(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_cron_new"], backend)
        await backend.store_result(Q, tasks["retriable_cron_failed_should_retry"])
        await self._setup_schedule(tasks["retriable_cron_failed_should_retry"], backend)

        task = await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]))
        assert task is None

    async def test_retriable_cron_failed_should_retry_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_cron_new"], backend)
        await backend.store_result(Q, tasks["retriable_cron_failed_should_retry"])
        await self._setup_schedule(tasks["retriable_cron_failed_should_retry"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]), timeout=.01)

    async def test_retriable_cron_retried_completed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_cron_new"], backend)
        await backend.store_result(Q, tasks["retriable_cron_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_cron_failed_should_retry"], backend)
        await backend.store_result(Q, tasks["retriable_cron_retried_completed"])

        task = await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]))
        assert task is not None
        assert task == tasks["retriable_cron_retried_completed"]

    async def test_retriable_cron_retried_completed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_cron_new"], backend)
        await backend.store_result(Q, tasks["retriable_cron_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_cron_failed_should_retry"], backend)
        self._simulate_processing(tasks["retriable_cron_retried_completed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]), timeout=.02)
        assert task is not None
        assert task == tasks["retriable_cron_retried_completed"]

    async def test_retriable_cron_retried_failed(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_cron_new"], backend)
        await backend.store_result(Q, tasks["retriable_cron_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_cron_failed_should_retry"], backend)
        await backend.store_result(Q, tasks["retriable_cron_retried_failed"])

        task = await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]))
        assert task is None

    @pytest.mark.skip("BUG")
    async def test_retriable_cron_retried_failed_w_timeout(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_cron_new"], backend)
        await backend.store_result(Q, tasks["retriable_cron_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_cron_failed_should_retry"], backend)
        self._simulate_processing(tasks["retriable_cron_retried_failed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]), timeout=.01)

        task = await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]), timeout=0.2)
        assert task is not None
        assert task == tasks["retriable_cron_retried_failed"]

    @pytest.mark.skip("BUG (redis)")
    async def test_retriable_cron_retried_failed_w_timeout_weird(self, tasks: dict[str, dict], backend: Backend):
        await self._setup_schedule_processing(tasks["retriable_cron_new"], backend)
        await backend.store_result(Q, tasks["retriable_cron_failed_should_retry"])
        await self._setup_schedule_processing(tasks["retriable_cron_failed_should_retry"], backend)
        self._simulate_processing(tasks["retriable_cron_retried_failed"], backend)

        with pytest.raises(TimeoutError):
            await backend.get_result(Q, str(tasks["retriable_cron_new"]["id"]), timeout=0.2)


async def test_get_result(task_dict: dict, backend: Backend):
    await backend.connect()

    assert await backend.size(Q) == 0
    await backend.append(Q, task_dict)
    assert await backend.size(Q) == 1

    popped_tasks = await backend.pop(Q)
    popped_task = popped_tasks[0].copy()
    assert task_dict == popped_task

    # modify task to a completed task
    popped_task |= {"completed": True, "result": 3, "finished_at": datetime.now(timezone.utc).isoformat()}
    success = await backend.store_result(Q, popped_task)
    assert success

    result = await backend.get_result(Q, task_dict['id'])
    assert result is not None


async def test_get_result_non_existent_task(task_dict: dict, backend: Backend):
    await backend.connect()

    assert await backend.size(Q) == 0

    result = await backend.get_result(Q, task_dict['id'])
    assert result is None


async def test_get_result_timeout(backend: Backend):
    await backend.connect()

    with pytest.raises(TimeoutError, match="Task random-id did not complete within 0.1 seconds"):
        await backend.get_result(Q, "random-id", timeout=0.1)


async def test_cron(backend: Backend):
    _task_cron = TaskFactory.create_cron_from_task(simple_async_task(1, 2), "* * * * *")

    deterministic_id = str(_task_cron.deterministic_id)
    cron_data = _task_cron.model_dump(mode="json")

    await backend.connect()
    assert await backend.add_cron(Q, deterministic_id, cron_data) is True
    assert await backend.add_cron(Q, deterministic_id, cron_data) is False

    assert await backend.list_crons(Q) == [cron_data]
    assert await backend.list_crons("different-queue") == []

    assert await backend.delete_cron("different-queue", deterministic_id) is False
    assert await backend.delete_cron(Q, deterministic_id) is True
    assert await backend.delete_cron(Q, deterministic_id) is False

    assert await backend.list_crons(Q) == []
    assert await backend.list_crons("different-queue") == []
