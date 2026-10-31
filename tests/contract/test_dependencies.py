import asyncio
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Sequence

from sheppy import CURRENT_TASK, Depends, Queue, Task, Worker, task
from tests.dependencies import assert_is_completed, assert_is_failed

dependency_calls = 0
events: list[str] = []


@pytest.fixture(autouse=True)
def reset_dependency_state():
    global dependency_calls
    dependency_calls = 0
    events.clear()


def counted_dependency() -> int:
    global dependency_calls
    dependency_calls += 1
    return dependency_calls


@task
def cached_dependencies_task(
    first: int = Depends(counted_dependency),
    second: int = Depends(counted_dependency),
    fresh: int = Depends(counted_dependency, use_cache=False),
    cached_again: int = Depends(counted_dependency),
) -> tuple[int, int, int, int]:
    return first, second, fresh, cached_again


def sync_resource():
    events.append("sync-enter")
    try:
        yield "sync"
    finally:
        events.append("sync-exit")


async def async_resource():
    events.append("async-enter")
    try:
        yield "async"
    finally:
        events.append("async-exit")


@task
def sync_generator_task(value: str = Depends(sync_resource)) -> str:
    events.append("task")
    return value


@task
def async_generator_task(value: str = Depends(async_resource)) -> str:
    events.append("task")
    return value


@task
def failing_generator_task(value: str = Depends(sync_resource)) -> str:
    del value
    events.append("task")
    raise ValueError("task failed")


def scoped_resource():
    events.append("dependency-enter")
    try:
        yield "value"
    finally:
        events.append("dependency-exit")


def scope_middleware(task: Task, _queue: Queue):
    events.append("middleware-before")
    task, exception = yield
    events.append("middleware-after")
    return task


@task(middleware=[scope_middleware])
def function_scope_task(value: str = Depends(scoped_resource, scope="function")) -> str:
    events.append("task")
    return value


@task(middleware=[scope_middleware])
def request_scope_task(value: str = Depends(scoped_resource, scope="request")) -> str:
    events.append("task")
    return value


def retry_resource():
    global dependency_calls
    dependency_calls += 1
    current = dependency_calls
    events.append(f"enter-{current}")
    try:
        yield current
    finally:
        events.append(f"exit-{current}")


@task(retry=1, retry_delay=0)
def retry_generator_task(
    current: Task = CURRENT_TASK,
    value: int = Depends(retry_resource),
) -> int:
    if current.retry_count == 0:
        raise ValueError("retry")
    return value


async def test_dependency_cache_and_opt_out(queue: Queue, worker: Worker):
    task_obj = cached_dependencies_task()
    await queue.add(task_obj)

    await worker.work(1)

    processed = await queue.get_task(task_obj)
    assert_is_completed(processed)
    assert processed.result == (1, 1, 2, 1)
    assert dependency_calls == 2


@pytest.mark.parametrize(
    ("task_factory", "resource_name"),
    [(sync_generator_task, "sync"), (async_generator_task, "async")],
    ids=["sync", "async"],
)
async def test_generator_teardown(task_factory, resource_name: str, queue: Queue, worker: Worker):
    task_obj = task_factory()
    await queue.add(task_obj)

    await worker.work(1)

    processed = await queue.get_task(task_obj)
    assert_is_completed(processed)
    assert processed.result == resource_name
    assert events == [f"{resource_name}-enter", "task", f"{resource_name}-exit"]


async def test_generator_teardown_after_task_failure(queue: Queue, worker: Worker):
    task_obj = failing_generator_task()
    await queue.add(task_obj)

    await worker.work(1)

    processed = await queue.get_task(task_obj)
    assert_is_failed(processed)
    assert str(processed.exception) == "ValueError: task failed"
    assert events == ["sync-enter", "task", "sync-exit"]


@pytest.mark.parametrize(
    ("task_factory", "expected_events"),
    [
        (
            function_scope_task,
            ["middleware-before", "dependency-enter", "task", "dependency-exit", "middleware-after"],
        ),
        (
            request_scope_task,
            ["middleware-before", "dependency-enter", "task", "middleware-after", "dependency-exit"],
        ),
    ],
    ids=["function", "request"],
)
async def test_dependency_scope_order(task_factory, expected_events: list[str], queue: Queue, worker: Worker):
    task_obj = task_factory()
    await queue.add(task_obj)

    await worker.work(1)

    processed = await queue.get_task(task_obj)
    assert_is_completed(processed)
    assert events == expected_events


async def test_retry_gets_fresh_generator_and_cache(queue: Queue, worker: Worker):
    task_obj = retry_generator_task()
    await queue.add(task_obj)

    await asyncio.wait_for(worker.work(2), timeout=5)

    processed = await queue.get_task(task_obj)
    assert_is_completed(processed)
    assert processed.retry_count == 1
    assert processed.result == 2
    assert dependency_calls == 2
    assert events == ["enter-1", "exit-1", "enter-2", "exit-2"]


@task
def explicit_value_task(value: int = Depends(counted_dependency)) -> int:
    return value


@pytest.mark.parametrize(("args", "kwargs"), [((1,), {}), ((), {"value": 1})], ids=["positional", "keyword"])
async def test_explicit_values_bypass_dependency_injection(args, kwargs, queue: Queue, worker: Worker):
    task_obj = explicit_value_task(*args, **kwargs)
    await queue.add(task_obj)

    await worker.work(1)

    processed = await queue.get_task(task_obj)
    assert_is_completed(processed)
    assert processed.result == 1
    assert dependency_calls == 0


def current_task_dependency(current: Task = CURRENT_TASK) -> Task:
    return current


@task
def nested_current_task(
    current: Task = CURRENT_TASK,
    nested: Task = Depends(current_task_dependency),
) -> bool:
    return current is nested


async def test_current_task_is_injected_recursively(queue: Queue, worker: Worker):
    task_obj = nested_current_task()
    await queue.add(task_obj)

    await worker.work(1)

    processed = await queue.get_task(task_obj)
    assert_is_completed(processed)
    assert processed.result is True


@task
def current_task_after_dependency_task(
    value: int = Depends(counted_dependency),
    current: Task = CURRENT_TASK,
) -> tuple[int, str]:
    return value, str(current.id)


async def test_current_task_can_follow_a_dependency(queue: Queue, worker: Worker):
    task_obj = current_task_after_dependency_task()
    await queue.add(task_obj)

    await worker.work(1)

    processed = await queue.get_task(task_obj)
    assert_is_completed(processed)
    assert processed.result == (1, str(processed.id))


@task
def unresolved_parameter_annotation_task(
    value: "Sequence[int]" = Depends(counted_dependency),
) -> int:
    return value


async def test_unresolved_parameter_annotation(queue: Queue, worker: Worker):
    task_obj = unresolved_parameter_annotation_task()
    await queue.add(task_obj)

    await worker.work(1)

    processed = await queue.get_task(task_obj)
    assert_is_completed(processed)
    assert processed.result == 1


def request_cleanup_error_resource():
    try:
        yield "value"
    finally:
        raise RuntimeError("request cleanup failed")


@task
def request_cleanup_error_task(value: str = Depends(request_cleanup_error_resource)) -> str:
    events.append("task")
    return value


async def test_request_cleanup_error_fails_task(queue: Queue, worker: Worker):
    task_obj = request_cleanup_error_task()
    await queue.add(task_obj)

    await worker.work(1)

    processed = await queue.get_task(task_obj)
    assert_is_failed(processed)
    assert str(processed.exception) == "RuntimeError: request cleanup failed"
    assert processed.result is None
    assert processed.retry_count == 0
    assert processed.next_retry_at is None
    assert events == ["task"]
