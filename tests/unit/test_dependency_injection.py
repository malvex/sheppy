import asyncio
from typing import TYPE_CHECKING, Annotated

if TYPE_CHECKING:
    from collections.abc import Sequence

import pytest

from sheppy import CURRENT_TASK, Depends, MemoryBackend, Queue, Task, task
from sheppy._utils.task_execution import TaskProcessor
from sheppy.exceptions import DependencyScopeError, TaskTimeoutError

calls = 0
events: list[str] = []


@pytest.fixture(autouse=True)
def reset_dependency_state():
    global calls
    calls = 0
    events.clear()


async def execute(task_obj: Task, processor: TaskProcessor | None = None) -> tuple[Exception | None, Task]:
    queue = Queue(MemoryBackend(instant_processing=False), "dependency-test")
    return await (processor or TaskProcessor()).process_task(task_obj, queue, "test-worker")


def count_dependency() -> int:
    global calls
    calls += 1
    return calls


@task
def cached_dependency_task(
    first: int = Depends(count_dependency),
    second: int = Depends(count_dependency),
) -> tuple[int, int]:
    return first, second


def left_parent(value: int = Depends(count_dependency)) -> int:
    return value


def right_parent(value: int = Depends(count_dependency)) -> int:
    return value


@task
def nested_cache_task(
    left: int = Depends(left_parent),
    right: int = Depends(right_parent),
) -> tuple[int, int]:
    return left, right


def original_dependency() -> int:
    raise AssertionError("original dependency should be overridden")


def other_original_dependency() -> int:
    raise AssertionError("original dependency should be overridden")


def override_dependency() -> int:
    global calls
    calls += 1
    return calls


@task
def overridden_cache_task(
    first: int = Depends(original_dependency),
    second: int = Depends(original_dependency),
) -> tuple[int, int]:
    return first, second


@task
def distinct_originals_cache_task(
    first: int = Depends(original_dependency),
    second: int = Depends(other_original_dependency),
) -> tuple[int, int]:
    return first, second


def counted_generator():
    global calls
    calls += 1
    current = calls
    events.append(f"enter-{current}")
    try:
        yield current
    finally:
        events.append(f"exit-{current}")


@task
def cached_generator_task(
    first: int = Depends(counted_generator),
    second: int = Depends(counted_generator),
) -> tuple[int, int]:
    return first, second


@task
def uncached_generator_task(
    first: int = Depends(counted_generator),
    second: int = Depends(counted_generator, use_cache=False),
) -> tuple[int, int]:
    return first, second


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


@task(timeout=0.01)
async def timeout_generator_task(value: str = Depends(async_resource, scope="function")) -> str:
    events.append("task")
    await asyncio.sleep(1)
    return value


def inner_resource():
    events.append("inner-enter")
    try:
        yield "inner"
    finally:
        events.append("inner-exit")


def outer_resource(inner: str = Depends(inner_resource)):
    events.append("outer-enter")
    try:
        yield f"outer-{inner}"
    finally:
        events.append("outer-exit")


@task
def nested_generator_task(value: str = Depends(outer_resource)) -> str:
    events.append("task")
    return value


def function_child_resource():
    yield "child"


def request_parent_resource(child: str = Depends(function_child_resource, scope="function")):
    yield child


@task
def invalid_scope_task(value: str = Depends(request_parent_resource)) -> str:
    return value


def exception_resource():
    events.append("dependency-enter")
    try:
        yield "value"
    except ValueError:
        events.append("dependency-saw-error")
        raise
    finally:
        events.append("dependency-exit")


@task
def function_scope_failure_task(value: str = Depends(exception_resource, scope="function")) -> str:
    del value
    events.append("task")
    raise ValueError("task failed")


def broken_cleanup_resource():
    try:
        yield "value"
    finally:
        raise RuntimeError("cleanup failed")


@task(retry=1, retry_delay=0)
def cleanup_failure_task(value: str = Depends(broken_cleanup_resource)) -> str:
    events.append("task")
    return value


@task(retry=1, retry_delay=0)
def failed_attempt_cleanup_failure_task(value: str = Depends(broken_cleanup_resource)) -> str:
    del value
    raise ValueError("task failed")


class InferredDependency:
    value = 42


@task
def inferred_default_dependency_task(value: InferredDependency = Depends()) -> int:
    return value.value


@task
def inferred_annotated_dependency_task(value: Annotated[InferredDependency, Depends()]) -> int:
    return value.value


@task
def string_annotated_dependency_task(value: "Annotated[int, Depends(count_dependency)]") -> int:
    return value


class AsyncCallableDependency:
    async def __call__(self) -> int:
        return 42


class SyncGeneratorCallableDependency:
    def __call__(self):
        events.append("sync-callable-enter")
        try:
            yield 42
        finally:
            events.append("sync-callable-exit")


class AsyncGeneratorCallableDependency:
    async def __call__(self):
        events.append("async-callable-enter")
        try:
            yield 42
        finally:
            events.append("async-callable-exit")


@task
def async_callable_dependency_task(value: int = Depends(AsyncCallableDependency())) -> int:
    return value


@task
def sync_generator_callable_dependency_task(
    value: int = Depends(SyncGeneratorCallableDependency()),
) -> int:
    return value


@task
def async_generator_callable_dependency_task(
    value: int = Depends(AsyncGeneratorCallableDependency()),
) -> int:
    return value


@task
def request_scope_failure_task(value: str = Depends(exception_resource, scope="request")) -> str:
    del value
    events.append("task")
    raise ValueError("task failed")


async def test_depends_infers_default_dependency_from_annotation():
    exception, processed = await execute(inferred_default_dependency_task())

    assert exception is None
    assert processed.result == 42


async def test_depends_infers_annotated_dependency_from_annotation():
    exception, processed = await execute(inferred_annotated_dependency_task())

    assert exception is None
    assert processed.result == 42


async def test_string_annotated_dependency_is_resolved():
    exception, processed = await execute(string_annotated_dependency_task())

    assert exception is None
    assert processed.result == 1
    assert calls == 1


@pytest.mark.parametrize(
    ("task_factory", "expected_events"),
    [
        (async_callable_dependency_task, []),
        (
            sync_generator_callable_dependency_task,
            ["sync-callable-enter", "sync-callable-exit"],
        ),
        (
            async_generator_callable_dependency_task,
            ["async-callable-enter", "async-callable-exit"],
        ),
    ],
    ids=["async", "sync-generator", "async-generator"],
)
async def test_callable_object_dependencies(task_factory, expected_events: list[str]):
    exception, processed = await execute(task_factory())

    assert exception is None
    assert processed.result == 42
    assert events == expected_events


async def test_request_scoped_generator_receives_task_exception():
    exception, processed = await execute(request_scope_failure_task())

    assert isinstance(exception, ValueError)
    assert processed.status == "failed"
    assert events == [
        "dependency-enter",
        "task",
        "dependency-saw-error",
        "dependency-exit",
    ]


async def test_nested_dependencies_share_cache():
    exception, processed = await execute(nested_cache_task())

    assert exception is None
    assert processed.result == (1, 1)
    assert calls == 1


async def test_cache_does_not_cross_task_attempts():
    _, first = await execute(cached_dependency_task())
    _, second = await execute(cached_dependency_task())

    assert first.result == (1, 1)
    assert second.result == (2, 2)
    assert calls == 2


async def test_overrides_share_the_original_dependency_cache_entry():
    processor = TaskProcessor(dependency_overrides={original_dependency: override_dependency})
    exception, processed = await execute(overridden_cache_task(), processor)

    assert exception is None
    assert processed.result == (1, 1)
    assert calls == 1


async def test_distinct_originals_do_not_share_override_cache_entries():
    processor = TaskProcessor(dependency_overrides={
        original_dependency: override_dependency,
        other_original_dependency: override_dependency,
    })
    exception, processed = await execute(distinct_originals_cache_task(), processor)

    assert exception is None
    assert processed.result == (1, 2)
    assert calls == 2


async def test_cached_generator_has_one_lifecycle():
    exception, processed = await execute(cached_generator_task())

    assert exception is None
    assert processed.result == (1, 1)
    assert calls == 1
    assert events == ["enter-1", "exit-1"]


async def test_uncached_generators_have_independent_lifecycles():
    exception, processed = await execute(uncached_generator_task())

    assert exception is None
    assert processed.result == (1, 2)
    assert calls == 2
    assert events == ["enter-1", "enter-2", "exit-2", "exit-1"]


async def test_generator_dependencies_close_after_timeout():
    exception, processed = await execute(timeout_generator_task())

    assert isinstance(exception, TaskTimeoutError)
    assert processed.status == "failed"
    assert events == ["async-enter", "task", "async-exit"]


async def test_nested_generators_close_in_reverse_order():
    exception, processed = await execute(nested_generator_task())

    assert exception is None
    assert processed.result == "outer-inner"
    assert events == [
        "inner-enter",
        "outer-enter",
        "task",
        "outer-exit",
        "inner-exit",
    ]


async def test_request_generator_cannot_depend_on_function_scoped_dependency():
    exception, processed = await execute(invalid_scope_task())

    assert isinstance(exception, DependencyScopeError)
    assert processed.status == "failed"
    assert events == []


async def test_function_scoped_generator_receives_task_exception():
    exception, processed = await execute(function_scope_failure_task())

    assert isinstance(exception, ValueError)
    assert processed.status == "failed"
    assert events == [
        "dependency-enter",
        "task",
        "dependency-saw-error",
        "dependency-exit",
    ]


async def test_request_scope_cleanup_failure_marks_task_failed_and_schedules_retry():
    exception, processed = await execute(cleanup_failure_task())

    assert isinstance(exception, RuntimeError)
    assert str(exception) == "cleanup failed"
    assert processed.status == "retrying"
    assert processed.result is None
    assert str(processed.exception) == "RuntimeError: cleanup failed"
    assert processed.retry_count == 1
    assert processed.next_retry_at is not None
    assert events == ["task"]


async def test_cleanup_failure_during_failed_attempt_preserves_retry_schedule():
    exception, processed = await execute(failed_attempt_cleanup_failure_task())

    assert isinstance(exception, RuntimeError)
    assert str(exception) == "cleanup failed"
    assert processed.status == "retrying"
    assert processed.retry_count == 1
    assert processed.next_retry_at is not None


InferredDependencyAlias = Annotated[InferredDependency, Depends()]


@task
def inferred_alias_task(value: InferredDependencyAlias) -> int:
    return value.value


def first_value_dependency() -> str:
    return "first"


def second_value_dependency() -> str:
    return "second"


@task
def duplicate_depends_declaration_task(
    value: Annotated[str, Depends(first_value_dependency)] = Depends(second_value_dependency),
) -> str:
    return value


@task
def scope_sensitive_cache_task(
    function_value: int = Depends(counted_generator, scope="function"),
    request_value: int = Depends(counted_generator, scope="request"),
) -> tuple[int, int]:
    return function_value, request_value


def failing_dependency() -> str:
    events.append("failing-dependency")
    raise ValueError("dependency failed")


@task
def partially_resolved_dependency_task(
    resource: str = Depends(exception_resource),
    failed: str = Depends(failing_dependency),
) -> str:
    return resource + failed


def no_yield_resource():
    yield from ()


@task
def no_yield_task(value: str = Depends(no_yield_resource)) -> str:
    return value


def multiple_yield_resource():
    yield "first"
    yield "second"


@task
def multiple_yield_task(value: str = Depends(multiple_yield_resource)) -> str:
    return value


def nested_generator_override(value: int = Depends(count_dependency)):
    events.append("override-enter")
    try:
        yield value
    finally:
        events.append("override-exit")


@task(timeout=0.01)
async def request_timeout_generator_task(
    value: str = Depends(async_resource, scope="request"),
) -> str:
    events.append("task")
    await asyncio.sleep(1)
    return value


async def test_annotated_alias_dependency():
    exception, processed = await execute(inferred_alias_task())

    assert exception is None
    assert processed.result == 42


def test_annotated_and_default_dependencies_are_rejected():
    with pytest.raises(AssertionError) as exc_info:
        duplicate_depends_declaration_task()

    assert str(exc_info.value) == (
        "Cannot specify `Depends` in `Annotated` and default value together for 'value'"
    )


async def test_dependency_scope_is_part_of_cache_key():
    exception, processed = await execute(scope_sensitive_cache_task())

    assert exception is None
    assert processed.result == (1, 2)
    assert calls == 2
    assert events == ["enter-1", "enter-2", "exit-1", "exit-2"]


async def test_resolution_failure_unwinds_already_entered_dependencies():
    exception, processed = await execute(partially_resolved_dependency_task())

    assert isinstance(exception, ValueError)
    assert str(exception) == "dependency failed"
    assert processed.status == "failed"
    assert events == [
        "dependency-enter",
        "failing-dependency",
        "dependency-saw-error",
        "dependency-exit",
    ]


async def test_generator_dependency_must_yield():
    exception, processed = await execute(no_yield_task())

    assert isinstance(exception, RuntimeError)
    assert str(exception) == "generator didn't yield"
    assert processed.status == "failed"


async def test_multiple_yield_cleanup_failure_marks_task_failed():
    exception, processed = await execute(multiple_yield_task())

    assert isinstance(exception, RuntimeError)
    assert str(exception) == "generator didn't stop"
    assert processed.status == "failed"
    assert processed.result is None


async def test_generator_override_resolves_nested_dependencies_once():
    processor = TaskProcessor(dependency_overrides={original_dependency: nested_generator_override})
    exception, processed = await execute(overridden_cache_task(), processor)

    assert exception is None
    assert processed.result == (1, 1)
    assert calls == 1
    assert events == ["override-enter", "override-exit"]


async def test_request_generator_dependencies_close_after_timeout():
    exception, processed = await execute(request_timeout_generator_task())

    assert isinstance(exception, TaskTimeoutError)
    assert processed.status == "failed"
    assert events == ["async-enter", "task", "async-exit"]


@task
def unresolved_return_annotation_task(value: int = Depends(count_dependency)) -> "Sequence[int]":
    return value


async def test_unresolved_task_return_annotation_does_not_fail_execution():
    exception, processed = await execute(unresolved_return_annotation_task())

    assert exception is None
    assert processed.result == 1


def current_task_after_default_dependency(
    value: int = 7,
    current: Task = CURRENT_TASK,
) -> tuple[int, str]:
    return value, str(current.id)


@task
def nested_current_task_after_default(
    nested: tuple[int, str] = Depends(current_task_after_default_dependency),
    current: Task = CURRENT_TASK,
) -> bool:
    return nested == (7, str(current.id))


async def test_current_task_can_follow_a_nested_dependency_default():
    exception, processed = await execute(nested_current_task_after_default())

    assert exception is None
    assert processed.result is True


@task
def current_task_with_positional_followers_task(
    first: int,
    current: Task = CURRENT_TASK,
    last: int = 0,
    *values: int,
) -> tuple[int, str, int, tuple[int, ...]]:
    return first, str(current.id), last, values


async def test_current_task_preserves_following_positional_and_variadic_arguments():
    exception, processed = await execute(current_task_with_positional_followers_task(1, 2, 3, 4))

    assert exception is None
    assert processed.result == (1, str(processed.id), 2, (3, 4))


@task(retry=1, retry_delay=0)
def retriable_invalid_scope_task(value: str = Depends(request_parent_resource)) -> str:
    return value


async def test_dependency_scope_error_does_not_schedule_retry():
    exception, processed = await execute(retriable_invalid_scope_task())

    assert isinstance(exception, DependencyScopeError)
    assert processed.status == "failed"
    assert processed.retry_count == 0
    assert processed.next_retry_at is None


@task
def function_scope_cleanup_failure_task(
    value: str = Depends(broken_cleanup_resource, scope="function"),
) -> str:
    return value


async def test_function_scope_cleanup_failure_marks_task_failed():
    exception, processed = await execute(function_scope_cleanup_failure_task())

    assert isinstance(exception, RuntimeError)
    assert str(exception) == "cleanup failed"
    assert processed.status == "failed"
