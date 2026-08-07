import pytest

from sheppy import task, workflow
from sheppy._utils.functions import resolve_function


def sample_middleware(task, queue):  # noqa: ARG001
    task, exception = yield task  # noqa: ARG001
    return task


@task(middleware=[sample_middleware])
def marked_task_with_middleware(x: int) -> int:
    return x


@task
def marked_task(x: int) -> int:
    return x


@workflow
def marked_workflow():
    result = yield marked_task(1)
    return result


def unmarked_function():
    pass


def test_decorators_set_marker():
    assert getattr(marked_task, "__sheppy_task__", False) is True
    assert getattr(marked_workflow, "__sheppy_task__", False) is True
    assert getattr(unmarked_function, "__sheppy_task__", False) is False


def test_task_decorator_stores_middleware_on_wrapper():
    assert getattr(marked_task, "__sheppy_middleware__", None) == []
    assert getattr(marked_task_with_middleware, "__sheppy_middleware__", None) == [sample_middleware]
    # the undecorated function must not carry task-level middleware
    assert getattr(marked_task_with_middleware.__wrapped__, "__sheppy_middleware__", None) is None


def test_resolve_marked_task():
    func = resolve_function("tests.unit.test_resolve_function:marked_task")
    assert func is marked_task.__wrapped__


def test_resolve_marked_task_wrapped():
    func = resolve_function("tests.unit.test_resolve_function:marked_task", wrapped=False)
    assert func is marked_task


def test_resolve_marked_workflow():
    func = resolve_function("tests.unit.test_resolve_function:marked_workflow")
    assert func is marked_workflow.__wrapped__


def test_resolve_unmarked_function_refused():
    with pytest.raises(ValueError, match="Refusing to resolve function"):
        resolve_function("tests.unit.test_resolve_function:unmarked_function")


def test_resolve_stdlib_function_refused():
    with pytest.raises(ValueError, match="Refusing to resolve function"):
        resolve_function("os:system")


def test_unresolvable_function():
    with pytest.raises(ValueError, match="Cannot resolve function"):
        resolve_function("tests.unit.test_resolve_function:does_not_exist")


def test_dynamic_task_creation_marks_original():
    def dynamic_fn():
        pass

    task(dynamic_fn)
    assert getattr(dynamic_fn, "__sheppy_task__", False) is True
