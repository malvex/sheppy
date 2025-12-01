from uuid import UUID

import pytest
from pydantic import ValidationError

from sheppy import Task, task
from sheppy.models import TaskConfig
from tests.dependencies import (
    Status,
    failing_task,
    simple_async_task,
    simple_async_task_no_param,
    simple_sync_task,
    simple_sync_task_no_param,
)


@pytest.fixture(params=["async_task", "sync_task"])
def task_fn(request):
    if request.param == "async_task":
        return simple_async_task

    if request.param == "sync_task":
        return simple_sync_task

    raise NotImplementedError


@task(retry=5)
def task_with_retry_only():
    pass


@task(retry=5, retry_delay=1)
def task_with_retry():
    pass


@task(retry=5, retry_delay=[1, 2, 3])
def task_with_retry_and_exponential_delay():
    pass


@task
def task_reconstruct() -> Status:
    return Status(code=200, message="OK")


@task
def task_reconstruct_no_annotation():
    return Status(code=200, message="OK")


@task
def async_task_reconstruct_no_annotation():
    return Status(code=200, message="OK")


@task
def task_reconstruct_from_annotation() -> Status:
    return {"code": 200, "message": "OK"}


def test_instanced_task_defaults(task_fn):
    task = task_fn(1, 2)

    # new tasks should have the following values
    assert isinstance(task.id, UUID)
    assert task.completed is False
    assert task.error is None
    assert task.result is None
    assert task.created_at is not None
    assert task.finished_at is None
    assert task.scheduled_at is None
    assert task.retry_count == 0
    assert task.last_retry_at is None
    assert task.next_retry_at is None

    if task_fn == simple_async_task:
        fn_name = "tests.dependencies:simple_async_task"
    else:
        fn_name = "tests.dependencies:simple_sync_task"

    assert task.spec.func == fn_name
    assert task.spec.args == (1, 2)
    assert task.spec.kwargs == {}
    assert task.spec.return_type == "builtins.int"
    assert task.spec.middleware == []

    assert task.config.retry == 0
    assert task.config.retry_delay == 1.0


def test_task_is_frozen(task_fn):
    task = task_fn(1, 2)

    with pytest.raises(ValidationError, match="Instance is frozen"):
        task.completed = True

    with pytest.raises(ValidationError, match="Instance is frozen"):
        task.error = "hello"

    with pytest.raises(ValidationError, match="Instance is frozen"):
        task.spec.func = "hello"

    with pytest.raises(ValidationError, match="Instance is frozen"):
        task.spec.args = (10, 20)

    with pytest.raises(ValidationError, match="Instance is frozen"):
        task.config.retry = 5.5

    with pytest.raises(ValidationError, match="Instance is frozen"):
        task.config = TaskConfig()

    with pytest.raises(TypeError, match="does not support item assignment"):
        task.spec.args[0] = 5


def test_unique_id(task_fn):
    task1 = task_fn(1, 2)
    task2 = task_fn(1, 2)

    assert task1 != task2
    assert task1.id != task2.id
    assert task1.spec == task2.spec
    assert task1.config == task2.config


@pytest.mark.parametrize("task,is_retriable", [
    (simple_async_task(1, 2), False),
    (simple_sync_task(1, 2), False),
    (task_with_retry_only(), True),
    (task_with_retry(), True),
    (task_with_retry_and_exponential_delay(), True),
])
def test_computed_attributes(task: Task, is_retriable: bool):
    assert task.config.retry == (5 if is_retriable else 0)
    assert task.is_retriable == is_retriable
    assert task.should_retry == is_retriable

    task.__dict__["retry_count"] = 2
    assert task.should_retry == is_retriable

    task.__dict__["retry_count"] = 10
    assert task.should_retry is False


@pytest.mark.parametrize("arg", [
    pytest.param(Status(code=200, message="OK"), id="pydantic-model"),
    pytest.param({"code": 200, "message": "OK"}, id="dict"),
    pytest.param({"code": "200", "message": "OK"}, id="dict-string-to-int"),
])
def test_input_pydantic_conversion(arg):
    @task
    def convert_input(status: Status):
        pass

    t = convert_input(arg)
    assert isinstance(t.spec.args[0], Status)
    assert t.spec.args[0] == Status(code=200, message="OK")


@pytest.mark.parametrize("task_fn,expected", [
    (task_reconstruct, "tests.dependencies.Status"),
    (task_reconstruct_no_annotation, None),
    (task_reconstruct_from_annotation, "tests.dependencies.Status"),
    (simple_async_task_no_param, "builtins.int"),
    (simple_sync_task_no_param, "builtins.int"),
    (failing_task, "builtins.NoneType"),  # ! FIXME?: this should probably just be `None`
    (task_with_retry_only, None),
])
def test_task_return_type(task_fn, expected):
    assert task_fn().spec.return_type == expected


@pytest.mark.parametrize("task_fn", [
    pytest.param(task_reconstruct, id="pydantic-model"),
    pytest.param(task_reconstruct_from_annotation, id="dict-string-to-int"),
])
def test_reconstruct_result(task_fn):
    task_orig = task_fn()
    task_orig_dict = task_orig.model_dump(mode="json")

    assert task_orig.spec.return_type == "tests.dependencies.Status"

    task = Task.model_validate(task_orig_dict | {"result": {"code": 200, "message": "OK"}})

    assert isinstance(task.result, Status)
    assert task.result == Status(code=200, message="OK")


@pytest.mark.parametrize("task_fn", [
    pytest.param(task_reconstruct_no_annotation, id="dict"),
    pytest.param(async_task_reconstruct_no_annotation, id="dict_async"),
])
def test_reconstruct_result_no_annotation(task_fn):
    task_orig = task_fn()
    task_orig_dict = task_orig.model_dump(mode="json")

    assert task_orig.spec.return_type is None

    task = Task.model_validate(task_orig_dict | {"result": {"code": 200, "message": "OK"}})

    # no automatic model conversion if there is no annotation provided!
    assert isinstance(task.result, dict)
    assert task.result == {"code": 200, "message": "OK"}


def test_immediate_validation(task_fn):
    """Test validation immediately on Task instantiation"""
    task_fn(1, 2)
    task_fn("1", 2)  # converts str to int
    task_fn("1", y=2)  # kwargs also fine
    task_fn(x=1, y=2)  # kwargs also fine

    with pytest.raises(ValidationError, match="unable to parse string as an integer"):
        task_fn("a", 2)  # bad input type, fail
    with pytest.raises(ValidationError, match="No value provided for parameter 'y'"):
        task_fn(1)  # missing arguments, fail
    with pytest.raises(ValidationError, match="Too many positional arguments"):
        task_fn(1, 2, 3)  # too many arugments, fail
    with pytest.raises(ValidationError, match="No value provided for parameter 'y'"):
        task_fn(2, x=1)


def test_retry_delay_cannot_be_empty_list():
    @task(retry=1, retry_delay=[])
    def task_empty_retry_delay():
        pass

    with pytest.raises(ValidationError, match="retry_delay list cannot be empty"):
        task_empty_retry_delay()
