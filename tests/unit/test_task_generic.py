import pytest
from pydantic import BaseModel, ValidationError

from sheppy import Task, task
from sheppy.models import TaskSpec


class ResultModel(BaseModel):
    x: int


@task
def int_task() -> int:
    return 1


@task
def model_task() -> ResultModel:
    return ResultModel(x=1)


@task
def unannotated_task():
    return None


def make_spec(func_name: str) -> TaskSpec:
    return TaskSpec(func=f"tests.unit.test_task_generic:{func_name}")


class TestGenericTask:
    def test_bare_task_accepts_any_result(self):
        class Anything:
            pass

        result = Anything()

        task = Task(spec=make_spec("unannotated_task"), result=result)

        assert task.result is result

    def test_parametrized_task_accepts_matching_result(self):
        task = Task[int](spec=make_spec("int_task"), result=1)

        assert task.result == 1

    def test_parametrized_task_allows_none_result(self):
        task = Task[int](spec=make_spec("int_task"), result=None)

        assert task.result is None

    def test_parametrized_task_rejects_incompatible_result(self):
        with pytest.raises(ValidationError):
            Task[int](spec=make_spec("int_task"), result="nope")

    def test_parametrized_task_coerces_pydantic_model_result(self):
        task = Task[ResultModel](spec=make_spec("model_task"), result={"x": 1})

        assert isinstance(task.result, ResultModel)
        assert task.result.x == 1

    def test_parametrized_task_json_round_trip(self):
        task = Task[int](spec=make_spec("int_task"), result=42)

        task2 = Task[int].model_validate(task.model_dump(mode="json"))

        assert task2.result == 42

    def test_parametrized_model_validate_rejects_incompatible_result(self):
        data = Task(spec=make_spec("unannotated_task"), result="nope").model_dump(mode="json")

        with pytest.raises(ValidationError):
            Task[int].model_validate(data)
