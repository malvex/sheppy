from collections.abc import Generator
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from sheppy import CURRENT_TASK, Depends, Task, task, workflow
from sheppy._workflow import Workflow, WorkflowRunner
from sheppy.models import TaskConfig, TaskSpec


@task
def add(x: int, y: int) -> int:
    return x + y


@task(retry=2, retry_delay=0)
def flaky() -> str:
    return "ok"


@workflow
def sequential_workflow(x: int):
    a = yield add(x, 1)
    b = yield add(a.result, 10)
    return b.result


@workflow
def one_step_workflow():
    t = yield flaky()
    return t.result


@workflow
def inner_workflow(x: int):
    t = yield add(x, 1)
    return t.result * 10


@workflow
def outer_workflow(x: int):
    r = yield inner_workflow(x)
    t = yield add(r, 100)
    return t.result


@workflow
def crashing_workflow():
    a = yield add(5, 5)
    raise RuntimeError(f"boom after {a.result}")


@workflow
def invalid_yield_workflow():
    yield 42


@workflow
def mixed_yield_workflow():
    yield [inner_workflow(1), add(1, 2)]


@workflow
def tuple_yield_workflow():
    results = yield add(1, 1), add(2, 2)
    return results


@workflow
def returns_task_workflow() -> Generator[Any, Any, Task]:
    t = yield add(1, 2)
    return t


class MyModel(BaseModel):
    name: str
    x: int


@workflow
def annotated_return_workflow() -> Generator[Any, Any, MyModel]:
    yield add(1, 1)
    return MyModel(name="wf", x=1)


@workflow
def unannotated_return_workflow():
    yield add(1, 1)
    return MyModel(name="wf", x=1)


def _completed(task: Task, result: object) -> Task:
    return task.model_copy(update={"status": "completed", "result": result})


# --- @workflow decoration ---

def test_workflow_rejects_non_generator_function():
    with pytest.raises(TypeError, match="generator"):

        @workflow
        def wf():  # pragma: no cover
            return 5


def test_workflow_rejects_async_generator_function():
    with pytest.raises(TypeError, match="async generator"):

        @workflow
        async def wf():  # pragma: no cover
            yield


def test_workflow_rejects_current_task_injection():
    with pytest.raises(TypeError, match="CURRENT_TASK"):

        @workflow
        def wf(_current: Task = CURRENT_TASK):  # pragma: no cover
            yield add(1, 1)


def test_workflow_rejects_dependency_injection():
    def dep() -> int:
        return 1

    with pytest.raises(TypeError, match="dependency injection"):

        @workflow
        def wf(x: int = Depends(dep)):  # pragma: no cover
            yield add(x, 1)


def test_workflow_decorator_with_parentheses():
    @workflow()
    def wf():
        yield add(1, 1)

    assert isinstance(wf(), Workflow)


# --- call-time argument validation (same behavior as @task) ---

def test_workflow_validates_missing_args_at_call_time():
    with pytest.raises(ValidationError):
        sequential_workflow()


def test_workflow_validates_unexpected_args_at_call_time():
    with pytest.raises(ValidationError):
        sequential_workflow(1, unknown=2)


def test_workflow_coerces_args_like_tasks():
    wf = sequential_workflow("5")
    assert wf.args == (5,)


# --- Task.is_terminal ---

@pytest.mark.parametrize(("status", "error", "retry", "retry_count", "expected"), [
    ("completed", None, 0, 0, True),
    ("failed", "Exception: x", 0, 0, True),      # permanent failure
    ("failed", "Exception: x", 2, 2, True),      # retries exhausted
    ("crashed", "Exception: x", 0, 0, True),
    ("retrying", "Exception: x", 2, 1, False),   # retry still scheduled
    ("failed", "Exception: x", 2, 0, False),     # retry still possible
    ("pending", None, 0, 0, False),
    ("processing", None, 0, 0, False),
    ("new", None, 0, 0, False),
])
def test_task_is_terminal(status: str, error: str | None, retry: int, retry_count: int, expected: bool):
    t = Task(
        status=status,  # type: ignore[arg-type]
        error=error,
        retry_count=retry_count,
        spec=TaskSpec(func="tests.unit.test_workflow:add"),
        config=TaskConfig(retry=retry),
    )
    assert t.is_terminal is expected


# --- WorkflowRunner ---

def test_runner_first_run_returns_pending_task():
    wf = sequential_workflow(1)

    result = WorkflowRunner(wf).run()

    assert not result.workflow.completed
    assert result.workflow.error is None
    assert len(result.pending_tasks) == 1

    t1 = result.pending_tasks[0]
    assert t1.spec.func == "tests.unit.test_workflow:add"
    assert t1.spec.args == (1, 1)
    assert t1.workflow_id == wf.id
    assert result.workflow.task_order == [str(t1.id)]


def test_runner_replay_advances_and_completes():
    wf = sequential_workflow(1)
    r1 = WorkflowRunner(wf).run()
    t1 = r1.pending_tasks[0]
    done1 = _completed(t1, 2)

    r2 = WorkflowRunner(r1.workflow, {t1.id: done1}).run()

    assert not r2.workflow.completed
    assert len(r2.pending_tasks) == 1
    t2 = r2.pending_tasks[0]
    assert t2.spec.args == (2, 10)  # built from the first task's result
    assert r2.workflow.task_order == [str(t1.id), str(t2.id)]

    r3 = WorkflowRunner(r2.workflow, {t1.id: done1, t2.id: _completed(t2, 12)}).run()

    assert r3.workflow.completed
    assert r3.workflow.error is None
    assert r3.workflow.final_result == 12
    assert r3.workflow.finished_at is not None
    assert r3.pending_tasks == []


def test_runner_assigns_deterministic_step_ids():
    # any replay computes the same task ID for the same step,
    # so concurrent resumes cannot enqueue the same step twice
    wf = sequential_workflow(1)
    r1 = WorkflowRunner(wf).run()
    t1 = r1.pending_tasks[0]
    done1 = _completed(t1, 2)

    r2a = WorkflowRunner(r1.workflow, {t1.id: done1}).run()
    r2b = WorkflowRunner(r1.workflow, {t1.id: done1}).run()

    assert r2a.pending_tasks[0].id == r2b.pending_tasks[0].id


def test_runner_non_terminal_task_keeps_workflow_waiting():
    # a failed task that still has retries left must NOT be treated as done
    wf = one_step_workflow()
    r1 = WorkflowRunner(wf).run()
    t1 = r1.pending_tasks[0]

    retrying = t1.model_copy(update={"status": "retrying", "error": "Exception: x", "retry_count": 1})
    r2 = WorkflowRunner(r1.workflow, {t1.id: retrying}).run()

    assert not r2.workflow.completed
    assert [t.id for t in r2.pending_tasks] == [t1.id]


def test_runner_sends_finished_task_back_into_generator():
    wf = one_step_workflow()
    r1 = WorkflowRunner(wf).run()
    t1 = r1.pending_tasks[0]

    r2 = WorkflowRunner(r1.workflow, {t1.id: _completed(t1, "ok")}).run()

    assert r2.workflow.completed
    assert r2.workflow.final_result == "ok"


def test_runner_nested_workflow():
    wf = outer_workflow(5)

    r1 = WorkflowRunner(wf).run()
    t_inner = r1.pending_tasks[0]
    assert t_inner.spec.args == (5, 1)
    done_inner = _completed(t_inner, 6)

    r2 = WorkflowRunner(r1.workflow, {t_inner.id: done_inner}).run()
    t_outer = r2.pending_tasks[0]
    assert t_outer.spec.args == (60, 100)  # nested return value was sent back

    r3 = WorkflowRunner(r2.workflow, {t_inner.id: done_inner, t_outer.id: _completed(t_outer, 160)}).run()
    assert r3.workflow.completed
    assert r3.workflow.final_result == 160


def test_runner_error_in_workflow_body():
    wf = crashing_workflow()
    r1 = WorkflowRunner(wf).run()
    t1 = r1.pending_tasks[0]

    r2 = WorkflowRunner(r1.workflow, {t1.id: _completed(t1, 10)}).run()

    assert r2.workflow.error == "RuntimeError: boom after 10"
    assert not r2.workflow.completed
    assert r2.workflow.finished_at is not None


def test_runner_invalid_yield_fails_with_clear_error():
    result = WorkflowRunner(invalid_yield_workflow()).run()

    assert result.workflow.error is not None
    assert "Invalid yield" in result.workflow.error
    assert "int" in result.workflow.error


def test_runner_mixed_yield_fails_with_clear_error():
    result = WorkflowRunner(mixed_yield_workflow()).run()

    assert result.workflow.error is not None
    assert "Invalid yield" in result.workflow.error
    assert "homogeneous" in result.workflow.error


def test_runner_tuple_yield_behaves_like_list_yield():
    r1 = WorkflowRunner(tuple_yield_workflow()).run()

    assert len(r1.pending_tasks) == 2
    t1, t2 = r1.pending_tasks

    r2 = WorkflowRunner(r1.workflow, {t1.id: _completed(t1, 2), t2.id: _completed(t2, 4)}).run()

    assert r2.workflow.completed
    sent_back = r2.workflow.final_result
    assert isinstance(sent_back, tuple)  # mirrors the yielded type
    assert [t.result for t in sent_back] == [2, 4]


def test_final_result_tasks_survive_json_roundtrip():
    r1 = WorkflowRunner(returns_task_workflow()).run()
    t1 = r1.pending_tasks[0]

    r2 = WorkflowRunner(r1.workflow, {t1.id: _completed(t1, 3)}).run()
    assert r2.workflow.completed
    assert isinstance(r2.workflow.final_result, Task)

    # simulate what the backend stores and what get_workflow() returns
    stored = r2.workflow.model_dump(mode="json")
    reloaded = Workflow.model_validate(stored)

    assert isinstance(reloaded.final_result, Task)
    assert reloaded.final_result.id == t1.id
    assert reloaded.final_result.status == "completed"
    assert reloaded.final_result.result == 3


def test_final_result_plain_data_is_untouched_by_task_revival():
    wf = Workflow(
        func="tests.unit.test_workflow:sequential_workflow",
        final_result={"__sheppy_task__": "not a task", "nested": [{"__sheppy_task__": {"bogus": 1}}]},
    )

    assert wf.final_result == {"__sheppy_task__": "not a task", "nested": [{"__sheppy_task__": {"bogus": 1}}]}


def test_final_result_reconstructed_from_generator_return_hint():
    r1 = WorkflowRunner(annotated_return_workflow()).run()
    t1 = r1.pending_tasks[0]

    r2 = WorkflowRunner(r1.workflow, {t1.id: _completed(t1, 2)}).run()

    assert r2.workflow.completed
    assert isinstance(r2.workflow.final_result, MyModel)

    # and it stays reconstructed after a backend JSON round-trip
    reloaded = Workflow.model_validate(r2.workflow.model_dump(mode="json"))
    assert isinstance(reloaded.final_result, MyModel)
    assert reloaded.final_result == MyModel(name="wf", x=1)


def test_final_result_without_hint_stays_raw():
    r1 = WorkflowRunner(unannotated_return_workflow()).run()
    t1 = r1.pending_tasks[0]

    r2 = WorkflowRunner(r1.workflow, {t1.id: _completed(t1, 2)}).run()

    assert r2.workflow.completed
    reloaded = Workflow.model_validate(r2.workflow.model_dump(mode="json"))
    assert reloaded.final_result == {"name": "wf", "x": 1}
