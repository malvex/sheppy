from collections.abc import Callable, Generator
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from typing import Any, NamedTuple, ParamSpec, TypeVar, overload
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from ._utils.functions import resolve_function, stringify_function
from .models import Task

P = ParamSpec('P')
R = TypeVar('R')


@dataclass
class WorkflowContext:
    workflow_id: UUID
    step_counter: int = 0
    stored_tasks: dict[int, UUID] = field(default_factory=dict)
    task_order: list[str] = field(default_factory=list)

    def next_task_id(self) -> UUID:
        step_idx = self.step_counter
        self.step_counter += 1

        if step_idx in self.stored_tasks:
            return self.stored_tasks[step_idx]

        task_id = uuid4()
        self.task_order.append(str(task_id))

        return task_id


_workflow_context: ContextVar[WorkflowContext | None] = ContextVar('workflow_context', default=None)


def get_workflow_context() -> WorkflowContext | None:
    return _workflow_context.get()


def set_workflow_context(ctx: WorkflowContext | None) -> None:
    _workflow_context.set(ctx)


class Workflow(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    func: str
    args: tuple[Any, ...] = Field(default_factory=tuple)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    task_order: list[str] = Field(default_factory=list)
    processed_tasks: dict[str, Task] = Field(default_factory=dict)
    pending_task_ids: list[str] = Field(default_factory=list)
    completed: bool = False
    final_result: Any = None
    error: str | None = None
    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: AwareDatetime | None = None

    def get_stored_task_ids(self) -> dict[int, UUID]:
        return {idx: UUID(tid) for idx, tid in enumerate(self.task_order)}

    def get_incomplete_task_ids(self) -> list[UUID]:
        return [UUID(tid) for tid in self.pending_task_ids]

    def __repr__(self) -> str:
        return (
            f"Workflow(id={self.id!r}, func={self.func!r}, "
            f"completed={self.completed}, tasks={len(self.processed_tasks)})"
        )


class WorkflowResult(NamedTuple):
    workflow: Workflow
    pending_tasks: list[Task]


@overload
def workflow(
    func: Callable[P, Generator[Task | list[Task], Task | list[Task], R]], /
) -> Callable[P, Workflow]:
    ...


@overload
def workflow() -> Callable[
    [Callable[P, Generator[Task | list[Task], Task | list[Task], R]]],
    Callable[P, Workflow]
]:
    ...


def workflow(
    func: Callable[P, Generator[Task | list[Task], Task | list[Task], R]] | None = None,
) -> (
    Callable[P, Workflow] |
    Callable[
        [Callable[P, Generator[Task | list[Task], Task | list[Task], R]]],
        Callable[P, Workflow]
    ]
):
    def decorator(
        fn: Callable[P, Generator[Task | list[Task], Task | list[Task], R]]
    ) -> Callable[P, Workflow]:
        func_string = stringify_function(fn)

        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Workflow:
            return Workflow(
                func=func_string,
                args=args,
                kwargs=kwargs,
            )

        return wrapper

    if func is not None:
        return decorator(func)

    return decorator


class WorkflowRunner:
    def __init__(self, workflow: Workflow, task_results: dict[UUID, Task] | None = None):
        self.workflow = workflow
        self.task_results = task_results or {}

    def run(self) -> WorkflowResult:
        ctx = WorkflowContext(
            workflow_id=self.workflow.id,
            stored_tasks=self.workflow.get_stored_task_ids(),
            task_order=list(self.workflow.task_order),
        )
        _workflow_context.set(ctx)

        try:
            gen = resolve_function(self.workflow.func)(*self.workflow.args, **self.workflow.kwargs)

            try:
                result, pending = self._run_generator(gen)
            except StopIteration as e:
                result, pending = e.value, []

            processed_tasks = dict(self.workflow.processed_tasks)
            for task_id, task in self.task_results.items():
                if task.status in ['completed', 'failed']:
                    processed_tasks[str(task_id)] = task

            if pending:
                return WorkflowResult(
                    workflow=self._make_workflow(ctx, processed_tasks, pending_task_ids=[str(t.id) for t in pending]),
                    pending_tasks=pending,
                )

            return WorkflowResult(
                workflow=self._make_workflow(ctx, processed_tasks, completed=True, final_result=result),
                pending_tasks=[],
            )

        except Exception as e:
            return WorkflowResult(
                workflow=self._make_workflow(ctx, error=f"{type(e).__name__}: {e}"),
                pending_tasks=[],
            )
        finally:
            _workflow_context.set(None)

    def _run_generator(self, gen: Generator[Any, Any, Any]) -> tuple[Any, list[Task]]:
        result_to_send: Any = None

        while True:
            yielded = gen.send(result_to_send)
            result_to_send, pending = self._handle_yield(yielded)

            if pending:
                return None, pending

    def _handle_yield(self, yielded: Any) -> tuple[Any, list[Task]]:
        items, is_list = (yielded, True) if isinstance(yielded, list) else ([yielded], False)

        if items and isinstance(items[0], Workflow):
            results = []
            for wf in items:
                result, pending = self._run_nested_workflow(wf)
                if pending:
                    return None, pending
                results.append(result)
            return (results if is_list else results[0]), []

        results = []
        pending = []
        for task in items:
            result, is_pending = self._process_task(task)
            results.append(result)
            if is_pending:
                pending.append(task)

        if pending:
            return None, pending
        return (results if is_list else results[0]), []

    def _run_nested_workflow(self, wf: Workflow) -> tuple[Any, list[Task]]:
        gen = resolve_function(wf.func)(*wf.args, **wf.kwargs)
        try:
            return self._run_generator(gen)
        except StopIteration as e:
            return e.value, []

    def _process_task(self, task: Task) -> tuple[Task, bool]:
        task_id = str(task.id)
        cached = self.workflow.processed_tasks.get(task_id)

        if cached and (cached.status == 'completed' or cached.error):
            return cached, False

        if task.id in self.task_results:
            return self.task_results[task.id], False

        return task, True

    def _make_workflow(
        self,
        ctx: WorkflowContext,
        processed_tasks: dict[str, Task] | None = None,
        pending_task_ids: list[str] | None = None,
        completed: bool = False,
        final_result: Any = None,
        error: str | None = None,
    ) -> Workflow:
        data = self.workflow.model_dump()
        data['task_order'] = ctx.task_order
        data['pending_task_ids'] = pending_task_ids or []
        data['completed'] = completed

        if processed_tasks is not None:
            data['processed_tasks'] = {k: v.model_dump() for k, v in processed_tasks.items()}
        if final_result is not None:
            data['final_result'] = final_result
        if error is not None:
            data['error'] = error
        if completed or error:
            data['finished_at'] = datetime.now(timezone.utc)

        return Workflow.model_validate(data)
