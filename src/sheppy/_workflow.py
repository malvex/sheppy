import inspect
from collections.abc import Callable, Generator
from contextlib import closing
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from typing import Any, NamedTuple, ParamSpec, TypeVar, overload
from uuid import UUID, uuid4, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from ._utils.functions import (
    reconstruct_workflow_result,
    resolve_function,
    stringify_function,
)
from ._utils.validation import _is_depends_parameter, validate_input
from .exceptions import WorkflowError
from .models import CURRENT_TASK, Task

P = ParamSpec('P')
R = TypeVar('R')

WORKFLOW_TASK_NS = UUID('7005b432-c135-4131-b19e-d3dc89703a9b')


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

        task_id = uuid5(WORKFLOW_TASK_NS, f"{self.workflow_id}:{step_idx}")
        self.task_order.append(str(task_id))

        return task_id


_workflow_context: ContextVar[WorkflowContext | None] = ContextVar('workflow_context', default=None)


def get_workflow_context() -> WorkflowContext | None:
    return _workflow_context.get()


class Workflow(BaseModel):
    """A workflow instance created when a workflow function is called.

    A workflow is a durable generator: each `yield` of a `Task` (or a list of
    tasks for fan-out) is an await point. Yielded tasks are queued, and once
    they reach a terminal state the generator is replayed from the start with
    finished tasks fed back in, until it either returns (workflow completed)
    or raises (workflow failed).

    Attributes:
        id: Unique identifier for the workflow.
        func: Fully qualified function name, e.g. `my_module:my_workflow`.
        args: Positional arguments passed to the workflow function.
        kwargs: Keyword arguments passed to the workflow function.
        task_order: Deterministic task IDs in creation order (one per replay step).
        completed: True when the workflow function has returned.
        final_result: Return value of the workflow function (when completed).
        error: Error message if the workflow failed. None otherwise.
        created_at: Timestamp when the workflow was created.
        finished_at: Timestamp when the workflow completed or failed. None while running.

    Note:
        - You should not create Workflow instances directly. Instead, use the `@workflow` decorator
          to define a workflow function, and then call that function to create a Workflow instance.
        - `args` and `kwargs` must be JSON serializable.
        - The workflow function must be deterministic: the sequence of tasks it creates may only
          depend on the results of previously yielded tasks, never on external state or randomness.

    Example:
        ```python
        from sheppy import task, workflow

        @task
        async def add(x: int, y: int) -> int:
            return x + y

        @workflow
        def my_workflow(x: int):
            a = yield add(x, 1)          # a is the finished Task, a.result == x + 1
            b = yield add(a.result, 10)  # sequential step
            return b.result

        wf = my_workflow(5)  # returns a Workflow instance, it is NOT executed yet
        ```
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    """UUID: Unique identifier for the workflow."""
    func: str
    """str: Fully qualified function name, e.g. `my_module:my_workflow`"""
    args: tuple[Any, ...] = Field(default_factory=tuple)
    """tuple[Any, ...]: Positional arguments passed to the workflow function."""
    kwargs: dict[str, Any] = Field(default_factory=dict)
    """dict[str, Any]: Keyword arguments passed to the workflow function."""
    task_order: list[str] = Field(default_factory=list)
    """list[str]: Deterministic task IDs in creation order (one per replay step)."""
    completed: bool = False
    """bool: True when the workflow function has returned."""
    final_result: Any = None
    """Any: Return value of the workflow function (when completed)."""
    error: str | None = None
    """str|None: Error message if the workflow failed. None otherwise."""
    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    """datetime: Timestamp when the workflow was created."""
    finished_at: AwareDatetime | None = None
    """datetime|None: Timestamp when the workflow completed or failed. None while running."""

    def get_stored_task_ids(self) -> dict[int, UUID]:
        """Map of replay step index to deterministic task ID, from previous runs."""
        return {idx: UUID(tid) for idx, tid in enumerate(self.task_order)}

    @model_validator(mode='after')
    def _reconstruct_final_result(self) -> 'Workflow':
        if self.final_result is not None:
            self.__dict__["final_result"] = reconstruct_workflow_result(self.func, self.final_result)

        return self

    def __repr__(self) -> str:
        return f"Workflow(id={self.id!r}, func={self.func!r}, completed={self.completed}, steps={len(self.task_order)})"


class WorkflowResult(NamedTuple):
    """Result of driving a workflow (via `Queue.add_workflow()` or `Queue.resume_workflow()`).

    Attributes:
        workflow: The updated workflow state.
        pending_tasks: Tasks the workflow is currently waiting on (empty when finished).
    """
    workflow: Workflow
    pending_tasks: list[Task]


# what a workflow generator may yield at each await point
WorkflowYield = Task | Workflow | list[Task] | list[Workflow] | tuple[Task, ...] | tuple[Workflow, ...]


def _validate_workflow_function(fn: Callable[..., Any]) -> None:
    if inspect.isasyncgenfunction(fn):
        raise TypeError("@workflow does not support async generator functions, use a regular generator function")

    if not inspect.isgeneratorfunction(fn):
        raise TypeError("@workflow can only decorate generator functions (a function that yields tasks)")

    for param_name, param in inspect.signature(fn).parameters.items():
        if param.default is CURRENT_TASK:
            raise TypeError(f"@workflow function cannot use CURRENT_TASK injection (parameter '{param_name}')")
        if _is_depends_parameter(param):
            raise TypeError(f"@workflow function cannot use dependency injection (parameter '{param_name}')")


@overload
def workflow(
    func: Callable[P, Generator[WorkflowYield, Any, R]], /
) -> Callable[P, Workflow]:
    ...


@overload
def workflow() -> Callable[
    [Callable[P, Generator[WorkflowYield, Any, R]]],
    Callable[P, Workflow]
]:
    ...


def workflow(
    func: Callable[P, Generator[WorkflowYield, Any, R]] | None = None,
) -> (
    Callable[P, Workflow] |
    Callable[
        [Callable[P, Generator[WorkflowYield, Any, R]]],
        Callable[P, Workflow]
    ]
):
    """Decorator to define a workflow from a generator function.

    Each `yield` of a `Task` (or a list of tasks for fan-out) is an await
    point: the tasks are queued, and once they finish the generator is
    replayed with finished tasks fed back in. Yielding a `Workflow` runs it
    inline as a sub-workflow and sends back its return value.

    The decorated function must be a plain (non-async) generator function,
    and it must be deterministic across replays. Arguments are validated
    against the function signature at call time exactly like `@task`.
    """
    def decorator(
        fn: Callable[P, Generator[WorkflowYield, Any, R]]
    ) -> Callable[P, Workflow]:
        _validate_workflow_function(fn)
        func_string = stringify_function(fn)

        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Workflow:
            validated_args, validated_kwargs = validate_input(fn, tuple(args), dict(kwargs))
            return Workflow(
                func=func_string,
                args=validated_args,
                kwargs=validated_kwargs,
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
        token = _workflow_context.set(ctx)

        try:
            gen = resolve_function(self.workflow.func)(*self.workflow.args, **self.workflow.kwargs)

            with closing(gen):
                try:
                    result, pending = self._run_generator(gen)
                except StopIteration as e:
                    result, pending = e.value, []

            if pending:
                return WorkflowResult(
                    workflow=self._make_workflow(ctx),
                    pending_tasks=pending,
                )

            return WorkflowResult(
                workflow=self._make_workflow(
                    ctx,
                    completed=True,
                    final_result=reconstruct_workflow_result(self.workflow.func, result),
                ),
                pending_tasks=[],
            )

        except Exception as e:
            return WorkflowResult(
                workflow=self._make_workflow(ctx, error=f"{type(e).__name__}: {e}"),
                pending_tasks=[],
            )
        finally:
            _workflow_context.reset(token)

    def _run_generator(self, gen: Generator[Any, Any, Any]) -> tuple[Any, list[Task]]:
        result_to_send: Any = None

        while True:
            yielded = gen.send(result_to_send)
            result_to_send, pending = self._handle_yield(yielded)

            if pending:
                return None, pending

    def _handle_yield(self, yielded: Any) -> tuple[Any, list[Task]]:
        if isinstance(yielded, (list, tuple)):
            items = list(yielded)
            wrap: type[list[Any] | tuple[Any, ...]] | None = type(yielded)
        else:
            items = [yielded]
            wrap = None

        if all(isinstance(item, Workflow) for item in items):
            results = []
            for wf in items:
                result, pending = self._run_nested_workflow(wf)
                if pending:
                    return None, pending
                results.append(result)
            return (wrap(results) if wrap else results[0]), []

        if all(isinstance(item, Task) for item in items):
            results = []
            pending = []
            for task in items:
                result, is_pending = self._process_task(task)
                results.append(result)
                if is_pending:
                    pending.append(task)

            if pending:
                return None, pending
            return (wrap(results) if wrap else results[0]), []

        raise WorkflowError(
            "Invalid yield in workflow: expected a Task, a Workflow, or a homogeneous "
            f"list or tuple of one of those, got {self._describe_yield(yielded)}"
        )

    @staticmethod
    def _describe_yield(yielded: Any) -> str:
        if isinstance(yielded, (list, tuple)):
            types = ", ".join(sorted({type(item).__name__ for item in yielded}))
            return f"{type(yielded).__name__} containing: {types or 'nothing (empty)'}"
        return f"{type(yielded).__name__} ({yielded!r})"

    def _run_nested_workflow(self, wf: Workflow) -> tuple[Any, list[Task]]:
        gen = resolve_function(wf.func)(*wf.args, **wf.kwargs)
        with closing(gen):
            try:
                return self._run_generator(gen)
            except StopIteration as e:
                return e.value, []

    def _process_task(self, task: Task) -> tuple[Task, bool]:
        cached = self.task_results.get(task.id)

        if cached is not None and cached.is_terminal:
            return cached, False

        return task, True

    def _make_workflow(
        self,
        ctx: WorkflowContext,
        *,
        completed: bool = False,
        final_result: Any = None,
        error: str | None = None,
    ) -> Workflow:
        data = self.workflow.model_dump()
        data['task_order'] = ctx.task_order

        if completed:
            data['completed'] = True
            data['final_result'] = final_result
        if error is not None:
            data['error'] = error
        if completed or error:
            data['finished_at'] = datetime.now(timezone.utc)

        return Workflow.model_validate(data)
