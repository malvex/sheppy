import builtins
import importlib
import json
from datetime import datetime, timezone
from traceback import format_exception
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    ParamSpec,
)
from uuid import UUID, uuid4, uuid5

from croniter import croniter
from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from typing_extensions import NotRequired, TypedDict, TypeVar

from ._utils.functions import reconstruct_result
from .exceptions import TaskFailedError

P = ParamSpec('P')
R = TypeVar('R', default=Any)


TASK_CRON_NS = UUID('7005b432-c135-4131-b19e-d3dc89703a9a')

# sentinel object for current task injection (def my_task(x: int, task: Task = CURRENT_TASK): ...)
CURRENT_TASK = object()


def cron_expression_validator(value: str) -> str:
    if not croniter.is_valid(value):
        raise ValueError(f"{value} is not a valid cron expression")

    return value

CronExpression = Annotated[str, AfterValidator(cron_expression_validator)]
TaskStatus = Literal['new', 'scheduled', 'pending', 'processing', 'retrying',
                     'completed', 'failed', 'crashed', 'cancelled', 'unknown']

TTLValue = Annotated[int, Field(gt=0)] | None | Literal["inherit"]

RateLimitStrategy = Literal["sliding_window", "fixed_window"]


class RateLimit(TypedDict):
    max_rate: int
    rate_period: float  # time window for the rate limit in seconds
    key: NotRequired[str]  # defaults to task name, can be set to group rate limits across tasks
    strategy: NotRequired[RateLimitStrategy]  # defaults to "sliding_window"


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return repr(value)  # repr acceptable here as it's only used for TaskException
    return value


class TaskException(BaseModel):
    """Serializable representation of an exception raised during task execution.

    Captures everything needed to inspect a task failure in a JSON-serializable form, so it can be stored in any backend and reconstructed later.

    Attributes:
        type: Exception class name, e.g. `ValueError`.
        module: Module where the exception class is defined, e.g. `builtins`.
        message: Exception message, i.e. `str(exception)`.
        args: Exception args, sanitized to be JSON serializable (non-serializable values are replaced with their repr).
        traceback: Full formatted traceback of the exception. None for synthesized exceptions (e.g. worker crash).

    Example:
        ```python
        from sheppy import Queue, task

        @task
        def failing_task():
            raise ValueError("something went wrong")

        q = Queue(...)
        t = await q.add(failing_task())
        t = await q.wait_for(t)

        assert t.exception is not None
        print(t.exception.type)       # "ValueError"
        print(t.exception.message)    # "something went wrong"
        print(t.exception.traceback)  # full formatted traceback

        # best-effort reconstruction of the original exception
        exc = t.exception.to_exception()
        assert isinstance(exc, ValueError)
        ```
    """
    model_config = ConfigDict(frozen=True)

    type: str
    """str: Exception class name, e.g. `ValueError`."""
    module: str
    """str: Module where the exception class is defined, e.g. `builtins`."""
    message: str
    """str: Exception message, i.e. `str(exception)`."""
    args: tuple[Any, ...] = Field(default_factory=tuple)
    """tuple[Any, ...]: Exception args, sanitized to be JSON serializable."""
    traceback: str | None = None
    """str|None: Full formatted traceback. None for synthesized exceptions (e.g. worker crash)."""

    @classmethod
    def from_exception(cls, exception: BaseException) -> 'TaskException':
        return cls(
            type=type(exception).__qualname__,
            module=type(exception).__module__,
            message=str(exception),
            args=tuple(_json_safe(arg) for arg in exception.args),
            traceback="".join(format_exception(exception)),
        )

    def to_exception(self) -> Exception:
        exc_class = self._resolve_exception_class()
        if exc_class is not None:
            for args in (tuple(self.args), (self.message,)):
                try:
                    return exc_class(*args)
                except Exception:
                    continue

        # fallback if original exception cannot be reconstructed
        return TaskFailedError(f"{self.module}:{self.type}", self.message, self.traceback)

    def _resolve_exception_class(self) -> builtins.type[Exception] | None:
        try:
            obj: Any = importlib.import_module(self.module)
            for attr in self.type.split('.'):
                obj = getattr(obj, attr)
        except (ImportError, AttributeError):
            return None

        if isinstance(obj, type) and issubclass(obj, Exception):
            return obj
        return None

    def __str__(self) -> str:
        return f"{self.type}: {self.message}"

    def __repr__(self) -> str:
        return f"TaskException(type={self.type!r}, message={self.message!r})"


class TaskSpec(BaseModel):
    """Task specification.

    Attributes:
        func: Fully qualified function name, e.g. `my_module.my_submodule:my_function`
        args: Positional arguments to be passed to the function.
        kwargs: Keyword arguments to be passed to the function.

    Note:
        - You should not create TaskSpec instances directly. Instead, use the `@task` decorator to define a task function, and then call that function to create a Task instance.
        - `args` and `kwargs` must be JSON serializable.
        - `Task` is generic over the result type: `Task[int]` means `result` is `int | None`.

    Example:
        ```python
        from sheppy import task

        @task
        def my_task(x: int, y: str) -> str:
            return f"Received {x} and {y}"

        t = my_task(42, "hello")  # returns a Task instance, it is NOT executed yet

        print(t.spec.func)  # e.g. "my_module:my_task"
        print(t.spec.args)  # (42, "hello")
        ```
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    func: str
    """str: Fully qualified function name, e.g. `my_module.my_submodule:my_function`"""
    args: tuple[Any, ...] = Field(default_factory=tuple)
    """tuple[Any, ...]: Positional arguments to be passed to the function."""
    kwargs: dict[str, Any] = Field(default_factory=dict)
    """dict[str, Any]: Keyword arguments to be passed to the function."""

    @model_validator(mode="before")
    @classmethod
    def ignore_legacy_middleware(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data.pop("middleware", None)
        return data

class TaskConfig(BaseModel):
    """Task configuration.

    Attributes:
        retry: Number of times to retry the task if it fails. Default is 0 (no retries).
        retry_delay: Delay between retries in seconds. A single float is used for every
            attempt; a list sets the delay per attempt, and the last value is reused
            when attempts outnumber list entries. Default is 1.0 seconds.
        timeout: Maximum execution time in seconds. Default is None (no timeout).
        retry_on_timeout: If True, a task that times out is retried. Default is False.
        retry_on_crash: If True, a task whose worker crashed is retried
            (RedisBackend only). Default is False.
        rate_limit: Optional RateLimit dict limiting how often the task may run.
        ttl: TTL for the task in seconds, applied once the task finishes. None disables
            expiry; "inherit" (default) falls back to the backend's `ttl` setting.
        error_ttl: TTL for the task in seconds applied when the task fails. None disables
            expiry; "inherit" (default) falls back to `ttl`.

    Note:
        - You should not create TaskConfig instances directly. Instead, use the `@task` decorator to define a task function, and then call that function to create a Task instance.
        - `retry` must be a non-negative integer.
        - `retry_delay` list must not be empty.

    Example:
        ```python
        from sheppy import task

        @task(retry=3, retry_delay=[1, 2, 3])
        def my_task():
            raise Exception("Something went wrong")

        t = my_task()
        print(t.config.retry)  # 3
        print(t.config.retry_delay)  # [1.0, 2.0, 3.0]
        ```
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    retry: int = Field(default=0, ge=0)
    """int: Number of times to retry the task if it fails. Default is 0 (no retries)."""
    retry_delay: float | list[float] = Field(default=1.0)
    """float|list[float]: Delay between retries in seconds. A single float is used for every attempt; a list sets the delay per attempt, and the last value is reused when attempts outnumber list entries. Default is 1.0 seconds."""

    timeout: float | None = None  # seconds
    retry_on_timeout: bool = False
    retry_on_crash: bool = False
    rate_limit: RateLimit | None = None

    ttl: TTLValue = "inherit"
    """int|None: TTL for the task in seconds, applied once the task finishes. None disables expiry; "inherit" (default) falls back to the backend's `ttl` setting."""
    error_ttl: TTLValue = "inherit"
    """int|None: TTL for the task in seconds applied when the task fails. None disables expiry; "inherit" (default) falls back to `ttl`."""

    @field_validator('retry_delay')
    @classmethod
    def validate_retry_delay(cls, v: float | list[float]) -> float | list[float]:
        if isinstance(v, list) and len(v) == 0:
            raise ValueError("retry_delay list cannot be empty")
        return v


class Task(BaseModel, Generic[R]):
    """A task instance created when a task function is called.

    Attributes:
        id: Unique identifier for the task.
        status: Task status.
        exception: Exception data if the task failed, as a TaskException. None if the task succeeded or is not yet executed.
        error: Deprecated, use `exception` instead.
        result: The result of the task execution. If the task failed, this will be None.
        spec: Task specification
        config: Task configuration
        created_at: Timestamp when the task was created.
        finished_at: Timestamp when the task was finished. None if the task is not yet finished.
        scheduled_at: Timestamp when the task is scheduled to run. None if the task is not scheduled.
        retry_count: Number of times the task has been retried.
        last_retry_at: Timestamp when the task was last retried. None if the task has never been retried.
        next_retry_at: Timestamp when the task is scheduled to be retried next. None if the task is not scheduled for retry.
        is_retriable: Returns True if the task is configured to be retriable.
        should_retry: Returns True if the task should be retried based on its retry configuration and current retry count.
        workflow_id: ID of the workflow this task belongs to (if created within a workflow).

    Note:
        - You should not create Task instances directly. Instead, use the `@task` decorator to define a task function, and then call that function to create a Task instance.
        - `args` and `kwargs` in `spec` must be JSON serializable.

    Example:
        ```python
        from sheppy import task

        @task
        def add(x: int, y: int) -> int:
            return x + y

        t = add(2, 3)
        print(t.id)  # UUID of the task
        print(t.spec.func)  # "my_module:add"
        print(t.spec.args)  # (2, 3)
        print(t.result)  # None (not yet executed)
        ```
    """
    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    """UUID: Unique identifier for the task."""
    status: TaskStatus = 'new'
    """TaskStatus: Task status."""
    exception: TaskException | None = None
    """TaskException|None: Exception data if the task failed. None if the task succeeded or is not yet executed."""
    result: R | None = None
    """R|None: The result of the task execution. This will be None if the task failed or is not yet executed."""

    spec: TaskSpec
    """Task specification"""
    config: TaskConfig = Field(default_factory=TaskConfig)
    """Task configuration"""

    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    """datetime: Timestamp when the task was created."""
    finished_at: AwareDatetime | None = None
    """datetime|None: Timestamp when the task was finished. None if the task is not yet finished."""
    scheduled_at: AwareDatetime | None = None
    """datetime|None: Timestamp when the task is scheduled to run. None if the task is not scheduled."""

    retry_count: int = 0
    """int: Number of times the task has been retried."""
    last_retry_at: AwareDatetime | None = None
    """datetime|None: Timestamp when the task was last retried. None if the task has never been retried."""
    next_retry_at: AwareDatetime | None = None
    """datetime|None: Timestamp when the task is scheduled to be retried next. None if the task is not scheduled for retry."""

    workflow_id: UUID | None = None
    """UUID|None: ID of the workflow this task belongs to (if created within a workflow)."""

    cron_id: UUID | None = None
    """UUID|None: ID of the CronTask that created this job (temporary)"""

    # caller: str | None = None
    # worker: str | None = None

    # extra: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_retriable(self) -> bool:
        """Returns True if the task is configured to be retriable."""
        return self.config.retry > 0

    @property
    def should_retry(self) -> bool:
        """Returns True if the task should be retried based on its retry configuration and current retry count."""
        return self.config.retry > 0 and self.retry_count < self.config.retry

    @property
    def is_terminal(self) -> bool:
        """Returns True if the task reached a final state (completed or failed with no retries left)."""
        return self.status == 'completed' or (self.exception is not None and not self.should_retry)

    @property
    def completed(self) -> bool:
        """Return whether the task status is 'completed'. Deprecated: use `status` instead."""
        import warnings  # noqa: PLC0415
        warnings.warn("task.completed is deprecated, use task.status instead", category=DeprecationWarning, stacklevel=2)
        return self.status == "completed"

    @property
    def error(self) -> str | None:
        """Deprecated, use `task.exception` instead. Returns the error as a `"Type: message"` string."""
        import warnings  # noqa: PLC0415
        warnings.warn("task.error is deprecated, use task.exception instead", category=DeprecationWarning, stacklevel=2)
        return str(self.exception) if self.exception is not None else None

    @model_validator(mode='after')
    def _reconstruct_pydantic_result(self) -> 'Task[R]':
        """Reconstruct result if it's pydantic model."""

        if self.result is not None:
            self.__dict__["result"] = reconstruct_result(self.spec.func, self.result)

        return self

    def __str__(self) -> str:
        """Same as __repr__."""
        return self.__repr__()

    def __repr__(self) -> str:
        """String representation of the Task."""
        parts = {
            "id": repr(self.id),
            "func": repr(self.spec.func),
            "args": repr(self.spec.args),
            "kwargs": repr(self.spec.kwargs),
            "status": repr(self.status),
            "exception": repr(self.exception)
        }

        if self.retry_count > 0:
            parts["retry_count"] = str(self.retry_count)

        return f"Task({', '.join([f'{k}={v}' for k, v in parts.items()])})"


class TaskCron(BaseModel):
    """A cron definition that creates tasks on a schedule.

    Attributes:
        id: Unique identifier for the cron definition.
        expression: Cron expression defining the schedule, e.g. "*/5 * * * *" for every 5 minutes.
        spec: Task specification
        config: Task configuration
        managed_by: Origin of the cron definition. None means programmatic
            (added via `Queue.add_cron()`); "pyproject" means declared in a
            pyproject.toml file and reconciled by workers.
    Note:
        - You should not create TaskCron instances directly. Instead, use the `add_cron` method of the Queue class to create a cron definition.
        - `args` and `kwargs` in `spec` must be JSON serializable.

    Example:
        ```python
        from sheppy import Queue, task

        q = Queue(...)

        @task
        def say_hello(to: str) -> str:
            s = f"Hello, {to}!"
            print(s)
            return s

        # add_cron returns bool indicating success
        success = await q.add_cron(say_hello("World"), "*/5 * * * *")
        assert success is True

        # retrieve all cron jobs
        crons = await q.get_crons()
        for cron in crons:
            print(cron.id)  # UUID of the cron definition
            print(cron.expression)  # "*/5 * * * *"
            print(cron.spec.func)  # "my_module:say_hello"
            print(cron.spec.args)  # ["World"]
        ```
    """
    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    """UUID: Unique identifier for the cron definition."""
    expression: CronExpression
    """str: Cron expression defining the schedule, e.g. "*/5 * * * *" for every 5 minutes."""

    spec: TaskSpec
    """Task specification"""
    config: TaskConfig
    """Task configuration"""

    managed_by: str | None = None
    """str|None: Origin of the cron definition; None for programmatic, "pyproject" for declarative."""

    # enabled: bool = True
    # last_run: AwareDatetime | None = None
    # next_run: AwareDatetime | None = None

    @property
    def deterministic_id(self) -> UUID:
        """Deterministic UUID to prevent duplicated cron definitions.

        This property generates a deterministic UUID for the cron definition based on its spec, config, and expression.
        This ensures that identical cron definitions always have the same UUID, preventing duplicates.

        Returns:
            UUID: A deterministic UUID based on the cron definition's spec, config, and expression.

        Example:
            ```python
            from sheppy import task, Queue
            from sheppy.task_factory import TaskFactory

            @task
            def say_hello(to: str) -> None:
                print(f"Hello, {to}!")

            q = Queue(...)
            success = await q.add_cron(say_hello("World"), "*/5 * * * *")
            assert success is True

            success = await q.add_cron(say_hello("World"), "*/5 * * * *")
            assert success is False  # duplicate cron definition prevented

            # second example
            cron1 = TaskFactory.create_cron_from_task(say_hello("World"), "*/5 * * * *")
            cron2 = TaskFactory.create_cron_from_task(say_hello("World"), "*/5 * * * *")
            assert cron1.deterministic_id == cron2.deterministic_id
            assert cron1.id != cron2.id  # different random UUIDs
            ```
        """
        s = self.spec.model_dump_json() + self.config.model_dump_json() + self.expression
        return uuid5(TASK_CRON_NS, s)

    def next_run(self, start: datetime | None = None) -> datetime:
        """Get the next scheduled run time based on the cron expression.

        Args:
            start: The starting point to calculate the next run time. If None, the current UTC time is used.

        Returns:
            datetime: The next scheduled run time.
        """
        if not start:
            start = datetime.now(timezone.utc)
        return croniter(self.expression, start).get_next(datetime)

    def create_task(self, start: datetime) -> Task[Any]:
        """Create a Task instance for the next scheduled run. Used by workers to create tasks based on the cron schedule.

        The task ID is deterministic based on the cron definition and the scheduled time to prevent duplicates.

        Args:
            start: The scheduled time for the task.

        Returns:
            Task: A new Task instance scheduled to run at the specified time.
        """
        return Task[Any](
            id=uuid5(TASK_CRON_NS, str(self.deterministic_id) + str(start.timestamp())),
            spec=self.spec.model_copy(deep=True),
            config=self.config.model_copy(deep=True),
            cron_id=self.deterministic_id,
        )
