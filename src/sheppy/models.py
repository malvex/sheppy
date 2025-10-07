import importlib
from datetime import datetime, timezone
from typing import (
    Annotated,
    Any,
    ParamSpec,
    TypeVar,
)
from uuid import UUID, uuid4, uuid5

from croniter import croniter
from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    model_validator,
)

P = ParamSpec('P')
R = TypeVar('R')


TASK_CRON_NS = UUID('7005b432-c135-4131-b19e-d3dc89703a9a')


def cron_expression_validator(value: str) -> str:
    if not croniter.is_valid(value):
        raise ValueError(f"{value} is not a valid cron expression")

    return value

CronExpression = Annotated[str, AfterValidator(cron_expression_validator)]


class Spec(BaseModel):
    """Task specification.

    Attributes:
        func (str): Fully qualified function name, e.g. `my_module.my_submodule:my_function`
        args (list[Any]): Positional arguments to be passed to the function.
        kwargs (dict[str, Any]): Keyword arguments to be passed to the function.
        return_type (str|None): Fully qualified return type name, e.g. `my_module.submodule:MyPydanticModel`. This is used to reconstruct the return value if it's a pydantic model.
        middleware (list[str]|None): List of fully qualified middleware function names to be applied to the task, e.g. `['my_module.submodule:my_middleware']`. Middleware will be applied in the order they are listed.

    Note:
        - You should not create Spec instances directly. Instead, use the `@task` decorator to define a task function, and then call that function to create a Task instance.
        - `args` and `kwargs` must be JSON serializable.

    Example:
        ```python
        from sheppy import task

        @task
        def my_task(x: int, y: str) -> str:
            return f"Received {x} and {y}"

        t = my_task(42, "hello")  # returns a Task instance, it is NOT executed yet

        print(t.spec.func)  # e.g. "my_module:my_task"
        print(t.spec.args)  # [42, "hello"]
        print(t.spec.return_type)  # "builtins.str"
        ```
    """
    model_config = ConfigDict(frozen=True)

    func: str
    """str: Fully qualified function name, e.g. `my_module.my_submodule:my_function`"""
    args: list[Any] = Field(default_factory=list)
    """list[Any]: Positional arguments to be passed to the function."""
    kwargs: dict[str, Any] = Field(default_factory=dict)
    """dict[str, Any]: Keyword arguments to be passed to the function."""
    return_type: str | None = None
    """str|None: Fully qualified return type name, e.g. `my_module.submodule:MyPydanticModel`. This is used to reconstruct the return value if it's a pydantic model."""
    middleware: list[str] | None = None
    """list[str]|None: List of fully qualified middleware function names to be applied to the task, e.g. `['my_module.submodule:my_middleware']`. Middleware will be applied in the order they are listed."""


class Config(BaseModel):
    """Task configuration

    Attributes:
        retry (int): Number of times to retry the task if it fails. Default is 0 (no retries).
        retry_delay (float|list[float]): Delay between retries in seconds. If a single float is provided, it will be used for all retries. If a list is provided, it will be used for each retry attempt respectively (exponential backoff). Default is 1.0 seconds.

    Note:
        - You should not create Config instances directly. Instead, use the `@task` decorator to define a task function, and then call that function to create a Task instance.
        - `retry` must be a non-negative integer.
        - `retry_delay` must be a positive float or a list of positive floats.

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
    model_config = ConfigDict(frozen=True)

    retry: int = Field(default=0, ge=0)
    """int: Number of times to retry the task if it fails. Default is 0 (no retries)."""
    retry_delay: float | list[float] = Field(default=1.0)
    """float|list[float]: Delay between retries in seconds. If a single float is provided, it will be used for all retries. If a list is provided, it will be used for each retry attempt respectively (exponential backoff). Default is 1.0 seconds."""

    # timeout: float | None = None  # seconds
    # tags: dict[str, str] = Field(default_factory=dict)


class Task(BaseModel):
    """A task instance created when a task function is called.

    Attributes:
        id (UUID): Unique identifier for the task.
        completed (bool): A completion flag that is set to True only if task finished successfully.
        error (str|None): Error message if the task failed. None if the task succeeded or is not yet executed.
        result (Any): The result of the task execution. If the task failed, this will be None.
        spec (sheppy.models.Spec): Task specification
        config (sheppy.models.Config): Task configuration
        created_at (datetime): Timestamp when the task was created.
        finished_at (datetime|None): Timestamp when the task was finished. None if the task is not yet finished.
        scheduled_at (datetime|None): Timestamp when the task is scheduled to run. None if the task is not scheduled.
        retry_count (int): Number of times the task has been retried.
        last_retry_at (datetime|None): Timestamp when the task was last retried. None if the task has never been retried.
        next_retry_at (datetime|None): Timestamp when the task is scheduled to be retried next. None if the task is not scheduled for retry.
        is_retriable (bool): Returns True if the task is configured to be retriable.
        should_retry (bool): Returns True if the task should be retried based on its retry configuration and current retry count.

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
        print(t.spec.args)  # [2, 3]
        print(t.result)  # None (not yet executed)
        ```
    """
    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    """UUID: Unique identifier for the task."""
    completed: bool = False
    """bool: A completion flag that is set to True only if task finished successfully."""
    error: str | None = None
    """str|None: Error message if the task failed. None if the task succeeded or is not yet executed."""
    result: Any = None
    """Any: The result of the task execution. This will be None if the task failed or is not yet executed."""

    spec: Spec
    """Task specification"""
    config: Config = Field(default_factory=Config)
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

    @model_validator(mode='after')
    def _reconstruct_pydantic_result(self) -> 'Task':
        """Reconstruct result if it's pydantic model."""

        if self.result and self.spec.return_type:
            # Reconstruct return if it's pydantic model
            module_name, type_name = self.spec.return_type.rsplit('.', 1)
            module = importlib.import_module(module_name)
            return_type = getattr(module, type_name)
            self.__dict__["result"] = TypeAdapter(return_type).validate_python(self.result)

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
            "completed": repr(self.completed),
            "error": repr(self.error)
        }

        if self.retry_count > 0:
            parts["retry_count"] = str(self.retry_count)

        return f"Task({', '.join([f'{k}={v}' for k, v in parts.items()])})"


class TaskCron(BaseModel):
    """A cron definition that creates tasks on a schedule.

    Attributes:
        id (UUID): Unique identifier for the cron definition.
        expression (str): Cron expression defining the schedule, e.g. "*/5 * * * *" for every 5 minutes.
        spec (sheppy.models.Spec): Task specification
        config (sheppy.models.Config): Task configuration

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

    spec: Spec
    """Task specification"""
    config: Config
    """Task configuration"""

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
            start (datetime|None): The starting point to calculate the next run time. If None, the current UTC time is used.

        Returns:
            datetime: The next scheduled run time.
        """
        if not start:
            start = datetime.now(timezone.utc)
        return croniter(self.expression, start).get_next(datetime)

    def create_task(self, start: datetime) -> Task:
        """Create a Task instance for the next scheduled run. Used by workers to create tasks based on the cron schedule.

        The task ID is deterministic based on the cron definition and the scheduled time to prevent duplicates.

        Args:
            start (datetime): The scheduled time for the task.

        Returns:
            Task: A new Task instance scheduled to run at the specified time.
        """
        return Task(
            id=uuid5(TASK_CRON_NS, str(self.deterministic_id) + str(start.timestamp())),
            spec=self.spec.model_copy(deep=True),
            config=self.config.model_copy(deep=True)
        )
