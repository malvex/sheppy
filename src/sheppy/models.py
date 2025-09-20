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
    model_config = ConfigDict(frozen=True)

    func: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    return_type: str | None = None
    middleware: list[str] | None = None


class Config(BaseModel):
    model_config = ConfigDict(frozen=True)

    retry: int = Field(default=0, ge=0)
    retry_delay: float | list[float] = Field(default=1.0)

    # timeout: float | None = None  # seconds
    # tags: dict[str, str] = Field(default_factory=dict)


class Task(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    completed: bool = False
    error: str | None = None
    result: Any = None

    spec: Spec
    config: Config = Field(default_factory=Config)

    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: AwareDatetime | None = None
    scheduled_at: AwareDatetime | None = None

    retry_count: int = 0
    last_retry_at: AwareDatetime | None = None
    next_retry_at: AwareDatetime | None = None
    # caller: str | None = None
    # worker: str | None = None

    # extra: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_retriable(self) -> bool:
        return self.config.retry > 0

    @property
    def should_retry(self) -> bool:
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
        return self.__repr__()

    def __repr__(self) -> str:
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
    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    expression: CronExpression

    spec: Spec
    config: Config

    # enabled: bool = True
    # last_run: AwareDatetime | None = None
    # next_run: AwareDatetime | None = None

    @property
    def deterministic_id(self) -> UUID:
        """Deterministic UUID to prevent duplicated cron definitions."""
        s = self.spec.model_dump_json() + self.config.model_dump_json() + self.expression
        return uuid5(TASK_CRON_NS, s)

    def next_run(self, start: datetime | None = None) -> datetime:
        if not start:
            start = datetime.now(timezone.utc)
        return croniter(self.expression, start).get_next(datetime)

    def create_task(self, start: datetime) -> Task:
        return Task(
            id=uuid5(TASK_CRON_NS, str(self.deterministic_id) + str(start.timestamp())),
            spec=self.spec.model_copy(deep=True),
            config=self.config.model_copy(deep=True)
        )
