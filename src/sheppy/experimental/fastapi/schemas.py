from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RetryRequest(BaseModel):
    delay_seconds: float = Field(
        default=0.0,
        description="Delay in seconds before retrying. If not provided, retries immediately."
    )
    force: bool = Field(
        default=False,
        description="Force retry even if the task completed successfully."
    )


class AddTaskRequest(BaseModel):
    model_config = {"extra": "forbid"}

    func: str = Field(description="Fully qualified function name (e.g., 'mymodule:my_task')")
    args: list[Any] = Field(default_factory=list, description="Positional arguments for the task")
    kwargs: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the task")


class ScheduleTaskRequest(BaseModel):
    model_config = {"extra": "forbid"}

    func: str = Field(description="Fully qualified function name (e.g., 'mymodule:my_task')")
    args: list[Any] = Field(default_factory=list, description="Positional arguments for the task")
    kwargs: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the task")
    delay_seconds: float | None = Field(
        default=None,
        description="Delay in seconds before executing. Mutually exclusive with 'at'."
    )
    at: datetime | None = Field(
        default=None,
        description="Specific datetime to execute at (must be timezone-aware). Mutually exclusive with 'delay_seconds'."
    )


class CronRequest(BaseModel):
    model_config = {"extra": "forbid"}

    func: str = Field(description="Fully qualified function name (e.g., 'mymodule:my_task')")
    args: list[Any] = Field(default_factory=list, description="Positional arguments for the task")
    kwargs: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the task")
    expression: str = Field(description="Cron expression")
