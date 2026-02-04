import os
from collections.abc import MutableMapping
from typing import Literal

from pydantic import BaseModel, Field, field_validator

LogLevelType = Literal["debug", "info", "warning", "error"]

DEFAULT_QUEUE = "default"
DEFAULT_MAX_CONCURRENT_TASKS = 10
DEFAULT_SHUTDOWN_TIMEOUT = 30.0
DEFAULT_LOG_LEVEL: LogLevelType = "info"


class EnvConfig(BaseModel):
    backend_url: str | None = None
    queue: str = DEFAULT_QUEUE
    max_concurrent_tasks: int = Field(default=DEFAULT_MAX_CONCURRENT_TASKS, ge=1)
    shutdown_timeout: float = Field(default=DEFAULT_SHUTDOWN_TIMEOUT, ge=0)
    log_level: LogLevelType = DEFAULT_LOG_LEVEL

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, v: str) -> str:
        return v.lower() if isinstance(v, str) else v

    @property
    def queue_list(self) -> list[str]:
        return [q.strip() for q in self.queue.split(",") if q.strip()]


def load_config(environ: MutableMapping[str, str]) -> EnvConfig:
    return EnvConfig(
        backend_url=environ.get("SHEPPY_BACKEND_URL"),
        queue=environ.get("SHEPPY_QUEUE", DEFAULT_QUEUE),
        max_concurrent_tasks=environ.get("SHEPPY_MAX_CONCURRENT_TASKS", DEFAULT_MAX_CONCURRENT_TASKS),
        shutdown_timeout=environ.get("SHEPPY_SHUTDOWN_TIMEOUT", DEFAULT_SHUTDOWN_TIMEOUT),
        log_level=environ.get("SHEPPY_LOG_LEVEL", DEFAULT_LOG_LEVEL),
    )


config = load_config(os.environ)
