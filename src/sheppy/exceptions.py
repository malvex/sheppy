class TaskTimeoutError(TimeoutError):
    pass


class MiddlewareError(Exception):
    pass


class WorkflowError(Exception):
    pass


class WorkerCrashedError(Exception):
    pass


class TaskCancellationError(Exception):
    """Raised when a task cannot be cancelled (already claimed by a worker, already finished, or not found)."""


class TaskFailedError(Exception):
    """Fallback exception for task failures whose original exception class cannot be reconstructed."""

    def __init__(self, original_type: str, message: str, traceback: str | None = None) -> None:
        self.original_type = original_type
        self.traceback = traceback
        super().__init__(message)
