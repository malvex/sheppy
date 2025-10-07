# Handling Errors in Tasks

When a task raises an exception during its execution, Sheppy automatically marks the task as failed. The exception message is captured and stored in the `error` attribute of the task instance.
