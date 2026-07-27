# `TaskException` model reference

`TaskException` is the JSON-serializable record of a task or workflow failure: the exception's type, module, message, sanitized args, and formatted traceback. It is stored on `task.exception` (and `workflow.exception`) when execution fails, so failures can be inspected from any process. `to_exception()` rebuilds the original exception on a best-effort basis, falling back to `TaskFailedError` when the class cannot be imported or constructed.

::: sheppy.models.TaskException
