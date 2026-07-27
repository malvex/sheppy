# `Task` model reference

A `Task` is a frozen Pydantic model describing one unit of work: which function to call, with what arguments, under what configuration, and the result or exception data after the task is processed.

You rarely construct `Task` directly: calling a `@task`-decorated function returns one.

::: sheppy.Task
