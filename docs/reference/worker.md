# `Worker` class reference

In most cases you run a worker with the CLI: `sheppy work`. See the [CLI reference](cli.md#sheppy-work) for its options.

To run a worker programmatically:

```python
import asyncio
from sheppy import RedisBackend, Worker

worker = Worker(queue_name="default", backend=RedisBackend(...))
asyncio.run(worker.work())
```

`queue_name` and `backend` are required arguments. The `SHEPPY_*` environment variables are read by the CLI, not by the `Worker` class. Configure the class explicitly.

::: sheppy.Worker
    options:
        members:
            - work
