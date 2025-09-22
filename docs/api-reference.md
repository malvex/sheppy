# Sheppy API Reference

Complete API documentation for all Sheppy components.

## Decorator `@task`

Decorator that converts a function into a queueable task.

```python
from sheppy import task

@task
async def add(x: int, y: int) -> int:
    return x + y
```

**Parameters:**
- `retry` (int, optional): Maximum retry attempts for failed tasks
- `retry_delay` (float or list[float], optional): Delay between retry attempts (exponential backoff supported through list)

**Returns:**
- A callable that returns a `Task` object (an instance of a task)

**Example:**

```python
@task(retry=3, retry_delay=[10, 30, 60])
async def retriable_task(msg: str):
    # will retry up to 3 times
    pass

# create task instance
task_instance = retriable_task(msg="hello")
```

## Models

### `Task`

Represents a queueable task with its state and metadata.

**Attributes:**

| Attribute       | Type               | Description                         |
| --------------- | ------------------ | ----------------------------------- |
| `id`            | `UUID`             | Unique identifier (UUID)            |
| `completed`     | `bool`             | Whether task completed successfully |
| `error`         | `str \| None`      | Error message if failed             |
| `result`        | `Any \| None`      | Return value if completed           |
| `spec`          | `Spec`             | Task specification (`Spec` model)   |
| `config`        | `Config`           | Task configuration (`Config` model) |
| `created_at`    | `datetime`         | When task was created               |
| `finished_at`   | `datetime \| None` | When task finished                  |
| `scheduled_at`  | `datetime \| None` | When task should run                |
| `retry_count`   | `int`              | How many times was the task retried |
| `last_retry_at` | `datetime \| None` | When last retry happened            |
| `next_retry_at` | `datetime \| None` | When next retry happens             |

**Computed Attributes:**

| Attribute      | Type   | Description                                            |
| -------------- | ------ | ------------------------------------------------------ |
| `is_retriable` | `bool` | If task is retriable (config.retry > 0)                |
| `should_retry` | `bool` | If task should be retried (used internally by workers) |


### `Spec`

Task specification describing what function will run and with what arguments.

**Attributes:**

| Attribute     | Type                | Description                                 |
| ------------- | ------------------- | ------------------------------------------- |
| `func`        | `str`               | Function name (module.submodule:func)       |
| `args`        | `list[Any]`         | Positional arguments                        |
| `kwargs`      | `dict[str, Any]`    | Keyword arguments                           |
| `return_type` | `str \| None`       | Function return type (filled automatically) |
| `middleware`  | `list[str] \| None` | Task Middleware                             |

### `Config`

Task configuration with optional task configuration.

**Attributes:**

| Attribute     | Type                   | Description              |
| ------------- | ---------------------- | ------------------------ |
| `retry`       | `float`                | Number of retry attempts |
| `retry_delay` | `float \| list[float]` | Delay in seconds         |

### `TaskCron`

Cron definition, including cron expression.

**Attributes:**

| Attribute    | Type     | Description                         |
| ------------ | -------- | ----------------------------------- |
| `id`         | `UUID`   | Unique identifier (UUID)            |
| `expression` | `str`    | Cron expression (e.g. "* * * * *")  |
| `spec`       | `Spec`   | Task specification (`Spec` model)   |
| `config`     | `Config` | Task configuration (`Config` model) |

**Computed Attributes:**

| Attribute          | Type   | Description                                                                             |
| ------------------ | ------ | --------------------------------------------------------------------------------------- |
| `deterministic_id` | `UUID` | Returns deterministic identifierto prevent duplicated cron definitions, used internally |


**Methods:**

#### `next_run(start: datetime | None = None) -> datetime`

Returns datetime of the next run.

#### `create_task(start: datetime) -> Task`

Create a scheduled task for the next run. Used internally by workers.

## Queue Operations

### `Queue`

Manages task storage and retrieval.

```python
from sheppy import Queue, RedisBackend

backend = RedisBackend("redis://localhost:6379")
queue = Queue(backend=backend, name="my-queue")
```

**Constructor Parameters:**

- `backend` (Backend): Task Backend
- `name` (str, optional): Queue name for task organization. Default: "default"

**Methods:**

#### `add(task: Task | list[Task]) -> list[bool]`

Add a task to the queue.

```python
await queue.add(task)
await queue.add([task1, task2])  # pass a list of Tasks for bulk add
```

**Parameters:**

- `task`: Task instance to queue, or list of Tasks for bulk add

#### `schedule(task: Task, at: datetime | timedelta) -> bool`

Schedule a task to a specific time.

```python
await queue.schedule(task, at=timedelta(minutes=5))  # task runs in 5 mins
await queue.schedule(task, at=datetime.fromisoformat("2026-01-01 00:00:00 +00:00"))  # must include timezone!
```

**Parameters:**

- `task`: Task instance to queue
- `at`: Datetime for specific datetime or timedelta

#### `get_task(task: Task | UUID) -> Task | None`

Retrieve a task by instance or ID.

```python
task = await queue.get_task("cf5ba47f-bab3-4853-a21c-ab12ed5ca66d")
task = await queue.get_task(task_instance)
```

**Returns:** Task with current state or None if not found

#### `wait_for(task: Task, timeout: float = 0) -> Task`

Blocking wait for a task to complete. Waits forever if timeout is zero, or throws exception when timeout is exceeded.

```python
try:
    completed = await queue.wait_for(task, timeout=60.0)
    print(completed.result)
except TimeoutError:
    print("Task didn't complete in time")
```

**Parameters:**

- `task`: Task to wait for
- `timeout`: Maximum seconds to wait. Default: 0 (wait forever)

**Raises:** `TimeoutError` if task doesn't complete in time

#### `size() -> int`

Get number of pending tasks.

```python
size = await queue.size()
print(f"{size} tasks waiting")
```

## Testing

### `TestQueue`

Synchronous queue for testing with pytest.

```python
from sheppy import TestQueue

queue = TestQueue()
```

**Key Differences from Queue:**

- First class task testing
- Synchronous API (no await needed)
- Immediate, but controlled step processing
- Dependency override support

**Methods:**

#### `add(task: Task | list[Task]) -> None`

Add a task. Supports adding multiple tasks at once (batch).

```python
queue.add(task)  # no await!
```

#### `process_next() -> Task | None`

Process the next task in the queue.

```python
processed_task = queue.process_next()
assert processed_task.completed
```

Hint: This allows you to process tasks one step at a time, reliably validating all possible task states.

#### `process_all() -> list[Task]`

Process all pending tasks.

```python
tasks = queue.process_all()
for task in tasks:
    assert task.completed
```

#### `process_scheduled(at: datetime | timedelta | None = None) -> list[Task]`

Process scheduled tasks after specified datetime or timedelta.

```python
queue.schedule(add(1, 2), at=timedelta(minutes=10))
tasks = queue.process_scheduled()
assert len(tasks) == 0  # nothing was processed because task was scheduled for later

tasks = queue.process_scheduled(timedelta(minutes=10))  # time travel 10 minutes into the future
assert len(tasks) == 1  # tasks was processed
assert tasks[0].completed
assert tasks[0].result == 3
```

#### `dependency_overrides`

(todo)

## Backends

### `Backend` (Abstract Base)

Base class for all storage backends.

### `RedisBackend`

Redis-based storage backend.

```python
from sheppy import RedisBackend

backend = RedisBackend("redis://localhost:6379")
```

**Constructor Parameters:**

- `url` (str): Redis connection URL
- (todo)

**Features:**

- Persistent storage
- Multi-process/multi-machine support
- Automatic connection pooling
- Configurable result TTL

## Dependency Injection

### `Depends`

FastAPI-compatible dependency injection.

```python
from sheppy import Depends

def get_service():
    return MyService()

@task
async def my_task(service: MyService = Depends(get_service)):
    return await service.process()
```

**Nested Dependencies:**

```python
def get_config():
    return {"db_url": "postgresql://..."}

def get_db(config: dict = Depends(get_config)):
    return Database(config["db_url"])

@task
async def query_data(db: Database = Depends(get_db)):
    return await db.query("SELECT * FROM users")
```

## Exceptions

### `BackendError`

Base exception for backend-related errors.

```python
from sheppy import BackendError

try:
    await queue.add(task)
except BackendError as e:
    print(f"Backend error: {e}")
```

### `TimeoutError`

Standard Python TimeoutError raised when waiting for results.

```python
try:
    result = await queue.wait_for(task, timeout=5.0)
except TimeoutError:
    print("Task didn't complete in 5 seconds")
```

## Type Hints

todo

## Configuration

### Environment Variables

todo

### Logging

todo
