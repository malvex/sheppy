# Sheppy CLI reference

Sheppy - async-native task queue for Python

**Usage**:

```console
$ sheppy [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--version`: Show the version and exit.
* `--help`: Show this message and exit.

**Commands**:

* `work`: Start a worker to process tasks from a queue.
* `task`: Task management commands
* `queue`: Queue management commands
* `cron`: Cron management commands

## `sheppy work`

Start a worker to process tasks from a queue.

**Usage**:

```console
$ sheppy work [OPTIONS]
```

**Options**:

* `-q, --queue <str>`: Queue name(s). Env: SHEPPY_QUEUE  [default: default]
* `-u, --backend-url <str>`: Backend URL. Env: SHEPPY_BACKEND_URL
* `-c, --max-concurrent <int range>`: Max concurrent tasks. Env: SHEPPY_MAX_CONCURRENT_TASKS  [default: 10; x&gt;=1]
* `--max-prefetch <int range>`: Max prefetch tasks  [x&gt;=1]
* `--oneshot`: Process pending tasks and then exit
* `--max-tasks <int range>`: Maximum amount of tasks to process  [x&gt;=1]
* `--disable-job-processing`: Disable job processing
* `--disable-scheduler`: Disable scheduler
* `--disable-cron-manager`: Disable cron manager
* `-l, --log-level <debug|info|warning|error>`: Logging level. Env: SHEPPY_LOG_LEVEL  [default: info]
* `--shutdown-timeout <float>`: Shutdown timeout in seconds. Env: SHEPPY_SHUTDOWN_TIMEOUT  [default: 30.0]
* `--prestart <str>`: Prestart hook &#x27;module:function&#x27; run before the worker starts. Env: SHEPPY_PRESTART
* `--help`: Show this message and exit.

## `sheppy task`

Task management commands

**Usage**:

```console
$ sheppy task [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `list`: List all tasks.
* `info`: Get detailed information about a specific...
* `retry`: Retry a failed task by re-queueing it.
* `test`: Test run a task function without queuing it.
* `add`: Add a new task to a queue.
* `schedule`: Schedule a task to run at a specific time.

### `sheppy task list`

List all tasks.

**Usage**:

```console
$ sheppy task list [OPTIONS]
```

**Options**:

* `-q, --queue <str>`: Queue name. Env: SHEPPY_QUEUE  [default: default]
* `-u, --backend-url <str>`: Backend URL. Env: SHEPPY_BACKEND_URL
* `-s, --status <all|pending|scheduled|completed|failed>`: Filter by status  [default: all]
* `-f, --format <table|json>`: Output format  [default: table]
* `--help`: Show this message and exit.

### `sheppy task info`

Get detailed information about a specific task.

**Usage**:

```console
$ sheppy task info [OPTIONS] {task_id}
```

**Arguments**:

* `task_id`: Task ID to get info for  [required]

**Options**:

* `-q, --queue <str>`: Queue name. Env: SHEPPY_QUEUE  [default: default]
* `-u, --backend-url <str>`: Backend URL. Env: SHEPPY_BACKEND_URL
* `--help`: Show this message and exit.

### `sheppy task retry`

Retry a failed task by re-queueing it.

**Usage**:

```console
$ sheppy task retry [OPTIONS] {task_id}
```

**Arguments**:

* `task_id`: Task ID to retry  [required]

**Options**:

* `-q, --queue <str>`: Queue name. Env: SHEPPY_QUEUE  [default: default]
* `-u, --backend-url <str>`: Backend URL. Env: SHEPPY_BACKEND_URL
* `-f, --force`: Force retry even if task hasn&#x27;t failed
* `--help`: Show this message and exit.

### `sheppy task test`

Test run a task function without queuing it.

**Usage**:

```console
$ sheppy task test [OPTIONS] {function}
```

**Arguments**:

* `function`: Function to test (module:function format)  [required]

**Options**:

* `-a, --args <str>`: JSON array of positional arguments  [default: []]
* `-k, --kwargs <str>`: JSON object of keyword arguments  [default: {}]
* `-t, --trace`: Show full execution trace
* `--help`: Show this message and exit.

### `sheppy task add`

Add a new task to a queue.

**Usage**:

```console
$ sheppy task add [OPTIONS] {function}
```

**Arguments**:

* `function`: Function to add (module:function format)  [required]

**Options**:

* `-a, --args <str>`: JSON array of positional arguments  [default: []]
* `-k, --kwargs <str>`: JSON object of keyword arguments  [default: {}]
* `-w, --wait`: Wait for task result
* `-t, --timeout <float>`: Timeout in seconds when waiting for result  [default: 0.0]
* `-q, --queue <str>`: Queue name. Env: SHEPPY_QUEUE  [default: default]
* `-u, --backend-url <str>`: Backend URL. Env: SHEPPY_BACKEND_URL
* `--help`: Show this message and exit.

### `sheppy task schedule`

Schedule a task to run at a specific time.

**Usage**:

```console
$ sheppy task schedule [OPTIONS] {function}
```

**Arguments**:

* `function`: Function to schedule (module:function format  [required]

**Options**:

* `-d, --delay <str>`: Delay before task execution (e.g., 30s, 5m, 2h, 1d)
* `--at <str>`: Execute at specific time (ISO format: 2024-01-20T15:30:00)
* `-a, --args <str>`: JSON array of positional arguments  [default: []]
* `-k, --kwargs <str>`: JSON object of keyword arguments  [default: {}]
* `-q, --queue <str>`: Queue name. Env: SHEPPY_QUEUE  [default: default]
* `-u, --backend-url <str>`: Backend URL. Env: SHEPPY_BACKEND_URL
* `--help`: Show this message and exit.

## `sheppy queue`

Queue management commands

**Usage**:

```console
$ sheppy queue [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `list`: List all queues with their pending task...

### `sheppy queue list`

List all queues with their pending task counts.

**Usage**:

```console
$ sheppy queue list [OPTIONS]
```

**Options**:

* `-u, --backend-url <str>`: Backend URL. Env: SHEPPY_BACKEND_URL
* `--help`: Show this message and exit.

## `sheppy cron`

Cron management commands

**Usage**:

```console
$ sheppy cron [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `list`: List all active crons.

### `sheppy cron list`

List all active crons.

**Usage**:

```console
$ sheppy cron list [OPTIONS]
```

**Options**:

* `-q, --queue <str>`: Queue name. Env: SHEPPY_QUEUE  [default: default]
* `-u, --backend-url <str>`: Backend URL. Env: SHEPPY_BACKEND_URL
* `--help`: Show this message and exit.
