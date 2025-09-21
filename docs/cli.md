# Sheppy CLI reference

Sheppy - Modern Task Queue

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

* `-q, --queue TEXT`: Name of queue to process (can be used multiple times)  [default: default]
* `-b, --backend [redis]`: Queue backend type  [default: redis]
* `-r, --redis-url TEXT`: Redis server URL  [default: redis://127.0.0.1:6379]
* `-c, --max-concurrent INTEGER RANGE`: Max concurrent tasks  [default: 10; x&gt;=1]
* `--reload`: Reload worker on file changes
* `--oneshot`: Process pending tasks and then exit
* `--max-tasks INTEGER RANGE`: Maximum amount of tasks to process  [x&gt;=1]
* `--disable-job-processing`: Disable job processing
* `--disable-scheduler`: Disable scheduler
* `--disable-cron-manager`: Disable cron manager
* `-l, --log-level [debug|info|warning|error]`: Logging level  [default: info]
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

* `-q, --queue TEXT`: Name of queue  [default: default]
* `-s, --status [all|pending|scheduled|completed|failed]`: Filter by status  [default: all]
* `-b, --backend [redis]`: Queue backend type  [default: redis]
* `-r, --redis-url TEXT`: Redis server URL  [default: redis://127.0.0.1:6379]
* `-f, --format [table|json]`: Output format  [default: table]
* `--help`: Show this message and exit.

### `sheppy task info`

Get detailed information about a specific task.

**Usage**:

```console
$ sheppy task info [OPTIONS] TASK_ID
```

**Arguments**:

* `TASK_ID`: Task ID to get info for  [required]

**Options**:

* `-q, --queue TEXT`: Name of queue  [default: default]
* `-b, --backend [redis]`: Queue backend type  [default: redis]
* `-r, --redis-url TEXT`: Redis server URL  [default: redis://127.0.0.1:6379]
* `--help`: Show this message and exit.

### `sheppy task retry`

Retry a failed task by re-queueing it.

**Usage**:

```console
$ sheppy task retry [OPTIONS] TASK_ID
```

**Arguments**:

* `TASK_ID`: Task ID to retry  [required]

**Options**:

* `-q, --queue TEXT`: Name of queue  [default: default]
* `-b, --backend [redis]`: Queue backend type  [default: redis]
* `-r, --redis-url TEXT`: Redis server URL  [default: redis://127.0.0.1:6379]
* `-f, --force`: Force retry even if task hasn&#x27;t failed
* `--help`: Show this message and exit.

### `sheppy task test`

Test run a task function without queuing it.

**Usage**:

```console
$ sheppy task test [OPTIONS] FUNCTION
```

**Arguments**:

* `FUNCTION`: Function to test (module:function format)  [required]

**Options**:

* `-a, --args TEXT`: JSON array of positional arguments  [default: []]
* `-k, --kwargs TEXT`: JSON object of keyword arguments  [default: {}]
* `-t, --trace`: Show full execution trace
* `--help`: Show this message and exit.

### `sheppy task add`

Add a new task to a queue.

**Usage**:

```console
$ sheppy task add [OPTIONS] FUNCTION
```

**Arguments**:

* `FUNCTION`: Function to add (module:function format)  [required]

**Options**:

* `-a, --args TEXT`: JSON array of positional arguments  [default: []]
* `-k, --kwargs TEXT`: JSON object of keyword arguments  [default: {}]
* `-w, --wait`: Wait for task result
* `-t, --timeout FLOAT`: Timeout in seconds when waiting for result  [default: 0.0]
* `-q, --queue TEXT`: Name of queue  [default: default]
* `-b, --backend [redis]`: Queue backend type  [default: redis]
* `-r, --redis-url TEXT`: Redis server URL  [default: redis://127.0.0.1:6379]
* `--help`: Show this message and exit.

### `sheppy task schedule`

Schedule a task to run at a specific time.

**Usage**:

```console
$ sheppy task schedule [OPTIONS] FUNCTION
```

**Arguments**:

* `FUNCTION`: Function to schedule (module:function format  [required]

**Options**:

* `-d, --delay TEXT`: Delay before task execution (e.g., 30s, 5m, 2h, 1d)
* `--at TEXT`: Execute at specific time (ISO format: 2024-01-20T15:30:00)
* `-a, --args TEXT`: JSON array of positional arguments  [default: []]
* `-k, --kwargs TEXT`: JSON object of keyword arguments  [default: {}]
* `-q, --queue TEXT`: Name of queue  [default: default]
* `-b, --backend [redis]`: Queue backend type  [default: redis]
* `-r, --redis-url TEXT`: Redis server URL  [default: redis://127.0.0.1:6379]
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

* `-b, --backend [redis]`: Queue backend type  [default: redis]
* `-r, --redis-url TEXT`: Redis server URL  [default: redis://127.0.0.1:6379]
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

* `-q, --queue TEXT`: Name of queue  [default: default]
* `-b, --backend [redis]`: Queue backend type  [default: redis]
* `-r, --redis-url TEXT`: Redis server URL  [default: redis://127.0.0.1:6379]
* `--help`: Show this message and exit.
