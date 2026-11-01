import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from typer.testing import CliRunner

import sheppy.cli.commands.task.list as task_list_command
from sheppy import Queue, Task
from sheppy._backend.memory import InMemoryBackend
from sheppy.cli.cli import app
from sheppy.models import TaskException, TaskSpec, TaskStatus

QUEUE_NAME = "cli-task-list"
BASE_TIME = datetime(2025, 1, 1, tzinfo=timezone.utc)
TASK_EXCEPTION = TaskException(type="ValueError", module="builtins", message="boom")
STATUSES = [
    "new",
    "pending",
    "scheduled",
    "processing",
    "retrying",
    "completed",
    "failed",
    "crashed",
    "cancelled",
    "unknown",
]


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def cli_backend(monkeypatch):
    backend = InMemoryBackend(instant_processing=False)
    monkeypatch.setattr(task_list_command, "_create_backend_from_url", lambda _url: backend)
    return backend


def make_task(
    function: str = "tests.tasks:alpha",
    status: TaskStatus = "pending",
    created_offset: int = 0,
    finished_at: datetime | None = None,
    scheduled_at: datetime | None = None,
    exception: TaskException | None = None,
) -> Task:
    return Task(
        spec=TaskSpec(func=function),
        status=status,
        created_at=BASE_TIME + timedelta(minutes=created_offset),
        finished_at=finished_at,
        scheduled_at=scheduled_at,
        exception=exception,
    )


def seed_tasks(backend: InMemoryBackend, tasks: list[Task]) -> None:
    results = asyncio.run(Queue(backend, QUEUE_NAME).add(tasks))
    assert all(results)


def invoke(runner: CliRunner, *args: str):
    return runner.invoke(app, [
        "task",
        "list",
        "--backend-url",
        "memory://",
        "--queue",
        QUEUE_NAME,
        *args,
    ])


def invoke_json(runner: CliRunner, *args: str):
    result = invoke(runner, "--format", "json", *args)
    assert result.exit_code == 0, result.output or repr(result.exception)
    return json.loads(result.output)


def task_ids(payload) -> list[str]:
    return [task["id"] for task in payload["tasks"]]


def test_defaults_to_newest_first(runner, cli_backend):
    oldest = make_task(created_offset=0)
    newest = make_task(created_offset=2)
    middle = make_task(created_offset=1)
    seed_tasks(cli_backend, [oldest, newest, middle])

    payload = invoke_json(runner)

    assert payload["count"] == 3
    assert task_ids(payload) == [str(newest.id), str(middle.id), str(oldest.id)]


def test_limit_returns_the_newest_tasks(runner, cli_backend):
    tasks = [make_task(created_offset=index) for index in range(3)]
    seed_tasks(cli_backend, tasks)

    payload = invoke_json(runner, "--limit", "2")

    assert payload["count"] == 2
    assert task_ids(payload) == [str(tasks[2].id), str(tasks[1].id)]


def test_default_limit_is_100(runner, cli_backend):
    tasks = [make_task(created_offset=index) for index in range(101)]
    seed_tasks(cli_backend, tasks)

    payload = invoke_json(runner)

    assert payload["count"] == 100
    assert task_ids(payload) == [str(task.id) for task in reversed(tasks[1:])]


@pytest.mark.parametrize("limit", ["0", "-1"])
def test_limit_must_be_positive(runner, limit):
    result = invoke(runner, "--limit", limit)

    assert result.exit_code != 0
    assert "Invalid value" in result.output


@pytest.mark.parametrize("status", STATUSES)
def test_status_filter(runner, cli_backend, status):
    exception = TASK_EXCEPTION if status in ("failed", "crashed") else None
    matching = make_task(status=status, created_offset=1, exception=exception)
    other = make_task(status="pending" if status == "completed" else "completed", created_offset=2)
    seed_tasks(cli_backend, [matching, other])

    payload = invoke_json(runner, "--status", status)

    assert task_ids(payload) == [str(matching.id)]
    assert payload["tasks"][0]["queue_status"] == status


def test_failed_filter_uses_displayed_exception_status(runner, cli_backend):
    matching = make_task(status="pending", exception=TASK_EXCEPTION)
    other = make_task(status="completed", created_offset=1)
    seed_tasks(cli_backend, [matching, other])

    payload = invoke_json(runner, "--status", "failed")

    assert task_ids(payload) == [str(matching.id)]
    assert payload["tasks"][0]["queue_status"] == "failed"


def test_function_filter_accepts_fully_qualified_and_bare_names(runner, cli_backend):
    first = make_task("myapp.tasks:send_email", created_offset=1)
    second = make_task("myapp.other:send_email", created_offset=2)
    other = make_task("myapp.tasks:send_sms", created_offset=3)
    seed_tasks(cli_backend, [first, second, other])

    fully_qualified = invoke_json(runner, "--function", "myapp.tasks:send_email")
    bare = invoke_json(runner, "--function", "send_email")
    substring = invoke_json(runner, "--function", "send")

    assert task_ids(fully_qualified) == [str(first.id)]
    assert task_ids(bare) == [str(second.id), str(first.id)]
    assert substring["count"] == 0


def test_combined_filters_are_applied_before_limit(runner, cli_backend):
    matching_oldest = make_task("tests.tasks:alpha", status="pending", created_offset=1)
    matching_newest = make_task("tests.tasks:alpha", status="pending", created_offset=2)
    wrong_function = make_task("tests.tasks:beta", status="pending", created_offset=3)
    wrong_status = make_task("tests.tasks:alpha", status="completed", created_offset=4)
    seed_tasks(cli_backend, [matching_oldest, matching_newest, wrong_function, wrong_status])
    args = ("--status", "pending", "--function", "alpha", "--limit", "1")

    payload = invoke_json(runner, *args)
    table_result = invoke(runner, *args)

    assert task_ids(payload) == [str(matching_newest.id)]
    assert table_result.exit_code == 0, table_result.output
    assert "showing 1 of 2" in table_result.output


@pytest.mark.usefixtures("cli_backend")
def test_empty_table_describes_active_filters(runner):
    result = invoke(runner, "--status", "failed", "--function", "missing")
    output = " ".join(result.output.split())

    assert result.exit_code == 0
    assert "No tasks found" in output
    assert "status 'failed'" in output
    assert "function 'missing'" in output


def test_created_sort_ascending(runner, cli_backend):
    tasks = [make_task(created_offset=index) for index in range(3)]
    seed_tasks(cli_backend, tasks)

    payload = invoke_json(runner, "--sort-by", "created", "--order", "asc")

    assert task_ids(payload) == [str(task.id) for task in tasks]


@pytest.mark.parametrize("field", ["finished", "scheduled"])
def test_optional_datetime_sorting(runner, cli_backend, field):
    early = make_task(created_offset=0, **{f"{field}_at": BASE_TIME + timedelta(days=1)})
    late = make_task(created_offset=1, **{f"{field}_at": BASE_TIME + timedelta(days=2)})
    missing = make_task(created_offset=2)
    seed_tasks(cli_backend, [early, late, missing])

    ascending = invoke_json(runner, "--sort-by", field, "--order", "asc")
    descending = invoke_json(runner, "--sort-by", field, "--order", "desc")

    assert task_ids(ascending) == [str(missing.id), str(early.id), str(late.id)]
    assert task_ids(descending) == [str(late.id), str(early.id), str(missing.id)]


def test_function_sorting(runner, cli_backend):
    charlie = make_task("tests.tasks:charlie", created_offset=0)
    alpha = make_task("tests.tasks:alpha", created_offset=2)
    bravo = make_task("tests.tasks:bravo", created_offset=1)
    seed_tasks(cli_backend, [charlie, alpha, bravo])

    ascending = invoke_json(runner, "--sort-by", "function", "--order", "asc")
    descending = invoke_json(runner, "--sort-by", "function", "--order", "desc")

    assert task_ids(ascending) == [str(alpha.id), str(bravo.id), str(charlie.id)]
    assert task_ids(descending) == [str(charlie.id), str(bravo.id), str(alpha.id)]


def test_status_sorting(runner, cli_backend):
    pending = make_task(status="pending", created_offset=0)
    completed = make_task(status="completed", created_offset=2)
    failed = make_task(status="failed", created_offset=1, exception=TASK_EXCEPTION)
    seed_tasks(cli_backend, [pending, completed, failed])

    ascending = invoke_json(runner, "--sort-by", "status", "--order", "asc")
    descending = invoke_json(runner, "--sort-by", "status", "--order", "desc")

    assert task_ids(ascending) == [str(completed.id), str(failed.id), str(pending.id)]
    assert task_ids(descending) == [str(pending.id), str(failed.id), str(completed.id)]
