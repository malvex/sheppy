import asyncio
import contextlib
from pathlib import Path

from sheppy import Queue, Worker
from sheppy._backend.base import Backend
from tests.conftest import TEST_QUEUE_NAME
from tests.dependencies import simple_sync_task, simple_sync_task_no_param

DECLARED_TOML = """
[[tool.sheppy.cron]]
task = "tests.dependencies:simple_sync_task"
expression = "0 3 * * *"
args = [1, 2]

[[tool.sheppy.cron]]
task = "tests.dependencies:simple_sync_task_no_param"
expression = "*/5 * * * *"
"""

DECLARED_TOML_REDUCED = """
[[tool.sheppy.cron]]
task = "tests.dependencies:simple_sync_task_no_param"
expression = "*/5 * * * *"
"""


def _make_worker(backend: Backend, cron_config_file: str) -> Worker:
    worker = Worker(TEST_QUEUE_NAME, backend, enable_job_processing=False, enable_scheduler=False)
    worker._cron_config_file = cron_config_file
    worker._cron_polling_interval = 0.001
    return worker


async def _run_worker_briefly(worker: Worker, seconds: float = 0.1) -> None:
    worker_task = asyncio.create_task(worker.work())
    await asyncio.sleep(seconds)
    worker_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await worker_task


async def test_declared_crons_are_created_and_reconciled(queue: Queue, worker_backend: Backend, tmp_path: Path) -> None:
    config_file = tmp_path / "pyproject.toml"
    config_file.write_text(DECLARED_TOML)

    worker = _make_worker(worker_backend, str(config_file))
    await _run_worker_briefly(worker)

    crons = await queue.get_crons()
    assert len(crons) == 2
    assert all(cron.managed_by == "pyproject" for cron in crons)
    functions = {cron.spec.func for cron in crons}
    assert functions == {"tests.dependencies:simple_sync_task", "tests.dependencies:simple_sync_task_no_param"}

    # drop one entry from the file. The next cycle must remove only that cron
    config_file.write_text(DECLARED_TOML_REDUCED)

    worker = _make_worker(worker_backend, str(config_file))
    await _run_worker_briefly(worker)

    crons = await queue.get_crons()
    assert [cron.spec.func for cron in crons] == ["tests.dependencies:simple_sync_task_no_param"]


async def test_programmatic_crons_survive_reconciliation(queue: Queue, worker_backend: Backend, tmp_path: Path) -> None:
    config_file = tmp_path / "pyproject.toml"
    config_file.write_text(DECLARED_TOML)

    # programmatic cron for a task that is not declared in the file
    await queue.add_cron(simple_sync_task(5, 5), "0 12 * * *")

    # programmatic cron identical to a declaration: it must not be hijacked or deleted
    await queue.add_cron(simple_sync_task(1, 2), "0 3 * * *")

    worker = _make_worker(worker_backend, str(config_file))
    await _run_worker_briefly(worker)

    crons = {(cron.spec.func, cron.spec.args): cron for cron in await queue.get_crons()}
    assert len(crons) == 3

    # the identical one stays programmatic
    identical = crons[("tests.dependencies:simple_sync_task", (1, 2))]
    assert identical.managed_by is None

    # now remove the declaration from the file. The programmatic twin must survive
    config_file.write_text("""
[[tool.sheppy.cron]]
task = "tests.dependencies:simple_sync_task_no_param"
expression = "*/5 * * * *"
""")

    worker = _make_worker(worker_backend, str(config_file))
    await _run_worker_briefly(worker)

    crons = {(cron.spec.func, cron.spec.args): cron for cron in await queue.get_crons()}
    assert ("tests.dependencies:simple_sync_task", (1, 2)) in crons
    assert ("tests.dependencies:simple_sync_task", (5, 5)) in crons


async def test_missing_config_file_keeps_state(queue: Queue, worker_backend: Backend, tmp_path: Path) -> None:
    await queue.add_cron(simple_sync_task_no_param(), "*/5 * * * *")

    worker = _make_worker(worker_backend, str(tmp_path / "does-not-exist.toml"))
    await _run_worker_briefly(worker)

    assert len(await queue.get_crons()) == 1
