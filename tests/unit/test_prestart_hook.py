import pytest
from typer.testing import CliRunner

from sheppy import Worker
from sheppy._config import load_config
from sheppy.cli.cli import app

HOOK_CALLS: list[str] = []


def record_hook() -> None:
    HOOK_CALLS.append("hook")


def failing_hook() -> None:
    raise RuntimeError("prestart failed")


@pytest.fixture(autouse=True)
def clear_hook_calls():
    HOOK_CALLS.clear()


@pytest.fixture
def mock_worker_work(monkeypatch):
    async def fake_work(*_args, **_kwargs):
        HOOK_CALLS.append("work")

    monkeypatch.setattr(Worker, "work", fake_work)


@pytest.fixture
def runner():
    return CliRunner()


class TestPrestartCLI:
    def test_hook_runs_before_worker_starts(self, runner, mock_worker_work):
        result = runner.invoke(app, ["work", "--backend-url", "memory://", "--prestart", f"{__name__}:record_hook"])

        assert result.exit_code == 0
        assert HOOK_CALLS == ["hook", "work"]

    def test_worker_without_hook(self, runner, mock_worker_work):
        result = runner.invoke(app, ["work", "--backend-url", "memory://"])

        assert result.exit_code == 0
        assert HOOK_CALLS == ["work"]

    def test_failing_hook_prevents_worker_start(self, runner, mock_worker_work):
        result = runner.invoke(app, ["work", "--backend-url", "memory://", "--prestart", f"{__name__}:failing_hook"])

        assert result.exit_code != 0
        assert isinstance(result.exception, RuntimeError)
        assert HOOK_CALLS == []

    def test_unresolvable_hook_prevents_worker_start(self, runner, mock_worker_work):
        result = runner.invoke(app, ["work", "--backend-url", "memory://", "--prestart", "nonexistent.module:func"])

        assert result.exit_code != 0
        assert isinstance(result.exception, ValueError)
        assert HOOK_CALLS == []


class TestPrestartConfig:
    def test_loads_from_env(self):
        cfg = load_config({"SHEPPY_PRESTART": "myapp.hooks:init_sentry"})
        assert cfg.prestart == "myapp.hooks:init_sentry"

    def test_default_is_none(self):
        assert load_config({}).prestart is None
