import pytest

from sheppy._utils import worker_thread


@pytest.mark.parametrize(
    "runner",
    [worker_thread._run_in_worker_thread, worker_thread._run_context_exit_in_worker_thread],
)
async def test_worker_thread_falls_back_to_asyncio(runner, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_thread, "anyio", None)

    result = await runner(lambda: 42)

    assert result == 42


@pytest.mark.skipif(worker_thread.anyio is None, reason="AnyIO is not installed")
async def test_context_exit_uses_dedicated_limiter(monkeypatch: pytest.MonkeyPatch) -> None:
    assert worker_thread.anyio is not None
    captured = {}

    async def run_sync(fn, **kwargs):
        captured.update(kwargs)
        return fn()

    monkeypatch.setattr(worker_thread.anyio.to_thread, "run_sync", run_sync)

    result = await worker_thread._run_context_exit_in_worker_thread(lambda: 42)

    assert result == 42
    limiter = captured["limiter"]
    assert isinstance(limiter, worker_thread.anyio.CapacityLimiter)
    assert limiter.total_tokens == 1
