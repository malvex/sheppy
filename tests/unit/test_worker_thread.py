import pytest

from sheppy._utils import task_execution


async def test_worker_thread_falls_back_to_asyncio(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(task_execution, "anyio", None)

    result = await task_execution._run_in_worker_thread(lambda: 42)

    assert result == 42
