import pytest

import sheppy._config
from sheppy import Queue
from sheppy._config import EnvConfig


@pytest.fixture
def mock_config(monkeypatch):
    def _mock_config(**kwargs):
        cfg = EnvConfig(**kwargs)
        monkeypatch.setattr(sheppy._config, "config", cfg)
        # also patch in modules that already imported it
        monkeypatch.setattr("sheppy.queue.config", cfg)
        return cfg
    return _mock_config


class TestQueueEnvVars:
    def test_queue_requires_backend(self, mock_config):
        mock_config(backend_url=None)
        with pytest.raises(ValueError, match="No backend provided"):
            Queue()

    def test_queue_uses_backend_url_from_config(self, mock_config):
        mock_config(backend_url="memory://")
        queue = Queue()
        assert queue.backend is not None
        assert queue.name == "default"

    def test_queue_uses_queue_name_from_config(self, mock_config):
        mock_config(backend_url="memory://", queue="myqueue")
        queue = Queue()
        assert queue.name == "myqueue"

    def test_queue_explicit_name_overrides_config(self, mock_config):
        mock_config(backend_url="memory://", queue="envqueue")
        queue = Queue(name="explicit")
        assert queue.name == "explicit"

    def test_queue_explicit_backend_overrides_config(self, mock_config):
        mock_config(backend_url="redis://should-not-be-used:6379")
        queue = Queue(backend="memory://")
        assert queue.backend is not None


# todo
# class TestWorkerEnvVars:
#     def test_worker_requires_backend(self, mock_config):
#         mock_config(backend_url=None)
#         with pytest.raises(ValueError, match="No backend provided"):
#             Worker()
