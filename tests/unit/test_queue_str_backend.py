import pytest

from sheppy import Queue
from sheppy.backend import LocalBackend, MemoryBackend, RedisBackend


class TestBackendInference:
    def test_backend_instance_passthrough(self):
        backend = MemoryBackend()
        queue = Queue(backend)
        assert queue.backend is backend

    def test_redis_url(self):
        queue = Queue("redis://localhost:6379")
        assert isinstance(queue.backend, RedisBackend)
        assert queue.backend.url == "redis://localhost:6379"

    def test_redis_url_with_path(self):
        queue = Queue("redis://localhost:6379/0")
        assert isinstance(queue.backend, RedisBackend)
        assert queue.backend.url == "redis://localhost:6379/0"

    def test_rediss_url(self):
        queue = Queue("rediss://localhost:6379")
        assert isinstance(queue.backend, RedisBackend)
        assert queue.backend.url == "rediss://localhost:6379"

    def test_local_url(self):
        queue = Queue("local://127.0.0.1:17420")
        assert isinstance(queue.backend, LocalBackend)
        assert queue.backend._host == "127.0.0.1"
        assert queue.backend._port == 17420

    def test_local_url_default_port(self):
        queue = Queue("local://127.0.0.1")
        assert isinstance(queue.backend, LocalBackend)
        assert queue.backend._host == "127.0.0.1"
        assert queue.backend._port == 17420

    def test_memory_url(self):
        queue = Queue("memory://")
        assert isinstance(queue.backend, MemoryBackend)

    def test_unknown_protocol_raises(self):
        with pytest.raises(ValueError, match="Unsupported backend protocol"):
            Queue("unknown://localhost")

    def test_invalid_url_raises(self):
        with pytest.raises(ValueError, match="Invalid backend URL"):
            Queue("not-a-valid-url")

    def test_queue_name_with_string_backend(self):
        queue = Queue("memory://", name="my-queue")
        assert queue.name == "my-queue"
        assert isinstance(queue.backend, MemoryBackend)
