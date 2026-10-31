import pytest

from sheppy import Queue
from sheppy._backend.memory import MemoryBackend
from sheppy._backend.redis import RedisBackend


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

    def test_redis_backend_kwargs(self):
        queue = Queue(
            "redis://localhost:6379",
            backend_kwargs={"ttl": 5, "error_ttl": 60, "consumer_group": "custom"},
        )
        assert isinstance(queue.backend, RedisBackend)
        assert queue.backend.ttl == 5
        assert queue.backend.error_ttl == 60
        assert queue.backend.consumer_group == "custom"

    def test_memory_backend_kwargs(self):
        queue = Queue("memory://", backend_kwargs={"ttl": 5, "error_ttl": 60})
        assert isinstance(queue.backend, MemoryBackend)
        assert queue.backend.ttl == 5
        assert queue.backend.error_ttl == 60

    def test_backend_kwargs_reject_backend_instance(self):
        with pytest.raises(ValueError, match="backend_kwargs can only be used"):
            Queue(MemoryBackend(), backend_kwargs={"ttl": 5})

    def test_backend_kwargs_reject_url_option(self):
        with pytest.raises(ValueError, match="cannot contain 'url'"):
            Queue("memory://", backend_kwargs={"url": "redis://localhost:6379"})

    def test_backend_kwargs_reject_invalid_ttl(self):
        with pytest.raises(ValueError, match="positive integer"):
            Queue("memory://", backend_kwargs={"ttl": 0})
        with pytest.raises(ValueError, match="positive integer"):
            Queue("redis://localhost:6379", backend_kwargs={"error_ttl": -1})
