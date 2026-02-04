import pytest
from pydantic import ValidationError

from sheppy._config import (
    DEFAULT_LOG_LEVEL,
    DEFAULT_MAX_CONCURRENT_TASKS,
    DEFAULT_QUEUE,
    DEFAULT_SHUTDOWN_TIMEOUT,
    EnvConfig,
    load_config,
)


class TestEnvConfig:
    def test_custom_values(self):
        cfg = EnvConfig(
            backend_url="redis://localhost:6379",
            queue="myqueue",
            max_concurrent_tasks=20,
            shutdown_timeout=60.0,
            log_level="debug",
        )
        assert cfg.backend_url == "redis://localhost:6379"
        assert cfg.queue == "myqueue"
        assert cfg.max_concurrent_tasks == 20
        assert cfg.shutdown_timeout == 60.0
        assert cfg.log_level == "debug"

    def test_queue_list_single(self):
        cfg = EnvConfig(queue="myqueue")
        assert cfg.queue_list == ["myqueue"]

    def test_queue_list_multiple(self):
        cfg = EnvConfig(queue="q1,q2,q3")
        assert cfg.queue_list == ["q1", "q2", "q3"]

    def test_queue_list_filters_empty(self):
        cfg = EnvConfig(queue="q1,,q2")
        assert cfg.queue_list == ["q1", "q2"]

    def test_log_level_case_insensitive(self):
        cfg = EnvConfig(log_level="DEBUG")
        assert cfg.log_level == "debug"

    def test_validation_max_concurrent_tasks_zero(self):
        with pytest.raises(ValidationError):
            EnvConfig(max_concurrent_tasks=0)

    def test_validation_max_concurrent_tasks_negative(self):
        with pytest.raises(ValidationError):
            EnvConfig(max_concurrent_tasks=-1)

    def test_validation_shutdown_timeout_negative(self):
        with pytest.raises(ValidationError):
            EnvConfig(shutdown_timeout=-1)

    def test_validation_shutdown_timeout_zero_allowed(self):
        cfg = EnvConfig(shutdown_timeout=0)
        assert cfg.shutdown_timeout == 0.0

    def test_validation_log_level_invalid(self):
        with pytest.raises(ValidationError):
            EnvConfig(log_level="invalid")

    def test_validation_max_concurrent_tasks_invalid_string(self):
        with pytest.raises(ValidationError):
            EnvConfig(max_concurrent_tasks="invalid")  # type: ignore[arg-type]


class TestLoadConfig:
    def test_loads_from_env(self):
        environ = {
            "SHEPPY_BACKEND_URL": "redis://test:6379",
            "SHEPPY_QUEUE": "testqueue",
            "SHEPPY_MAX_CONCURRENT_TASKS": "5",
            "SHEPPY_SHUTDOWN_TIMEOUT": "15.5",
            "SHEPPY_LOG_LEVEL": "warning",
        }

        cfg = load_config(environ)
        assert cfg.backend_url == "redis://test:6379"
        assert cfg.queue == "testqueue"
        assert cfg.max_concurrent_tasks == 5
        assert cfg.shutdown_timeout == 15.5
        assert cfg.log_level == "warning"

    def test_uses_defaults_when_not_set(self):
        cfg = load_config({})
        assert cfg.backend_url is None
        assert cfg.queue == DEFAULT_QUEUE
        assert cfg.max_concurrent_tasks == DEFAULT_MAX_CONCURRENT_TASKS
        assert cfg.shutdown_timeout == DEFAULT_SHUTDOWN_TIMEOUT
        assert cfg.log_level == DEFAULT_LOG_LEVEL

    def test_raises_on_invalid_max_concurrent(self):
        with pytest.raises(ValidationError):
            load_config({"SHEPPY_MAX_CONCURRENT_TASKS": "invalid"})

    def test_raises_on_invalid_log_level(self):
        with pytest.raises(ValidationError):
            load_config({"SHEPPY_LOG_LEVEL": "invalid"})
