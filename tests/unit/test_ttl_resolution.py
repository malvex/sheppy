from sheppy.backend.base import resolve_metadata_ttl
from sheppy.models import TTLValue

FINISHED = "2026-01-01T00:00:00+00:00"


def task_data(status: str, ttl: TTLValue = "inherit", error_ttl: TTLValue = "inherit", finished_at: str | None = FINISHED) -> dict:
    return {
        "status": status,
        "finished_at": finished_at,
        "config": {"ttl": ttl, "error_ttl": error_ttl},
    }


class TestResolveMetadataTtl:
    def test_unfinished_tasks_never_expire(self):
        # tasks that have not run yet (or are waiting for retry) must not lose their metadata
        for status in ("new", "scheduled", "pending", "processing", "retrying"):
            assert resolve_metadata_ttl(task_data(status, finished_at=None), ttl=100, error_ttl=10) is None
            assert resolve_metadata_ttl(task_data(status, ttl=5, error_ttl=2, finished_at=None), ttl=100, error_ttl=10) is None

    def test_missing_finished_at_is_treated_as_unfinished(self):
        task = {"status": "completed", "config": {"ttl": 5, "error_ttl": 2}}
        assert resolve_metadata_ttl(task, ttl=100, error_ttl=10) is None

    def test_no_expiry_when_nothing_configured(self):
        assert resolve_metadata_ttl(task_data("completed"), ttl=None, error_ttl="inherit") is None
        assert resolve_metadata_ttl(task_data("failed"), ttl=None, error_ttl="inherit") is None

    def test_backend_ttl_applies_to_regular_statuses(self):
        for status in ("completed", "cancelled", "unknown"):
            assert resolve_metadata_ttl(task_data(status), ttl=100, error_ttl=10) == 100

    def test_backend_error_ttl_applies_to_error_statuses(self):
        for status in ("failed", "crashed"):
            assert resolve_metadata_ttl(task_data(status), ttl=100, error_ttl=10) == 10

    def test_backend_error_ttl_inherit_falls_back_to_backend_ttl(self):
        for status in ("failed", "crashed"):
            assert resolve_metadata_ttl(task_data(status), ttl=100, error_ttl="inherit") == 100

    def test_backend_error_ttl_none_disables_expiry(self):
        for status in ("failed", "crashed"):
            assert resolve_metadata_ttl(task_data(status), ttl=100, error_ttl=None) is None

    def test_backend_ttl_none_disables_expiry(self):
        assert resolve_metadata_ttl(task_data("completed"), ttl=None, error_ttl=10) is None

    def test_task_ttl_overrides_backend_ttl(self):
        assert resolve_metadata_ttl(task_data("completed", ttl=5), ttl=100, error_ttl=10) == 5

    def test_task_ttl_none_disables_expiry(self):
        assert resolve_metadata_ttl(task_data("completed", ttl=None), ttl=100, error_ttl=10) is None

    def test_task_ttl_none_disables_expiry_on_error(self):
        # failed: task error_ttl is "inherit" -> task ttl (None) wins over backend error_ttl
        assert resolve_metadata_ttl(task_data("failed", ttl=None), ttl=100, error_ttl=10) is None

    def test_task_error_ttl_overrides_everything(self):
        assert resolve_metadata_ttl(task_data("failed", ttl=5, error_ttl=2), ttl=100, error_ttl=10) == 2

    def test_task_error_ttl_none_disables_expiry(self):
        assert resolve_metadata_ttl(task_data("failed", error_ttl=None), ttl=100, error_ttl=10) is None
        assert resolve_metadata_ttl(task_data("failed", ttl=5, error_ttl=None), ttl=100, error_ttl=10) is None

    def test_task_error_ttl_ignored_for_regular_statuses(self):
        assert resolve_metadata_ttl(task_data("completed", error_ttl=2), ttl=100, error_ttl=10) == 100

    def test_missing_config_keys_are_treated_as_inherit(self):
        # tasks serialized before TTL support existed have no ttl keys
        assert resolve_metadata_ttl({"status": "completed", "finished_at": FINISHED}, ttl=100, error_ttl=10) == 100
        assert resolve_metadata_ttl({"status": "failed", "finished_at": FINISHED}, ttl=100, error_ttl=10) == 10
        assert resolve_metadata_ttl({"status": "completed", "finished_at": FINISHED, "config": {}}, ttl=100, error_ttl=10) == 100

    def test_full_precedence_chain(self):
        # task error_ttl > task ttl > backend error_ttl > backend ttl ("inherit" levels are skipped)
        assert resolve_metadata_ttl(task_data("failed", ttl=5, error_ttl=2), ttl=100, error_ttl=10) == 2
        assert resolve_metadata_ttl(task_data("failed", ttl=5), ttl=100, error_ttl=10) == 5
        assert resolve_metadata_ttl(task_data("failed"), ttl=100, error_ttl=10) == 10
        assert resolve_metadata_ttl(task_data("failed"), ttl=100, error_ttl="inherit") == 100
