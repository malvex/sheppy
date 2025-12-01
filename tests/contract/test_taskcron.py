import pytest
from pydantic import ValidationError

from sheppy.models import TaskConfig, TaskCron, TaskSpec


@pytest.mark.parametrize("cron_expression", [
    "* * * * *",
    "1 1 1 1 1",
    "*/5 * * * *",
    "0 0 * * *",
    "0 0 * * 0",
    "0 0 * * 7",
    "0 0 1 * *",
    "0 0 1 1 *",
    "15 2 * * *",
    "0 */4 * * *",
    "0 9-17 * * 1-5",
    "0 9-17 * * mon",
    "@hourly",
    "@daily",
    "@weekly",
    "@monthly",
    "@yearly",
    "@annually",
])
def test_valid_cron_expressions(cron_expression):

    TaskCron(expression=cron_expression, spec=TaskSpec(func=""), config=TaskConfig())


@pytest.mark.parametrize("cron_expression", [
    "doggo",
    42,
    "",
    "* * * *",
    "60 * * * *",
    "* 24 * * *",
    "* * 32 * *",
    "* * * 13 *",
    "* * * * 8",
    "* * * * bob",
    "@invalid",
    "*/0 * * * *",
    None,
    3.14,
    ["* * * * *"],
    {"cron": "* * * * *"},
])
def test_invalid_cron_expressions(cron_expression):

    with pytest.raises(ValidationError):
        TaskCron(expression=cron_expression, spec=TaskSpec(func=""), config=TaskConfig())


@pytest.mark.parametrize("expression,spec,config", [
    ("* * * * *", {"func": "task.simple"}, {}),
    ("* * * * *", {"func": "module.function"}, {"retry": 3}),
    ("0 0 * * *", {"func": "daily.task", "args": [1, 2, 3]}, {}),
    ("@hourly", {"func": "process", "kwargs": {"key": "value"}}, {"retry_delay": 5.0}),
    ("*/5 * * * *", {"func": "monitor", "args": ["prod"], "kwargs": {"alert": True}}, {"retry": 1, "retry_delay": [1.0, 2.0]}),
])
def test_deterministic_id(expression, spec: dict, config: dict):

    different_expression = "*/5 9 * * *"
    different_spec = {"func": "different_func"}
    different_config = {"retry": "42"}

    tc1 = TaskCron(expression=expression, spec=spec, config=config)
    tc2 = TaskCron(expression=expression, spec=spec, config=config)

    assert tc1 != tc2
    assert tc1.id != tc2.id
    assert tc1.deterministic_id == tc2.deterministic_id

    tc_diff_expression = TaskCron(expression=different_expression, spec=spec, config=config)
    tc_diff_spec = TaskCron(expression=expression, spec=different_spec, config=config)
    tc_diff_config = TaskCron(expression=expression, spec=spec, config=different_config)

    ids = {
        tc1.id,
        tc2.id,
        tc_diff_expression.id,
        tc_diff_spec.id,
        tc_diff_config.id
    }
    assert len(ids) == 5

    deterministic_ids = {
        tc1.deterministic_id,
        tc2.deterministic_id,
        tc_diff_expression.deterministic_id,
        tc_diff_spec.deterministic_id,
        tc_diff_config.deterministic_id
    }
    assert len(deterministic_ids) == 4
