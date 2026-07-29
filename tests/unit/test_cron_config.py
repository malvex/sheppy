from sheppy._utils.cron_config import load_cron_declarations

VALID_TOML = """
[[tool.sheppy.cron]]
task = "myapp.tasks:cleanup"
expression = "0 3 * * *"
args = [30]
kwargs = {dry_run = true}
queue = "maintenance"

[[tool.sheppy.cron]]
task = "myapp.tasks:ping"
expression = "*/5 * * * *"
"""


def test_load_valid_declarations(tmp_path):
    path = tmp_path / "pyproject.toml"
    path.write_text(VALID_TOML)

    declarations = load_cron_declarations(path)

    assert declarations is not None
    assert len(declarations) == 2

    first, second = declarations
    assert first.task == "myapp.tasks:cleanup"
    assert first.expression == "0 3 * * *"
    assert first.args == (30,)
    assert first.kwargs == {"dry_run": True}
    assert first.queue == "maintenance"

    assert second.task == "myapp.tasks:ping"
    assert second.args == ()
    assert second.kwargs == {}
    assert second.queue is None


def test_missing_file_returns_none(tmp_path):
    assert load_cron_declarations(tmp_path / "nope.toml") is None


def test_invalid_toml_returns_none(tmp_path):
    path = tmp_path / "pyproject.toml"
    path.write_text("[[[not toml")
    assert load_cron_declarations(path) is None


def test_cron_section_not_a_list_returns_none(tmp_path):
    path = tmp_path / "pyproject.toml"
    path.write_text('[tool.sheppy]\ncron = "nope"\n')
    assert load_cron_declarations(path) is None


def test_no_cron_section_returns_empty_list(tmp_path):
    path = tmp_path / "pyproject.toml"
    path.write_text('[project]\nname = "something"\n')
    assert load_cron_declarations(path) == []


def test_invalid_entries_are_skipped(tmp_path):
    path = tmp_path / "pyproject.toml"
    path.write_text("""
[[tool.sheppy.cron]]
task = "myapp.tasks:ok"
expression = "* * * * *"

[[tool.sheppy.cron]]
expression = "* * * * *"

[[tool.sheppy.cron]]
task = "not-a-module-path"
expression = "* * * * *"

[[tool.sheppy.cron]]
task = "myapp.tasks:no_expression"

[[tool.sheppy.cron]]
task = "myapp.tasks:bad_args"
expression = "* * * * *"
args = "not-a-list"

[[tool.sheppy.cron]]
task = "myapp.tasks:bad_kwargs"
expression = "* * * * *"
kwargs = [1, 2]

[[tool.sheppy.cron]]
task = "myapp.tasks:bad_queue"
expression = "* * * * *"
queue = 5
""")

    declarations = load_cron_declarations(path)

    assert declarations is not None
    assert [d.task for d in declarations] == ["myapp.tasks:ok"]
