import pytest

from sheppy import Task
from sheppy._utils.task_execution import TaskProcessor
from sheppy.exceptions import TaskFailedError, WorkerCrashedError
from sheppy.models import TaskException
from tests.dependencies import CustomTaskError, simple_sync_task


class StrictError(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)


def capture(exc: BaseException) -> TaskException:
    try:
        raise exc
    except BaseException as e:
        return TaskException.from_exception(e)


class TestFromException:
    def test_captures_everything(self):
        te = capture(ValueError("bad input", 42))

        assert te.type == "ValueError"
        assert te.module == "builtins"
        assert te.message == str(ValueError("bad input", 42))
        assert te.args == ("bad input", 42)
        assert te.traceback is not None
        assert "ValueError" in te.traceback
        assert __file__.split("/")[-1] in te.traceback  # traceback should contain this file

    def test_custom_exception(self):
        te = capture(CustomTaskError("oops", 5))

        assert te.type == "CustomTaskError"
        assert te.module == "tests.dependencies"
        assert te.message == str(CustomTaskError("oops", 5))
        assert te.args == ("oops", 5)

    def test_non_serializable_args_are_replaced_with_repr(self):
        class NotSerializable:
            pass

        te = capture(ValueError(NotSerializable()))

        assert isinstance(te.args[0], str)
        assert "NotSerializable" in te.args[0]
        te.model_dump_json()  # must be JSON serializable

    def test_json_round_trip(self):
        te = capture(ValueError("bad input", 42))
        te2 = TaskException.model_validate_json(te.model_dump_json())

        assert te2 == te


class TestToException:
    def test_builtin_exception(self):
        exc = capture(ValueError("bad input", 42)).to_exception()

        assert isinstance(exc, ValueError)
        assert exc.args == ("bad input", 42)

    def test_custom_importable_exception(self):
        exc = capture(CustomTaskError("oops", 5)).to_exception()

        assert isinstance(exc, CustomTaskError)
        assert exc.args == ("oops", 5)

    def test_args_mismatch_falls_back_to_message(self):
        te = TaskException(
            type="StrictError",
            module="tests.unit.test_task_exception",
            message="some message",
            args=("not", "ints"),
        )

        exc = te.to_exception()

        assert isinstance(exc, StrictError)
        assert exc.args == ("some message",)

    def test_unimportable_module_falls_back_to_task_failed_error(self):
        te = TaskException(
            type="MyCustomError",
            module="does_not_exist_xyz",
            message="boom",
            args=("boom",),
            traceback="fake traceback",
        )

        exc = te.to_exception()

        assert isinstance(exc, TaskFailedError)
        assert exc.original_type == "does_not_exist_xyz:MyCustomError"
        assert str(exc) == "boom"
        assert exc.traceback == "fake traceback"

    def test_non_exception_class_falls_back_to_task_failed_error(self):
        te = TaskException(type="str", module="builtins", message="boom")

        exc = te.to_exception()

        assert isinstance(exc, TaskFailedError)
        assert exc.original_type == "builtins:str"


def test_str_and_repr():
    te = capture(ValueError("bad input"))

    assert str(te) == "ValueError: bad input"
    assert repr(te) == "TaskException(type='ValueError', message='bad input')"


class TestTaskProcessorIntegration:
    def test_mark_failed(self):
        task = simple_sync_task(1, 2)
        exc = ValueError("bad input")

        try:
            raise exc
        except ValueError as e:
            task = TaskProcessor.mark_failed(task, e)

        assert task.status == 'failed'
        assert task.result is None
        assert task.finished_at is not None
        assert task.exception is not None
        assert task.exception.type == "ValueError"
        assert task.exception.traceback is not None

    def test_mark_completed_clears_exception(self):
        task = simple_sync_task(1, 2)

        try:
            raise ValueError("bad input")
        except ValueError as e:
            task = TaskProcessor.mark_failed(task, e)

        assert task.exception is not None

        task = TaskProcessor.mark_completed(task, 3)

        assert task.status == 'completed'
        assert task.result == 3
        assert task.exception is None

    def test_mark_crashed_synthesizes_worker_crashed_error(self):
        task = simple_sync_task(1, 2)

        task = TaskProcessor.mark_crashed(task)

        assert task.status == 'crashed'
        assert task.exception is not None
        assert task.exception.type == "WorkerCrashedError"
        assert task.exception.module == "sheppy.exceptions"
        assert task.exception.message == "Worker crashed during execution"
        assert task.exception.traceback is None
        assert isinstance(task.exception.to_exception(), WorkerCrashedError)


class TestDeprecatedErrorShim:
    def test_error_is_none_for_new_task(self):
        task = simple_sync_task(1, 2)

        with pytest.warns(DeprecationWarning, match="task.error is deprecated"):
            assert task.error is None

    def test_error_matches_legacy_format(self):
        task = simple_sync_task(1, 2)

        try:
            raise ValueError("bad input")
        except ValueError as e:
            task = TaskProcessor.mark_failed(task, e)

        with pytest.warns(DeprecationWarning, match="task.error is deprecated"):
            assert task.error == "ValueError: bad input"

    def test_old_serialized_task_data_still_validates(self):
        task = simple_sync_task(1, 2)
        data = task.model_dump(mode="json")
        data["error"] = "ValueError: legacy"

        task2 = Task.model_validate(data)

        assert task2.exception is None
