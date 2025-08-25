import inspect

import pytest
from pydantic import ValidationError

from sheppy import Depends
from sheppy.task import Task, TaskInternal
from sheppy.utils.argument_processing import (
    _should_skip_param,
    _validate_and_dump,
    is_task_injection_param,
    prepare_task_arguments,
    process_task_arguments,
)

from .fixtures import (
    ComplexModel,
    UserModel,
    func_mixed_params,
    func_positional,
    func_regular,
    func_with_default,
    func_with_depends,
    func_with_no_annotation,
    func_with_pydantic,
    func_with_task_self,
    func_with_task_self_string,
    func_with_var_kwargs,
    func_with_wrong_name,
    func_with_wrong_type,
)


class TestIsTaskInjectionParam:
    """Test is_task_injection_param function."""

    @pytest.mark.parametrize("func,param_name,expected", [
        (func_with_task_self, "self", True),
        (func_with_task_self_string, "self", True),
        (func_with_wrong_name, "task", False),
        (func_with_no_annotation, "self", False),
        (func_with_wrong_type, "self", False),
    ])
    def test_is_task_injection_param(self, func, param_name, expected):
        sig = inspect.signature(func)
        param = sig.parameters[param_name]
        assert is_task_injection_param(param) == expected


class TestShouldSkipParam:
    """Test _should_skip_param function."""

    @pytest.mark.parametrize("func,param_name,expected", [
        (func_with_depends, "dep", True),
        (func_with_task_self, "self", True),
        (func_regular, "arg", False),
        (func_with_default, "arg", False),
    ])
    def test_should_skip_param(self, func, param_name, expected):
        sig = inspect.signature(func)
        param = sig.parameters[param_name]
        assert _should_skip_param(param) == expected


class TestValidateAndDump:
    """Test _validate_and_dump function."""

    @pytest.mark.parametrize("value,param_type,expected", [
        # No type annotation
        ("test", inspect.Parameter.empty, "test"),
        (123, inspect.Parameter.empty, 123),

        # Pydantic model validation
        ({"name": "Alice", "age": 25}, UserModel, {"name": "Alice", "age": 25, "email": None}),
        ({"name": "Bob"}, UserModel, {"name": "Bob", "age": 18, "email": None}),

        # Basic types
        ("123", int, 123),
        ("test", str, "test"),

        # Lists
        ([1, 2, 3], list[int], [1, 2, 3]),
        (["1", "2"], list[int], [1, 2]),

        # Optional types
        (None, str | None, None),
        ("value", str | None, "value"),
    ])
    def test_validate_and_dump(self, value, param_type, expected):
        result = _validate_and_dump(value, param_type)
        assert result == expected

    def test_validate_and_dump_errors(self):
        # Missing required field
        with pytest.raises(ValidationError):
            _validate_and_dump({}, UserModel)

        # Invalid field constraint
        with pytest.raises(ValidationError):
            _validate_and_dump({"count": 0, "users": [], "metadata": {}}, ComplexModel)


class TestProcessTaskArguments:
    """Test process_task_arguments function."""

    @pytest.mark.parametrize("func,args,kwargs,expected_args,expected_kwargs", [
        # Positional arguments
        (func_positional, ("test", "42", 3.14), {}, ["test", 42, 3.14], {}),

        # Keyword arguments
        (func_positional, (), {"a": "test", "b": "25"}, [], {"a": "test", "b": 25}),

        # Pydantic models
        (func_with_pydantic, ({"name": "Alice"}, "5"), {},
         [{"name": "Alice", "age": 18, "email": None}, 5], {}),

        # Skip depends params
        (func_mixed_params, ("test",), {"b": 30}, ["test"], {"b": 30}),

        # Var kwargs support
        (func_with_var_kwargs, ("test",), {"extra1": "value1", "extra2": 123},
         ["test"], {"extra1": "value1", "extra2": 123}),
    ])
    def test_process_task_arguments(self, func, args, kwargs, expected_args, expected_kwargs):
        processed_args, processed_kwargs = process_task_arguments(func, args, kwargs)
        assert processed_args == expected_args
        assert processed_kwargs == expected_kwargs


class TestPrepareTaskArguments:
    """Test prepare_task_arguments function."""

    def test_prepare_basic_args(self):
        task = Task(
            internal=TaskInternal(args=["test", 42])
        )

        resolved_values = {"a": "test", "b": 42}

        final_args, final_kwargs = prepare_task_arguments(task, resolved_values, func_positional)

        assert final_args == ["test", 42]
        assert final_kwargs == {}

    def test_prepare_with_task_injection(self):
        task = Task(
            internal=TaskInternal(args=["value"])
        )

        def func(self: Task, a: str):
            pass

        resolved_values = {"self": task, "a": "value"}

        final_args, final_kwargs = prepare_task_arguments(task, resolved_values, func)

        assert final_args == [task, "value"]
        assert final_kwargs == {}

    def test_prepare_with_dependencies(self):
        task = Task()

        def func(a: str, dep: int = Depends()):
            pass

        resolved_values = {"a": "test", "dep": 123}

        final_args, final_kwargs = prepare_task_arguments(task, resolved_values, func)

        assert final_args == []
        assert final_kwargs == {"a": "test", "dep": 123}

    def test_prepare_reconstruct_pydantic(self):
        task = Task()

        resolved_values = {
            "user": {"name": "Alice", "age": 30, "email": "alice@example.com"}
        }

        final_args, final_kwargs = prepare_task_arguments(task, resolved_values, func_with_pydantic)

        assert "user" in final_kwargs
        assert isinstance(final_kwargs["user"], UserModel)
        assert final_kwargs["user"].name == "Alice"
        assert final_kwargs["user"].age == 30
