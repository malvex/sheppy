import inspect

import pytest

from sheppy import Depends
from sheppy.task import Task
from sheppy.utils.dependency_injection import (
    DependencyResolver,
    get_depends_from_param,
    is_depends_param,
)

from .fixtures import (
    func_regular,
    func_with_annotated_depends,
    func_with_default,
    func_with_depends,
    get_async_generator_value,
    get_async_value,
    get_cached_value,
    get_generator_value,
    get_multiplied_value,
    get_simple_value,
)


class TestGetDependsFromParam:
    """Test get_depends_from_param function."""

    def test_depends_in_default(self):
        sig = inspect.signature(func_with_depends)
        param = sig.parameters["dep"]
        depends = get_depends_from_param(param)

        assert depends is not None
        assert depends.__class__.__name__ == "Depends"

    def test_depends_in_annotated(self):
        sig = inspect.signature(func_with_annotated_depends)
        param = sig.parameters["dep"]
        depends = get_depends_from_param(param)

        assert depends is not None
        assert depends.__class__.__name__ == "Depends"

    def test_no_depends(self):
        sig = inspect.signature(func_regular)
        param = sig.parameters["arg"]
        depends = get_depends_from_param(param)

        assert depends is None


class TestIsDependsParam:
    """Test is_depends_param function."""

    @pytest.mark.parametrize("func,param_name,expected", [
        (func_with_depends, "dep", True),
        (func_with_annotated_depends, "dep", True),
        (func_regular, "arg", False),
        (func_with_default, "arg", False),
    ])
    def test_is_depends_param(self, func, param_name, expected):
        sig = inspect.signature(func)
        param = sig.parameters[param_name]
        assert is_depends_param(param) == expected


class TestDependencyResolver:
    """Test DependencyResolver class."""

    @pytest.mark.asyncio
    async def test_simple_dependency(self):
        def func(a: str, dep: int = Depends(get_simple_value)):
            pass

        resolver = DependencyResolver()
        resolved = await resolver.solve_dependencies(func, ("test",))

        assert resolved == {"a": "test", "dep": 42}

    @pytest.mark.asyncio
    async def test_async_dependency(self):
        def func(dep: str = Depends(get_async_value)):
            pass

        resolver = DependencyResolver()
        resolved = await resolver.solve_dependencies(func)

        assert resolved == {"dep": "async_result"}

    @pytest.mark.asyncio
    async def test_nested_dependencies(self):
        def func(result: int = Depends(get_multiplied_value)):
            pass

        resolver = DependencyResolver()
        resolved = await resolver.solve_dependencies(func)

        assert resolved == {"result": 20}

    @pytest.mark.asyncio
    async def test_dependency_override(self):
        def get_original():
            return "original"

        def get_override():
            return "overridden"

        def func(dep: str = Depends(get_original)):
            pass

        resolver = DependencyResolver(dependency_overrides={get_original: get_override})
        resolved = await resolver.solve_dependencies(func)

        assert resolved == {"dep": "overridden"}

    @pytest.mark.asyncio
    async def test_dependency_caching(self):
        # Reset call count
        get_cached_value.call_count = 0

        def func(
            dep1: int = Depends(get_cached_value),
            dep2: int = Depends(get_cached_value)
        ):
            pass

        resolver = DependencyResolver()
        resolved = await resolver.solve_dependencies(func)

        # Both should have same value due to caching
        assert resolved["dep1"] == 1
        assert resolved["dep2"] == 1
        assert get_cached_value.call_count == 1

    @pytest.mark.asyncio
    async def test_generator_dependency(self):
        def func(dep: str = Depends(get_generator_value)):
            pass

        resolver = DependencyResolver()
        resolved = await resolver.solve_dependencies(func)

        assert resolved == {"dep": "generator_value"}

    @pytest.mark.asyncio
    async def test_async_generator_dependency(self):
        def func(dep: str = Depends(get_async_generator_value)):
            pass

        resolver = DependencyResolver()
        resolved = await resolver.solve_dependencies(func)

        assert resolved == {"dep": "async_generator_value"}

    @pytest.mark.asyncio
    async def test_map_positional_args(self):
        def func(a: str, b: int, c: float = 1.0):
            pass

        resolver = DependencyResolver()
        sig = inspect.signature(func)

        mapped = resolver._map_positional_args(sig, ("test", 42))

        assert mapped == {"a": "test", "b": 42}

    @pytest.mark.asyncio
    async def test_skip_special_params_in_mapping(self):
        def func(self: Task, a: str, dep: int = Depends()):
            pass

        resolver = DependencyResolver()
        sig = inspect.signature(func)

        mapped = resolver._map_positional_args(sig, ("test",))

        # Should only map 'a', skipping self and dep
        assert mapped == {"a": "test"}
