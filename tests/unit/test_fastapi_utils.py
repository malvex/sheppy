import inspect
from typing import Annotated

import pytest

import sheppy._utils.fastapi as fastapi_utils
from sheppy import task


def test_public_depends_options_are_keyword_only():
    parameters = inspect.signature(fastapi_utils.Depends).parameters

    assert parameters["use_cache"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["scope"].kind is inspect.Parameter.KEYWORD_ONLY
    with pytest.raises(TypeError):
        fastapi_utils.Depends(lambda: None, False)


def test_security_dependencies_are_not_supported():
    fastapi_params = pytest.importorskip("fastapi.params")

    def dependency() -> str:
        return "value"

    @task
    def default_security_task(value: str = fastapi_params.Security(dependency)) -> str:
        return value

    @task
    def annotated_security_task(value: Annotated[str, fastapi_params.Security(dependency)]) -> str:
        return value

    for security_task in (default_security_task, annotated_security_task):
        with pytest.raises(TypeError) as exc_info:
            security_task()
        assert str(exc_info.value) == "`Security` is not supported by Sheppy; use `Depends` instead"
