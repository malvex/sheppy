"""
Shared fixtures and test data for utils tests.
"""
import asyncio
from typing import Annotated, Any

from pydantic import BaseModel, Field

from sheppy import Depends
from sheppy.task import Task

# ============================================================================
# Test Models
# ============================================================================

class UserModel(BaseModel):
    name: str
    age: int = 18
    email: str | None = None


class ComplexModel(BaseModel):
    users: list[UserModel]
    metadata: dict[str, Any]
    count: int = Field(gt=0)


# ============================================================================
# Test Functions for Dynamic Testing
# ============================================================================

def func_with_task_self(self: Task):
    pass

def func_with_task_self_string(self: 'Task'):
    pass

def func_with_wrong_name(task: Task):
    pass

def func_with_no_annotation(self):
    pass

def func_with_wrong_type(self: str):
    pass

def func_with_depends(dep: int = Depends()):
    pass

def func_with_annotated_depends(dep: Annotated[int, Depends()]):
    pass

def func_regular(arg: str):
    pass

def func_with_default(arg=None):
    pass

def func_positional(a: str, b: int, c: float | None = None):
    pass

def func_with_pydantic(user: UserModel, count: int):
    pass

def func_with_var_kwargs(a: str, **kwargs):
    pass

def func_mixed_params(self: Task, a: str, dep: int = Depends(), b: int = 20):
    pass


# ============================================================================
# Dependency functions
# ============================================================================

def get_simple_value():
    return 42

async def get_async_value():
    await asyncio.sleep(0.001)
    return "async_result"

def get_base_value():
    return 10

def get_multiplied_value(base: int = Depends(get_base_value)):
    return base * 2

def get_cached_value():
    global _call_count
    _call_count = getattr(get_cached_value, 'call_count', 0) + 1
    get_cached_value.call_count = _call_count
    return _call_count

def get_generator_value():
    yield "generator_value"

async def get_async_generator_value():
    yield "async_generator_value"
