import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Optional

from pydantic import BaseModel

from sheppy import Depends, Task, task


class User(BaseModel):
    id: int
    name: str
    hashed_password: str | None = None
    email: str | None = None


class Status(BaseModel):
    code: int
    message: str


class Product(BaseModel):
    name: str
    price: int


class Order(BaseModel):
    id: int
    product: Product
    user: User
    quantity: int


# FastAPI dependencies

def get_config() -> dict[str, Any]:
    return {"debug": True, "env": "test"}


async def get_async_config() -> dict[str, Any]:
    await asyncio.sleep(0.001)
    return {"debug": False, "env": "production"}


def sync_dep_generator():
    yield {"foo": "bar"}

async def async_dep_generator():
    yield {"bar": "baz"}


def get_nested_dependency(config: dict[str, Any] = Depends(get_config)) -> dict[str, Any]:
    return {"service": "TestService", "config": config}


# Basic tasks
@task
def simple_sync_task_no_param() -> int:
    return 42


@task
async def simple_async_task_no_param() -> int:
    await asyncio.sleep(0.001)
    return 42


@task
def simple_sync_task(x: int, y: int) -> int:
    return x + y


@task
async def simple_async_task(x: int, y: int) -> int:
    await asyncio.sleep(0.001)
    return x + y


@task(retry=1, retry_delay=1)
async def add_retriable(x: int, y: int) -> int:
    await asyncio.sleep(0.001)
    return x + y


@task
def failing_task(message: str = "Task failed") -> None:
    raise ValueError(message)


@task
async def async_failing_task(message: str = "Async task failed") -> None:
    await asyncio.sleep(0.001)
    raise ValueError(message)


# Tasks with dependencies
@task
def task_with_dependency(
    value: int,
    config: dict[str, Any] = Depends(get_config)
) -> dict[str, Any]:
    return {"value": value, "config": config}


@task
async def async_task_with_dependency(
    value: int,
    config: dict[str, Any] = Depends(get_async_config)
) -> dict[str, Any]:
    await asyncio.sleep(0.001)
    return {"value": value, "config": config}


@task
def task_with_nested_dependencies(
    value: int,
    service: dict[str, Any] = Depends(get_nested_dependency)
) -> dict[str, Any]:
    return {"value": value, "service": service}


# Tasks with Pydantic models
@task
def task_with_pydantic_model(user: User) -> Status:
    return Status(
        code=200,
        message=f"Processed user {user.name}"
    )


@task
async def async_task_with_pydantic_model(user: User) -> Status:
    await asyncio.sleep(0.001)
    return Status(
        code=200,
        message=f"Processed user {user.name}"
    )


@task
def task_with_self(self: Task, x: int, y: int) -> dict[str, Any]:
    return {"task_id": self.id, "result": x + y}


@task
def task_with_self_middle(x: int, self: Task, y: int) -> dict[str, Any]:
    return {"task_id": self.id, "result": x + y}


@task
def task_with_self_end(x: int, y: int, self: Task) -> dict[str, Any]:
    return {"task_id": self.id, "result": x + y}


@task
def task_with_nested_models(order: Order) -> dict[str, int]:
    return {
        "total": order.product.price * order.quantity
    }


@task
def task_with_list_of_models(users: list[User]) -> list[str]:
    return [u.name for u in users]


@task
def task_with_optional_model(required: str, user: User | None = None) -> str:
    if user:
        return f"{required}: {user.name}"
    return f"{required}: no user"


# Mixed tasks (Pydantic + Dependencies)
@task
def task_with_model_and_dependency(
    user: User,
    config: dict[str, Any] = Depends(get_config)
) -> dict[str, Any]:
    return {
        "user": user.name,
        "debug": config["debug"]
    }


@task
async def async_task_with_model_and_dependency(
    product: Product,
    config: dict[str, Any] = Depends(get_async_config)
) -> dict[str, Any]:
    await asyncio.sleep(0.001)
    return {
        "product": product.name,
        "price": product.price,
        "env": config["env"]
    }


# Recursive Models for Deep Recursion Testing

class SelfNode(BaseModel):
    """Self-referential node for testing deep recursion."""
    value: int
    next: Optional['SelfNode'] = None


class NodeA(BaseModel):
    """First node type for mutual recursion testing."""
    value: int
    b_node: Optional['NodeB'] = None


class NodeB(BaseModel):
    """Second node type for mutual recursion testing."""
    value: int
    a_node: Optional['NodeA'] = None


SelfNode.model_rebuild()
NodeA.model_rebuild()
NodeB.model_rebuild()


# Tasks for Deep Recursion Testing

@task
def process_self_chain(root: SelfNode) -> dict[str, Any]:
    """Process a self-referential chain and extract statistics."""
    depth = 0
    values = []
    current: SelfNode | None = root

    while current is not None:
        depth += 1
        values.append(current.value)
        current = current.next

    return {
        "depth": depth,
        "sum": sum(values),
        "final_value": values[-1] if values else None,
        "all_values": values
    }


@task
def process_mutual_chain(root: NodeA) -> dict[str, Any]:
    """Process a mutually recursive chain of NodeA and NodeB."""
    depth = 0
    a_values: list[int] = []
    b_values: list[int] = []
    current_a: NodeA | None = root

    while True:
        if current_a is not None:
            depth += 1
            a_values.append(current_a.value)
            if current_a.b_node is None:
                break
            current_b = current_a.b_node
        else:
            break

        depth += 1
        b_values.append(current_b.value)
        if current_b.a_node is None:
            break
        current_a = current_b.a_node

    all_values = []
    for i in range(max(len(a_values), len(b_values))):
        if i < len(a_values):
            all_values.append(a_values[i])
        if i < len(b_values):
            all_values.append(b_values[i])

    return {
        "depth": depth,
        "a_count": len(a_values),
        "b_count": len(b_values),
        "final_value": all_values[-1] if all_values else None,
        "all_values": all_values
    }



@dataclass
class TaskTestCase:
    """Encapsulates a test case for a task."""
    name: str  # descriptive name for the test
    func: Callable[..., Any]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] | None = None
    expected_result: Any = None
    should_fail: bool = False
    expected_error: str | None = None

    def __post_init__(self) -> None:
        if self.kwargs is None:
            self.kwargs = {}

    def create_task(self) -> Task:
        """Create the task instance."""
        kwargs = self.kwargs if self.kwargs is not None else {}
        return self.func(*self.args, **kwargs)  # type: ignore[no-any-return]


# define test data
product1 = Product(name="Item", price=4)
user1 = User(id=1, name="Alice")
user2 = User(id=2, name="Bob")
order1 = Order(id=10, product=product1, user=user1, quantity=6)


class TaskTestCases:
    """Collection of all task test cases organized by category."""

    @staticmethod
    def simple_tasks() -> list[TaskTestCase]:
        """Basic task tests without dependencies."""
        return [
            TaskTestCase("sync_no_param", simple_sync_task_no_param, expected_result=42),
            TaskTestCase("async_no_param", simple_async_task_no_param, expected_result=42),
            TaskTestCase("sync_with_args", simple_sync_task, (1, 2), expected_result=3),
            TaskTestCase("async_with_args", simple_async_task, (2, 4), expected_result=6),
            TaskTestCase("sync_mixed_args", simple_sync_task, (1,), {"y": 2}, expected_result=3),
            TaskTestCase("async_mixed_args", simple_async_task, (2,), {"y": 4}, expected_result=6),
            TaskTestCase("sync_kwargs_only", simple_sync_task, kwargs={"x": 1, "y": 2}, expected_result=3),
            TaskTestCase("async_kwargs_only", simple_async_task, kwargs={"x": 2, "y": 4}, expected_result=6),
        ]

    @staticmethod
    def type_conversion_tasks() -> list[TaskTestCase]:
        """Tasks testing automatic type conversion."""
        return [
            TaskTestCase("sync_string_args", simple_sync_task, ("1", "2"), expected_result=3),
            TaskTestCase("async_string_args", simple_async_task, ("2", "4"), expected_result=6),
            TaskTestCase("sync_mixed_types", simple_sync_task, (1, "2"), expected_result=3),
            TaskTestCase("async_mixed_types", simple_async_task, (2, "4"), expected_result=6),
        ]

    @staticmethod
    def dependency_tasks() -> list[TaskTestCase]:
        """Tasks with dependency injection."""
        return [
            TaskTestCase("sync_dependency", task_with_dependency, (42,), expected_result={"value": 42, "config": {"debug": True, "env": "test"}}),
            TaskTestCase("async_dependency", async_task_with_dependency, (69,), expected_result={"value": 69, "config": {"debug": False, "env": "production"}}),
            TaskTestCase("nested_dependencies", task_with_nested_dependencies, (420,), expected_result={"value": 420, "service": {"service": "TestService", "config": {"debug": True, "env": "test"}}}),
        ]

    @staticmethod
    def pydantic_tasks() -> list[TaskTestCase]:
        """Tasks with Pydantic models."""
        return [
            TaskTestCase("pydantic_model", task_with_pydantic_model, (user1,), expected_result=Status(code=200, message="Processed user Alice")),
            TaskTestCase("async_pydantic_model", async_task_with_pydantic_model, (user1,), expected_result=Status(code=200, message="Processed user Alice")),
            TaskTestCase("nested_models", task_with_nested_models, (order1,), expected_result={"total": 24}),
            TaskTestCase("list_of_models", task_with_list_of_models, ([user1, user2],), expected_result=["Alice", "Bob"]),
        ]

    @staticmethod
    def optional_args_tasks() -> list[TaskTestCase]:
        """Tasks with optional arguments."""
        return [
            TaskTestCase("optional_with_value", task_with_optional_model, ("user", user1), expected_result="user: Alice"),
            TaskTestCase("optional_with_none", task_with_optional_model, ("user", None), expected_result="user: no user"),
            TaskTestCase("optional_omitted", task_with_optional_model, ("user",), expected_result="user: no user"),
        ]

    @staticmethod
    def deep_recursion_tasks() -> list[TaskTestCase]:
        """Tasks testing deep recursion with complex nested models."""

        # Build 10-level deep self-referential chain
        self_chain_10: dict[str, Any] = {"value": 999, "next": None}
        for i in range(9, 0, -1):
            self_chain_10 = {"value": i * 100, "next": self_chain_10}

        # Build 5-level deep self-referential chain
        self_chain_5: dict[str, Any] = {"value": 500, "next": None}
        for i in range(4, 0, -1):
            self_chain_5 = {"value": i * 100, "next": self_chain_5}

        # Build mutually recursive chain (A->B->A->B...->B)
        # A1(100)->B1(100)->A2(200)->B2(200)->...->A5(500)->B5(777)
        mutual_chain = {
            "value": 100,
            "b_node": {
                "value": 100,
                "a_node": {
                    "value": 200,
                    "b_node": {
                        "value": 200,
                        "a_node": {
                            "value": 300,
                            "b_node": {
                                "value": 300,
                                "a_node": {
                                    "value": 400,
                                    "b_node": {
                                        "value": 400,
                                        "a_node": {
                                            "value": 500,
                                            "b_node": {
                                                "value": 777,
                                                "a_node": None
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return [
            TaskTestCase(
                "self_ref_10_levels",
                process_self_chain,
                (self_chain_10,),
                expected_result={
                    "depth": 10,
                    "sum": sum([100, 200, 300, 400, 500, 600, 700, 800, 900, 999]),
                    "final_value": 999,
                    "all_values": [100, 200, 300, 400, 500, 600, 700, 800, 900, 999]
                }
            ),
            TaskTestCase(
                "self_ref_5_levels",
                process_self_chain,
                (self_chain_5,),
                expected_result={
                    "depth": 5,
                    "sum": sum([100, 200, 300, 400, 500]),
                    "final_value": 500,
                    "all_values": [100, 200, 300, 400, 500]
                }
            ),
            TaskTestCase(
                "mutual_recursion_10_levels",
                process_mutual_chain,
                (mutual_chain,),
                expected_result={
                    "depth": 10,
                    "a_count": 5,
                    "b_count": 5,
                    "final_value": 777,
                    "all_values": [100, 100, 200, 200, 300, 300, 400, 400, 500, 777]
                }
            ),
            # Single node test
            TaskTestCase(
                "self_ref_single_node",
                process_self_chain,
                ({"value": 42, "next": None},),
                expected_result={
                    "depth": 1,
                    "sum": 42,
                    "final_value": 42,
                    "all_values": [42]
                }
            ),
            # Empty mutual recursion (A node with no B node)
            TaskTestCase(
                "mutual_recursion_single_node",
                process_mutual_chain,
                ({"value": 100, "b_node": None},),
                expected_result={
                    "depth": 1,
                    "a_count": 1,
                    "b_count": 0,
                    "final_value": 100,
                    "all_values": [100]
                }
            ),
        ]

    @staticmethod
    def self_referencing_tasks() -> list[TaskTestCase]:
        """Tasks that should fail."""
        return [
            TaskTestCase("task_with_self", task_with_self, (22, 33), expected_result=55),
            TaskTestCase("task_with_self_middle", task_with_self_middle, (22, 33), expected_result=55),
            TaskTestCase("task_with_self_end", task_with_self_end, (22, 33), expected_result=55),
        ]

    @staticmethod
    def failing_tasks() -> list[TaskTestCase]:
        """Tasks that should fail."""
        return [
            TaskTestCase(
                "sync_failure",
                failing_task,
                kwargs={"message": "Test failure"},
                should_fail=True,
                expected_error="ValueError: Test failure"
            ),
            TaskTestCase(
                "async_failure",
                async_failing_task,
                kwargs={"message": "Async test failure"},
                should_fail=True,
                expected_error="ValueError: Async test failure"
            ),
        ]

    @staticmethod
    def all_successful_tasks() -> list[TaskTestCase]:
        """Get all tasks that should succeed."""
        return (
            TaskTestCases.simple_tasks() +
            TaskTestCases.type_conversion_tasks() +
            TaskTestCases.dependency_tasks() +
            TaskTestCases.pydantic_tasks() +
            TaskTestCases.optional_args_tasks() +
            TaskTestCases.deep_recursion_tasks()
        )

    @staticmethod
    def subset_successful_tasks() -> list[TaskTestCase]:
        """Get subset of tasks that should succeed."""
        return (
            TaskTestCases.simple_tasks()[:2] +
            TaskTestCases.type_conversion_tasks()[:2] +
            TaskTestCases.dependency_tasks()[:2] +
            TaskTestCases.pydantic_tasks()[:2] +
            TaskTestCases.optional_args_tasks()[:2] +
            TaskTestCases.deep_recursion_tasks()[:2]
        )


# middleware testing dependencies

class WrappedNumber(BaseModel):
    result: Any
    extra: str = "hello"


def middleware_noop(task: Task):
    task = yield task
    return task

# todo!
# async def async_middleware_noop(task: Task):
#     task = yield task
#     return task

def middleware_noop_no_val(task: Task):  # noqa: ARG001
    yield
    return

def middleware_noop_return_no_val(task: Task):
    yield task
    return

def middleware_noop_yield_no_val(task: Task):
    task = yield
    return task

def middleware_noop_pass(task: Task):  # noqa: ARG001
    pass

def middleware_noop_yield_only_no_val(task: Task):  # noqa: ARG001
    yield

# should fail (tbd: more wrong formats)
def middleware_noop_return_only_no_val(task: Task):  # noqa: ARG001
    return

def middleware_no_args():
    yield
    return

def middleware_too_many_args(task: Task, another_task: Task):  # noqa: ARG001
    yield
    return

def middleware_change_arg(task: Task):
    task.spec.__dict__["args"][0] = 5
    yield task

def middleware_change_return_value(task: Task):
    returning_task = yield task
    returning_task.__dict__["result"] += 100000
    return returning_task

def middleware_change_return_type(task: Task):
    returning_task = yield task
    wanted_value = WrappedNumber
    aaa = f"{wanted_value.__module__}.{wanted_value.__qualname__}"  # ! FIXME - must be done automagically
    returning_task.spec.__dict__["return_type"] = aaa

    returning_task.__dict__["result"] = {"result": returning_task.result, "extra": "hi from middleware"}
    return returning_task


@task(middleware=[middleware_noop])
def task_add_with_middleware_noop(x: int, y: int) -> int:
    return x + y

@task(middleware=[middleware_change_arg])
def task_add_with_middleware_change_arg(x: int, y: int) -> int:
    return x + y

@task(middleware=[middleware_change_return_value])
def task_add_with_middleware_change_return_value(x: int, y: int) -> int:
    return x + y

@task(middleware=[middleware_noop, middleware_change_arg, middleware_change_return_value])
def task_add_with_middleware_multiple(x: int, y: int) -> int:
    return x + y

@task(middleware=[middleware_change_return_type, middleware_change_return_value])
def task_add_with_middleware_change_return_type(x: int, y: int) -> int:
    return x + y
