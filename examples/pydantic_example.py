from pydantic import BaseModel
from sheppy import task

class UserData(BaseModel):
    name: str
    email: str
    age: int

class ProcessResult(BaseModel):
    user_id: int
    status: str

@task
async def process_user(data: UserData) -> ProcessResult:
    # automatic validation of inputs and outputs!
    user_id = 42
    return ProcessResult(user_id=user_id, status="active")

# this will validate automatically
data = UserData(name="Alice", email="alice@example.com", age=30)
task = process_user(data)

# you can also provide dict and it will be automatically validated
user_data = {"name": "Bob", "email": "bob@example.com", "age": 30}
task = process_user(user_data)

# input is validated immediately, before the task can even be queued
user_data = {"invalid": "input"}
task = process_user(user_data)  # throws a ValidationError exception!
