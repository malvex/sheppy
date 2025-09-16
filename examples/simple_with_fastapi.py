import asyncio
from collections.abc import Generator
from datetime import datetime

from fastapi import Depends, FastAPI
from pydantic import BaseModel
from sqlmodel import Field, Session, SQLModel, StaticPool, create_engine

from sheppy import MemoryBackend, Queue, Worker, task

################################################
# Database                                     #
################################################

class User(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    email: str
    hashed_password: str|None = None
    welcome_email_sent: bool = False


class UserCreate(BaseModel):
    email: str


engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)

# FastAPI dependency injection
def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session

################################################
# Tasks                                        #
################################################

class Email(BaseModel):
    to: str
    subject: str
    body: str

class Status(BaseModel):
    ok: bool


@task
async def send_welcome_email(user_id: int,
                             session: Session = Depends(get_session),  # supports Dependency Injection
                             ) -> Status:

    db_user = session.get(User, user_id)

    print(f"[{datetime.now()}] Sending welcome email to {db_user.email} (user_id: {user_id})")

    db_user.hashed_password = 42  # generate random password for the user
    db_user.welcome_email_sent = True
    #session.add(db_user)
    session.commit()
    session.refresh(db_user)

    return Status(ok=True)  # tasks can return Pydantic models


backend = MemoryBackend()

# FastAPI dependency injection
def get_queue():
    return Queue(backend)


# DEMO ONLY: in production run this as separate process
async def run_worker():
    w = Worker(backend=backend)
    await w.work()

################################################
# FastAPI                                      #
################################################

async def lifespan(app: FastAPI):
    SQLModel.metadata.create_all(engine)
    asyncio_task = asyncio.create_task(run_worker())
    yield
    asyncio_task.cancel()


app = FastAPI(title="Fancy Website", lifespan=lifespan)


@app.post("/register", status_code=201, response_model_exclude=["hashed_password"])
async def register(user_data: UserCreate,
                   session: Session = Depends(get_session),
                   queue: Queue = Depends(get_queue)) -> User:

    user_email = user_data.email

    # create a new user in DB
    db_user = User(email=user_email)
    session.add(db_user)
    session.commit()
    session.refresh(db_user)

    # welcome email wasn't sent yet
    assert db_user.welcome_email_sent == False

    # welcome email that sends email and stores a flag in DB
    task = send_welcome_email(db_user.id)
    await queue.add(task)

    # optional - for demo purposes we will wait for results
    task = await task.wait_for_result(task)

    assert task.completed
    assert isinstance(task.result, Status)
    assert task.result.ok == True

    # check DB was updated in task
    session.refresh(db_user)
    assert db_user.welcome_email_sent == True

    return db_user

# run with: `fastapi run examples/simple_with_fastapi.py` and go to http://localhost:8000/docs
