"""
FastAPI application with SQLModel and Sheppy integration.

This example demonstrates:
- Full FastAPI app structure with SQLModel for database operations
- Using sheppy for background tasks with database reads and writes
- FastAPI dependency injection with sheppy tasks
- Creating audit logs and managing users asynchronously
"""

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from sqlmodel import Session, select

from sheppy import Queue, RedisBackend

from .database import create_db_and_tables, get_session
from .models import AuditLog, User
from .schemas import AuditLogResponse, TaskStatus, UserCreate, UserResponse, UserUpdate
from .tasks import (
    bulk_update_users,
    cleanup_inactive_users,
    create_audit_log,
    update_user_activity,
)

# Configure sheppy backend
backend = RedisBackend("redis://127.0.0.1:6379")


def get_queue() -> Queue:
    """Dependency for getting the sheppy queue."""
    return Queue(backend)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup: Create database tables
    create_db_and_tables()
    yield
    # Shutdown: Clean up resources if needed


app = FastAPI(
    title="User Management API with Sheppy",
    description="A FastAPI application demonstrating sheppy integration with SQLModel for background tasks",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "User Management API with Sheppy",
        "docs": "/docs",
        "endpoints": {
            "users": "/users",
            "audit_logs": "/audit-logs",
        },
    }


@app.post("/users", response_model=UserResponse, status_code=201)
async def create_user(
    user_data: UserCreate,
    session: Session = Depends(get_session),
    queue: Queue = Depends(get_queue),
) -> User:
    """
    Create a new user.
    
    This endpoint:
    1. Creates a user in the database synchronously
    2. Queues a background task to create an audit log
    """
    # Check if user already exists
    statement = select(User).where(User.email == user_data.email)
    existing_user = session.exec(statement).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="User with this email already exists")
    
    statement = select(User).where(User.username == user_data.username)
    existing_user = session.exec(statement).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="User with this username already exists")
    
    # Create user
    user = User(
        email=user_data.email,
        username=user_data.username,
        full_name=user_data.full_name,
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    
    # Queue background task to create audit log
    task = create_audit_log(
        user_id=user.id,
        action="USER_CREATED",
        description=f"User {user.username} created",
        metadata={"email": user.email, "username": user.username},
    )
    await queue.add(task)
    
    return user


@app.get("/users", response_model=list[UserResponse])
async def list_users(
    skip: int = 0,
    limit: int = 100,
    session: Session = Depends(get_session),
) -> list[User]:
    """List all users with pagination."""
    statement = select(User).offset(skip).limit(limit)
    users = session.exec(statement).all()
    return list(users)


@app.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int,
    session: Session = Depends(get_session),
) -> User:
    """Get a specific user by ID."""
    user = session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.patch("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    session: Session = Depends(get_session),
    queue: Queue = Depends(get_queue),
) -> User:
    """
    Update a user.
    
    This endpoint demonstrates updating the user synchronously
    and creating an audit log in the background.
    """
    user = session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Update user fields
    update_data = user_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(user, key, value)
    
    from datetime import datetime
    user.updated_at = datetime.utcnow()
    session.add(user)
    session.commit()
    session.refresh(user)
    
    # Queue background task to create audit log
    task = create_audit_log(
        user_id=user.id,
        action="USER_UPDATED",
        description=f"User {user.username} updated",
        metadata=update_data,
    )
    await queue.add(task)
    
    return user


@app.post("/users/{user_id}/toggle-status", response_model=dict)
async def toggle_user_status(
    user_id: int,
    is_active: bool,
    queue: Queue = Depends(get_queue),
) -> dict:
    """
    Toggle user active status using a background task.
    
    This demonstrates using sheppy to handle database writes asynchronously.
    The API responds immediately while the task processes in the background.
    """
    task = update_user_activity(user_id=user_id, is_active=is_active)
    await queue.add(task)
    
    return {
        "message": "User status update queued",
        "task_id": task.id,
        "user_id": user_id,
        "is_active": is_active,
    }


@app.post("/users/bulk-update", response_model=dict)
async def bulk_update_users_endpoint(
    user_ids: list[int],
    update_data: UserUpdate,
    queue: Queue = Depends(get_queue),
) -> dict:
    """
    Bulk update multiple users in the background.
    
    This is useful for long-running operations that shouldn't block the API.
    """
    update_dict = update_data.model_dump(exclude_unset=True)
    
    task = bulk_update_users(user_ids=user_ids, update_data=update_dict)
    await queue.add(task)
    
    return {
        "message": f"Bulk update queued for {len(user_ids)} users",
        "task_id": task.id,
        "user_ids": user_ids,
    }


@app.post("/maintenance/cleanup-inactive-users", response_model=dict)
async def cleanup_inactive_users_endpoint(
    days_inactive: int = 90,
    queue: Queue = Depends(get_queue),
) -> dict:
    """
    Queue a task to cleanup inactive users.
    
    This demonstrates scheduling maintenance tasks that query
    and update the database based on conditions.
    """
    task = cleanup_inactive_users(days_inactive=days_inactive)
    await queue.add(task)
    
    return {
        "message": "Cleanup task queued",
        "task_id": task.id,
        "days_inactive": days_inactive,
    }


@app.get("/audit-logs", response_model=list[AuditLogResponse])
async def list_audit_logs(
    user_id: int | None = None,
    skip: int = 0,
    limit: int = 100,
    session: Session = Depends(get_session),
) -> list[AuditLog]:
    """
    List audit logs with optional filtering by user.
    
    This demonstrates reading data created by background tasks.
    """
    statement = select(AuditLog)
    
    if user_id is not None:
        statement = statement.where(AuditLog.user_id == user_id)
    
    statement = statement.order_by(AuditLog.created_at.desc()).offset(skip).limit(limit)
    audit_logs = session.exec(statement).all()
    return list(audit_logs)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
