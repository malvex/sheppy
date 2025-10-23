"""Tests for the FastAPI + SQLModel + Sheppy application."""

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from sheppy import MemoryBackend, Queue, Worker

# Import from the app
import sys
from pathlib import Path

# Add the app directory to the path
app_dir = Path(__file__).parent.parent / "app"
sys.path.insert(0, str(app_dir))

from database import get_session
from main import app, get_queue
from models import AuditLog, User


@pytest.fixture(name="session")
def session_fixture():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture
def backend():
    """Create a memory backend for testing."""
    return MemoryBackend()


@pytest.fixture
def queue(backend):
    """Create a test queue."""
    return Queue(backend, "pytest")


@pytest.fixture
def worker(backend):
    """Create a test worker with fast polling."""
    w = Worker("pytest", backend)
    # Speed up tests
    w._blocking_timeout = 0.01
    w._scheduler_polling_interval = 0.01
    w._cron_polling_interval = 0.01
    return w


@pytest.fixture
def client(session, queue):
    """Create a test client with dependency overrides."""
    app.dependency_overrides[get_session] = lambda: session
    app.dependency_overrides[get_queue] = lambda: queue
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


def test_root_endpoint(client):
    """Test the root endpoint returns API information."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "docs" in data
    assert "endpoints" in data


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_create_user(client, worker):
    """Test creating a user and audit log in background."""
    # Start worker to process tasks
    client.portal.start_task_soon(
        lambda: worker.work(max_tasks=1, register_signal_handlers=False)
    )
    
    user_data = {
        "email": "test@example.com",
        "username": "testuser",
        "full_name": "Test User",
    }
    
    response = client.post("/users", json=user_data)
    assert response.status_code == 201
    data = response.json()
    
    assert data["email"] == user_data["email"]
    assert data["username"] == user_data["username"]
    assert data["full_name"] == user_data["full_name"]
    assert data["is_active"] is True
    assert "id" in data
    assert "created_at" in data


def test_create_duplicate_user(client):
    """Test that creating a duplicate user fails."""
    user_data = {
        "email": "duplicate@example.com",
        "username": "duplicateuser",
        "full_name": "Duplicate User",
    }
    
    # First creation should succeed
    response = client.post("/users", json=user_data)
    assert response.status_code == 201
    
    # Second creation with same email should fail
    response = client.post("/users", json=user_data)
    assert response.status_code == 400
    assert "email" in response.json()["detail"].lower()


def test_list_users(client, session):
    """Test listing users with pagination."""
    # Create some users directly in the database
    for i in range(5):
        user = User(
            email=f"user{i}@example.com",
            username=f"user{i}",
            full_name=f"User {i}",
        )
        session.add(user)
    session.commit()
    
    # List all users
    response = client.get("/users")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 5
    
    # Test pagination
    response = client.get("/users?skip=2&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


def test_get_user(client, session):
    """Test getting a specific user."""
    # Create a user
    user = User(
        email="getuser@example.com",
        username="getuser",
        full_name="Get User",
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    
    # Get the user
    response = client.get(f"/users/{user.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == user.id
    assert data["email"] == user.email
    assert data["username"] == user.username


def test_get_nonexistent_user(client):
    """Test getting a user that doesn't exist."""
    response = client.get("/users/999")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_update_user(client, session, worker):
    """Test updating a user."""
    # Create a user
    user = User(
        email="updateuser@example.com",
        username="updateuser",
        full_name="Update User",
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    
    # Start worker to process audit log task
    client.portal.start_task_soon(
        lambda: worker.work(max_tasks=1, register_signal_handlers=False)
    )
    
    # Update the user
    update_data = {"full_name": "Updated User Name"}
    response = client.patch(f"/users/{user.id}", json=update_data)
    assert response.status_code == 200
    data = response.json()
    assert data["full_name"] == update_data["full_name"]
    assert data["email"] == user.email  # Unchanged


def test_toggle_user_status(client, session, worker):
    """Test toggling user active status via background task."""
    # Create a user
    user = User(
        email="toggleuser@example.com",
        username="toggleuser",
        is_active=True,
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    
    # Start worker to process the task
    client.portal.start_task_soon(
        lambda: worker.work(max_tasks=1, register_signal_handlers=False)
    )
    
    # Toggle status to inactive
    response = client.post(f"/users/{user.id}/toggle-status?is_active=false")
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    assert data["user_id"] == user.id
    assert data["is_active"] is False


def test_bulk_update_users(client, session, worker):
    """Test bulk updating multiple users."""
    # Create multiple users
    user_ids = []
    for i in range(3):
        user = User(
            email=f"bulkuser{i}@example.com",
            username=f"bulkuser{i}",
            is_active=True,
        )
        session.add(user)
        session.commit()
        session.refresh(user)
        user_ids.append(user.id)
    
    # Start worker to process the task
    client.portal.start_task_soon(
        lambda: worker.work(max_tasks=1, register_signal_handlers=False)
    )
    
    # Bulk update
    update_data = {"is_active": False}
    response = client.post(
        "/users/bulk-update",
        json={"user_ids": user_ids, "update_data": update_data}
    )
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    assert data["user_ids"] == user_ids


def test_cleanup_inactive_users(client, worker):
    """Test queuing cleanup task for inactive users."""
    # Start worker to process the task
    client.portal.start_task_soon(
        lambda: worker.work(max_tasks=1, register_signal_handlers=False)
    )
    
    response = client.post("/maintenance/cleanup-inactive-users?days_inactive=90")
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    assert data["days_inactive"] == 90


def test_list_audit_logs(client, session):
    """Test listing audit logs."""
    # Create a user
    user = User(
        email="audituser@example.com",
        username="audituser",
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    
    # Create some audit logs
    for i in range(3):
        audit_log = AuditLog(
            user_id=user.id,
            action=f"TEST_ACTION_{i}",
            description=f"Test action {i}",
        )
        session.add(audit_log)
    session.commit()
    
    # List all audit logs
    response = client.get("/audit-logs")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    
    # Filter by user_id
    response = client.get(f"/audit-logs?user_id={user.id}")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert all(log["user_id"] == user.id for log in data)


def test_audit_log_created_on_user_creation(client, session, worker):
    """Test that audit log is created when a user is created."""
    # Start worker to process the audit log task
    client.portal.start_task_soon(
        lambda: worker.work(max_tasks=1, register_signal_handlers=False)
    )
    
    user_data = {
        "email": "auditcheck@example.com",
        "username": "auditcheck",
        "full_name": "Audit Check",
    }
    
    response = client.post("/users", json=user_data)
    assert response.status_code == 201
    
    # Give the worker a moment to process (already done via portal.start_task_soon)
    # Check that audit log was created
    response = client.get("/audit-logs")
    assert response.status_code == 200
    audit_logs = response.json()
    
    # Should have at least one audit log for USER_CREATED
    user_created_logs = [log for log in audit_logs if log["action"] == "USER_CREATED"]
    assert len(user_created_logs) >= 1
