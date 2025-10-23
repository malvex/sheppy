# FastAPI + SQLModel + Sheppy Example

This example demonstrates a full-featured FastAPI application using SQLModel for database operations and Sheppy for background task processing.

## Features

This example showcases:

- **FastAPI Integration**: Full REST API with proper dependency injection
- **SQLModel Database**: User and AuditLog models with SQLite database
- **Background Tasks**: Using sheppy for async database operations
- **Audit Logging**: Automatic audit trail for user actions
- **Bulk Operations**: Background processing of bulk user updates
- **Maintenance Tasks**: Scheduled cleanup of inactive users

## Project Structure

```
with-sqlmodel/
├── app/
│   ├── __init__.py
│   ├── database.py      # Database configuration and session management
│   ├── models.py        # SQLModel database models
│   ├── schemas.py       # Pydantic schemas for API validation
│   ├── tasks.py         # Sheppy background tasks
│   └── main.py          # FastAPI application
├── tests/
│   └── test_app.py      # Application tests
└── README.md            # This file
```

## Requirements

- Python 3.10+
- Redis 6+ (for sheppy backend)

## Installation

1. Install dependencies:

```bash
pip install fastapi uvicorn sqlmodel sheppy redis httpx
```

2. Make sure Redis is running:

```bash
# Using Docker
docker run -d -p 6379:6379 redis:latest

# Or install locally
# On macOS: brew install redis && brew services start redis
# On Ubuntu: sudo apt-get install redis-server
```

## Running the Application

### 1. Start the FastAPI application

```bash
cd examples/fastapi-integration/with-sqlmodel
fastapi dev app/main.py
```

The API will be available at http://localhost:8000

### 2. Start the sheppy worker (in a separate terminal)

```bash
cd examples/fastapi-integration/with-sqlmodel
sheppy work
```

The worker will process background tasks from the queue.

## Quick Demo

To see all the features in action, run the demo script (after starting the app and worker):

```bash
cd examples/fastapi-integration/with-sqlmodel
python demo.py
```

This will demonstrate all the key features including user creation, updates, background tasks, and audit logging.

## API Endpoints

### Users

- `POST /users` - Create a new user (queues audit log creation)
- `GET /users` - List all users with pagination
- `GET /users/{user_id}` - Get a specific user
- `PATCH /users/{user_id}` - Update a user (queues audit log)
- `POST /users/{user_id}/toggle-status` - Toggle user active status (background task)
- `POST /users/bulk-update` - Bulk update multiple users (background task)

### Audit Logs

- `GET /audit-logs` - List audit logs (optionally filter by user_id)

### Maintenance

- `POST /maintenance/cleanup-inactive-users` - Queue cleanup task for inactive users

### Other

- `GET /` - Root endpoint with API information
- `GET /health` - Health check endpoint

## Usage Examples

### Create a User

```bash
curl -X POST "http://localhost:8000/users" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "john@example.com",
    "username": "johndoe",
    "full_name": "John Doe"
  }'
```

### List Users

```bash
curl "http://localhost:8000/users"
```

### Update User Status (Background Task)

```bash
curl -X POST "http://localhost:8000/users/1/toggle-status?is_active=false"
```

### Bulk Update Users (Background Task)

```bash
curl -X POST "http://localhost:8000/users/bulk-update" \
  -H "Content-Type: application/json" \
  -d '{
    "user_ids": [1, 2, 3],
    "update_data": {
      "is_active": true
    }
  }'
```

### View Audit Logs

```bash
# All audit logs
curl "http://localhost:8000/audit-logs"

# Audit logs for specific user
curl "http://localhost:8000/audit-logs?user_id=1"
```

### Cleanup Inactive Users (Background Task)

```bash
curl -X POST "http://localhost:8000/maintenance/cleanup-inactive-users?days_inactive=90"
```

## Interactive API Documentation

FastAPI provides automatic interactive documentation:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## How It Works

### 1. Dependency Injection

The example uses FastAPI's dependency injection system with sheppy tasks:

```python
@task
async def create_audit_log(
    user_id: int,
    action: str,
    description: str,
    session: Session = Depends(get_session),  # FastAPI dependency
) -> TaskStatus:
    # Task implementation
```

### 2. Background Task Queueing

Routes queue tasks without waiting for completion:

```python
@app.post("/users/{user_id}/toggle-status")
async def toggle_user_status(
    user_id: int,
    is_active: bool,
    queue: Queue = Depends(get_queue),
):
    task = update_user_activity(user_id=user_id, is_active=is_active)
    await queue.add(task)
    return {"message": "Task queued", "task_id": task.id}
```

### 3. Database Operations in Tasks

Tasks can perform complex database operations:

```python
@task
async def update_user_activity(
    user_id: int,
    is_active: bool,
    session: Session = Depends(get_session),
) -> TaskStatus:
    user = session.get(User, user_id)
    user.is_active = is_active
    session.commit()
    
    # Also create audit log
    audit_log = AuditLog(...)
    session.add(audit_log)
    session.commit()
```

## Key Concepts Demonstrated

1. **Async Database Operations**: Using sheppy to offload database writes to background workers
2. **Audit Trail**: Automatic logging of user actions using background tasks
3. **Bulk Operations**: Processing multiple records without blocking API responses
4. **FastAPI Dependency Injection**: Seamless integration with sheppy tasks
5. **Error Handling**: Proper error handling in background tasks with status returns
6. **Scalability**: Worker processes can be scaled independently from the API

## Production Considerations

1. **Database**: Switch from SQLite to PostgreSQL or MySQL for production
2. **Redis Configuration**: Use Redis with persistence for production
3. **Worker Scaling**: Run multiple sheppy workers for better throughput
4. **Monitoring**: Add logging and monitoring for background tasks
5. **Error Handling**: Implement retry logic and dead letter queues
6. **Database Connection Pooling**: Configure proper connection pool settings

## Testing

Run the tests:

```bash
cd examples/fastapi-integration/with-sqlmodel
pytest tests/ -v
```

## Troubleshooting

### Tasks not processing?

Make sure the sheppy worker is running:
```bash
sheppy work
```

### Redis connection error?

Ensure Redis is running:
```bash
redis-cli ping
# Should return: PONG
```

### Database errors?

The SQLite database is created automatically. If you encounter issues, delete `test.db` and restart the application.

## Learn More

- [Sheppy Documentation](https://docs.sheppy.org)
- [FastAPI Documentation](https://fastapi.tiangolo.com)
- [SQLModel Documentation](https://sqlmodel.tiangolo.com)
