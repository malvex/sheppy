"""Pydantic schemas for API request/response validation."""

from datetime import datetime

from pydantic import BaseModel, EmailStr


class UserCreate(BaseModel):
    """Schema for creating a new user."""

    email: EmailStr
    username: str
    full_name: str | None = None


class UserUpdate(BaseModel):
    """Schema for updating user information."""

    email: EmailStr | None = None
    full_name: str | None = None
    is_active: bool | None = None


class UserResponse(BaseModel):
    """Schema for user response."""

    id: int
    email: str
    username: str
    full_name: str | None = None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class AuditLogResponse(BaseModel):
    """Schema for audit log response."""

    id: int
    user_id: int | None
    action: str
    description: str
    metadata: str | None = None
    created_at: datetime

    class Config:
        from_attributes = True


class TaskStatus(BaseModel):
    """Schema for task completion status."""

    success: bool
    message: str
    task_id: str | None = None
