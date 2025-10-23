"""Background tasks using sheppy for database operations."""

import json
from datetime import datetime, timedelta

from fastapi import Depends
from sqlmodel import Session, select

from sheppy import task

from .database import get_session
from .models import AuditLog, User
from .schemas import TaskStatus


@task
async def create_audit_log(
    user_id: int,
    action: str,
    description: str,
    metadata: dict | None = None,
    session: Session = Depends(get_session),
) -> TaskStatus:
    """
    Background task to create an audit log entry.

    This demonstrates sheppy's ability to work with database operations
    using FastAPI's dependency injection pattern.
    """
    try:
        audit_log = AuditLog(
            user_id=user_id,
            action=action,
            description=description,
            metadata=json.dumps(metadata) if metadata else None,
        )
        session.add(audit_log)
        session.commit()
        session.refresh(audit_log)

        return TaskStatus(
            success=True,
            message=f"Audit log created with ID {audit_log.id}",
            task_id=str(audit_log.id),
        )
    except Exception as e:
        session.rollback()
        return TaskStatus(
            success=False,
            message=f"Failed to create audit log: {str(e)}",
        )


@task
async def update_user_activity(
    user_id: int,
    is_active: bool,
    session: Session = Depends(get_session),
) -> TaskStatus:
    """
    Background task to update user activity status.

    Demonstrates database write operations in background tasks.
    """
    try:
        user = session.get(User, user_id)
        if not user:
            return TaskStatus(
                success=False,
                message=f"User with ID {user_id} not found",
            )

        user.is_active = is_active
        user.updated_at = datetime.utcnow()
        session.add(user)
        session.commit()
        session.refresh(user)

        # Create an audit log for this action
        audit_log = AuditLog(
            user_id=user_id,
            action="USER_STATUS_CHANGED",
            description=f"User status changed to {'active' if is_active else 'inactive'}",
        )
        session.add(audit_log)
        session.commit()

        return TaskStatus(
            success=True,
            message=f"User {user_id} activity updated to {is_active}",
            task_id=str(user_id),
        )
    except Exception as e:
        session.rollback()
        return TaskStatus(
            success=False,
            message=f"Failed to update user activity: {str(e)}",
        )


@task
async def bulk_update_users(
    user_ids: list[int],
    update_data: dict,
    session: Session = Depends(get_session),
) -> TaskStatus:
    """
    Background task to update multiple users at once.

    Demonstrates bulk database operations in background tasks.
    This is useful for long-running operations that shouldn't block API responses.
    """
    try:
        updated_count = 0

        for user_id in user_ids:
            user = session.get(User, user_id)
            if user:
                if "email" in update_data:
                    user.email = update_data["email"]
                if "full_name" in update_data:
                    user.full_name = update_data["full_name"]
                if "is_active" in update_data:
                    user.is_active = update_data["is_active"]

                user.updated_at = datetime.utcnow()
                session.add(user)
                updated_count += 1

        session.commit()

        # Create audit log for bulk update
        audit_log = AuditLog(
            user_id=None,
            action="BULK_USER_UPDATE",
            description=f"Bulk updated {updated_count} users",
            metadata=json.dumps({"user_ids": user_ids, "update_data": update_data}),
        )
        session.add(audit_log)
        session.commit()

        return TaskStatus(
            success=True,
            message=f"Successfully updated {updated_count} out of {len(user_ids)} users",
        )
    except Exception as e:
        session.rollback()
        return TaskStatus(
            success=False,
            message=f"Failed to bulk update users: {str(e)}",
        )


@task
async def cleanup_inactive_users(
    days_inactive: int = 90,
    session: Session = Depends(get_session),
) -> TaskStatus:
    """
    Background task to find and deactivate users who haven't been active.

    This demonstrates querying the database and performing conditional updates.
    """
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days_inactive)

        # Find users who haven't been updated recently and are still active
        statement = select(User).where(
            User.updated_at < cutoff_date,
            User.is_active == True  # noqa: E712
        )
        users = session.exec(statement).all()

        deactivated_count = 0
        for user in users:
            user.is_active = False
            user.updated_at = datetime.utcnow()
            session.add(user)
            deactivated_count += 1

        session.commit()

        # Create audit log
        audit_log = AuditLog(
            user_id=None,
            action="CLEANUP_INACTIVE_USERS",
            description=f"Deactivated {deactivated_count} inactive users (inactive for {days_inactive} days)",
            metadata=json.dumps({"days_inactive": days_inactive, "count": deactivated_count}),
        )
        session.add(audit_log)
        session.commit()

        return TaskStatus(
            success=True,
            message=f"Deactivated {deactivated_count} inactive users",
        )
    except Exception as e:
        session.rollback()
        return TaskStatus(
            success=False,
            message=f"Failed to cleanup inactive users: {str(e)}",
        )
