#!/usr/bin/env python3
# ruff: noqa: T201
"""
Demo script to showcase the FastAPI + SQLModel + Sheppy example.

This script demonstrates the key features of the example application
by making API calls and showing the results.
"""

import asyncio

import httpx


async def main():
    """Run the demo."""
    base_url = "http://localhost:8000"

    print("=" * 70)
    print("FastAPI + SQLModel + Sheppy Demo")
    print("=" * 70)
    print()
    print("Prerequisites:")
    print("  1. Redis server running: redis-server")
    print("  2. FastAPI app running: fastapi dev app/main.py")
    print("  3. Sheppy worker running: sheppy work")
    print()
    print("=" * 70)
    print()

    async with httpx.AsyncClient() as client:
        try:
            # Check health
            print("1. Checking API health...")
            response = await client.get(f"{base_url}/health")
            print(f"   Status: {response.json()}")
            print()

            # Create users
            print("2. Creating users...")
            users_data = [
                {
                    "email": "alice@example.com",
                    "username": "alice",
                    "full_name": "Alice Smith",
                },
                {
                    "email": "bob@example.com",
                    "username": "bob",
                    "full_name": "Bob Johnson",
                },
                {
                    "email": "charlie@example.com",
                    "username": "charlie",
                    "full_name": "Charlie Brown",
                },
            ]

            user_ids = []
            for user_data in users_data:
                response = await client.post(f"{base_url}/users", json=user_data)
                if response.status_code == 201:
                    user = response.json()
                    user_ids.append(user["id"])
                    print(f"   ✓ Created user: {user['username']} (ID: {user['id']})")
                else:
                    print(f"   ✗ Failed to create user: {user_data['username']}")
            print()

            # List users
            print("3. Listing all users...")
            response = await client.get(f"{base_url}/users")
            users = response.json()
            print(f"   Found {len(users)} users:")
            for user in users:
                print(f"     - {user['username']} ({user['email']})")
            print()

            # Update a user
            if user_ids:
                print("4. Updating user (background task creates audit log)...")
                update_data = {"full_name": "Alice Johnson Smith"}
                response = await client.patch(
                    f"{base_url}/users/{user_ids[0]}", json=update_data
                )
                if response.status_code == 200:
                    print(f"   ✓ Updated user {user_ids[0]}")
                print()

            # Toggle user status
            if len(user_ids) > 1:
                print("5. Toggling user status (background task)...")
                response = await client.post(
                    f"{base_url}/users/{user_ids[1]}/toggle-status",
                    params={"is_active": False},
                )
                result = response.json()
                print(f"   ✓ Task queued: {result['task_id']}")
                print(f"   User {user_ids[1]} will be set to inactive")
                print()

            # Bulk update
            if len(user_ids) >= 2:
                print("6. Bulk updating users (background task)...")
                bulk_data = {
                    "user_ids": user_ids[:2],
                    "update_data": {"is_active": True},
                }
                response = await client.post(
                    f"{base_url}/users/bulk-update", json=bulk_data
                )
                result = response.json()
                print(f"   ✓ Task queued: {result['task_id']}")
                print(f"   {len(bulk_data['user_ids'])} users will be updated")
                print()

            # Wait a moment for background tasks to complete
            print("7. Waiting for background tasks to complete...")
            await asyncio.sleep(2)
            print("   ✓ Tasks should be completed")
            print()

            # View audit logs
            print("8. Viewing audit logs...")
            response = await client.get(f"{base_url}/audit-logs")
            logs = response.json()
            print(f"   Found {len(logs)} audit log entries:")
            for log in logs[:5]:  # Show first 5
                print(f"     - [{log['action']}] {log['description']}")
            print()

            # Queue cleanup task
            print("9. Queueing cleanup task for inactive users...")
            response = await client.post(
                f"{base_url}/maintenance/cleanup-inactive-users",
                params={"days_inactive": 90},
            )
            result = response.json()
            print(f"   ✓ Task queued: {result['task_id']}")
            print()

            print("=" * 70)
            print("Demo completed successfully!")
            print()
            print("You can explore more at:")
            print(f"  - API Docs: {base_url}/docs")
            print(f"  - ReDoc: {base_url}/redoc")
            print("=" * 70)

        except httpx.ConnectError:
            print("✗ Error: Could not connect to the API")
            print()
            print("Please ensure:")
            print("  1. Redis is running: redis-server")
            print("  2. FastAPI app is running: fastapi dev app/main.py")
            print("  3. The app is accessible at http://localhost:8000")
        except Exception as e:
            print(f"✗ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
