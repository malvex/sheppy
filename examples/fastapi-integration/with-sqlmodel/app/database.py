"""Database configuration and session management."""

from collections.abc import Generator

from sqlmodel import Session, SQLModel, create_engine

# SQLite database for development
DATABASE_URL = "sqlite:///./test.db"

# Create engine with check_same_thread=False for SQLite
engine = create_engine(
    DATABASE_URL,
    echo=False,  # Set to True to see SQL queries
    connect_args={"check_same_thread": False},
)


def create_db_and_tables() -> None:
    """Create all database tables."""
    SQLModel.metadata.create_all(engine)


def get_session() -> Generator[Session, None, None]:
    """Dependency for getting database sessions."""
    with Session(engine) as session:
        yield session
