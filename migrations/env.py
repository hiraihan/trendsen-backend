"""Alembic migration environment.

The database URL comes from the application settings, so there is exactly one place a
connection string is configured. Migrations are written as explicit native SQL; there are no
ORM models and no ``target_metadata``.
"""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from trendsentinel.config import get_settings

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Alembic's own metadata is intentionally empty: revisions contain hand-written SQL.
target_metadata = None


def get_database_url() -> str:
    """Return the connection string, in the form SQLAlchemy needs for psycopg 3.

    ``DATABASE_URL`` is a plain ``postgresql://`` libpq URL. SQLAlchemy would interpret that
    as psycopg2, so the driver is made explicit here rather than asking every deployment to
    change its connection string.
    """
    url = str(get_settings().database_url)
    for prefix in ("postgresql+psycopg://", "postgres://"):
        if url.startswith(prefix):
            return url

    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)

    return url


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting to a database."""
    context.configure(
        url=get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Connect to the database and apply migrations."""
    connectable = create_engine(get_database_url(), poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
