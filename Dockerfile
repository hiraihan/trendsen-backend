# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Builder: resolve and install dependencies into a self-contained virtualenv.
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

# uv from its official image, pinned by tag.
COPY --from=ghcr.io/astral-sh/uv:0.9.7 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# Optional PyPI index override, for networks where pypi.org is unreachable.
# Unset means "use the default index", so a normal build needs no arguments.
ARG UV_DEFAULT_INDEX=""
ENV UV_DEFAULT_INDEX=${UV_DEFAULT_INDEX}

WORKDIR /app

# Dependencies first: this layer is cached until pyproject.toml or uv.lock changes.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --no-dev

# Then the application itself.
COPY src ./src
RUN uv sync --frozen --no-dev

# ---------------------------------------------------------------------------
# Runtime: no compiler, no uv, no build cache.
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

# Unprivileged user: nothing in this service needs root at runtime.
RUN groupadd --system --gid 1001 app \
    && useradd --system --uid 1001 --gid app --create-home app

WORKDIR /app

COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chown=app:app src ./src
COPY --chown=app:app alembic.ini ./
COPY --chown=app:app migrations ./migrations

USER app

EXPOSE 8000

# Overridden by docker compose per service.
CMD ["uvicorn", "trendsentinel.main:app", "--host", "0.0.0.0", "--port", "8000"]