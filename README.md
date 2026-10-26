# TrendSentinel Scraper

Gather posts from public internet sources, normalize them into one shape, store them
idempotently in PostgreSQL, and serve them over a REST API.

> **Scope: collection, normalization, validation, storage and monitoring only.**
> No trend detection, no viral scoring, no sentiment analysis, no ML. That belongs to a
> separate application; this system is the data pipeline behind it.

- **Stack:** Python 3.12 · FastAPI · PostgreSQL 17 · native SQL (no ORM) · APScheduler
- **Model:** `sources` → collectors → normalized item → PostgreSQL → REST API → SPA

```
   sources ──▶ collectors ──▶ normalized item ──▶ PostgreSQL ──▶ REST API ──▶ SPA
   (config)     (fetch)        (validate)         (dedup)        (read)      (browse)
```

## Features

- **Hybrid sources** — official APIs and RSS/Atom feeds, all behind one collector contract.
- **Authenticated sources** — Reddit, X and Threads via their official APIs, with a
  write-only credential endpoint (no shell, no session scraping, no browser automation).
- **Idempotent storage** — re-running updates the same row instead of duplicating it.
- **Point-in-time metric snapshots** — a score is not a column; each run records a row.
- **Scheduler in its own process** — the API never schedules, the worker never serves HTTP.
- **Secret-safe by default** — no credential is ever logged, echoed, or committed.

## Sources

| Key | Integration | Auth | Setup |
|---|---|---|---|
| `hackernews` | official API | none | instant |
| `hnrss_frontpage` | RSS | none | instant |
| `github_blog` | RSS | none | instant |
| `reddit_technology` | official API | OAuth2 script app | free, 5 min |
| `x_trendsentinel` | official API | app-only bearer | **paid read tier** |
| `threads_profile` | official API | long-lived token | **Meta App Review** |

Reddit, X and Threads are accessed **only** through their official authenticated APIs with
credentials stored in `sources.config`; each is seeded inactive until an operator provisions
it. No source anywhere is read through browser automation, session cookies or login scraping.

## Quickstart

Requires Docker with Compose v2.

```bash
cp .env.example .env

# 1. Generate an admin key and set it as ADMIN_API_KEY in .env
python -c "import secrets; print(secrets.token_urlsafe(32))"

# 2. Allow the SPA's origin
#    CORS_ORIGINS=http://localhost:5173,http://localhost:5180

docker compose up -d --build
docker compose ps     # db healthy, migrate exited 0, api healthy, worker up
```

Bring up: `db` (PostgreSQL, loopback-only), `migrate` (one-shot Alembic), `api`
(uvicorn on `:8000`), `worker` (the scheduler). Verify:

```bash
curl -s localhost:8000/health    # {"status":"ok","database":"ok",...}
curl -s localhost:8000/sources
```

Collect now (the worker executes queued runs within ~10s):

```bash
export ADMIN_API_KEY='the-key-you-generated'
curl -s -X POST localhost:8000/collect/run \
  -H "X-API-Key: $ADMIN_API_KEY" -H 'content-type: application/json' \
  -d '{"source_key": "hackernews", "limit": 10}'
```

Read the data:

```bash
curl -s 'localhost:8000/items?source=hackernews&limit=5'
```

Watch the worker:

```bash
docker compose logs -f worker
```

## API

Base URL in development: `http://localhost:8000`. Every non-2xx response uses one error
envelope: `{"error":{"code":"...","message":"...","details":null}}`.

| Method | Path | Auth | What it does |
|---|---|---|---|
| GET | `/health` | — | liveness + DB reachability |
| GET | `/sources` | — | configured sources (`?active_only=true` to filter) |
| GET | `/sources/credentials` | `X-API-Key` | which credential keys are set, per source |
| POST | `/sources/{key}/credentials` | `X-API-Key` | store credentials, optionally activate |
| GET | `/items` | — | page of items (`source`, `limit`, `cursor`, `since`, `until`, `q`) |
| GET | `/items/{id}` | — | single item |
| GET | `/items/{id}/snapshots` | — | metric history for that item |
| GET | `/export/items.csv` | — | CSV export (`source`, `max_rows`) |
| GET | `/export/items.csv/plan` | — | export plan (row estimate) |
| GET | `/collect/status` | — | recent runs and their outcomes |
| POST | `/collect/run` | `X-API-Key` | queue a run (`source_key`, `limit`) |

Pagination is keyset: echo `next_cursor` back as `cursor` until it is `null`; never build
one. `config` in `/sources` responses is sanitized — credential-like keys are removed.

## Adding an authenticated source (Reddit / X / Threads)

The three credentialed sources are provisioned through a web screen or the API. The path:
obtain a credential from the platform → provision it → activate the source → collect.

| Source | Get the credential at | Keys | Cost / gate |
|---|---|---|---|
| Reddit | `reddit.com/prefs/apps` → script app | `client_id`, `client_secret`, `user_agent` | free |
| X | X developer portal → app-only bearer | `x_bearer_token` | paid read tier |
| Threads | Meta app dashboard → Threads API | `threads_access_token` (±60 days) | Meta App Review |

Example — provision Reddit:

```bash
curl -s -X POST localhost:8000/sources/reddit_technology/credentials \
  -H "X-API-Key: $ADMIN_API_KEY" -H 'content-type: application/json' \
  -d '{"credentials": {
         "reddit_client_id": "…",
         "reddit_client_secret": "…",
         "reddit_user_agent": "trendsentinel:trendsentinel-scraper:1.0 (by /u/your_name)"
       },
       "activate": true}'
```

The write merges into `sources.config` and flips `is_active` in the same transaction. Only key
names the source's own adapter declares are accepted; the stored value is never readable back.
Up to "the credential is accepted" this has been exercised against a live stack; landing real
rows for X/Threads awaits the paid tier and App Review.

## Development

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --all-groups     # install dependencies
uv run ruff check .      # lint
uv run mypy src tests    # type check
uv run pytest            # tests
```

Integration tests need a real PostgreSQL; they skip with an explicit reason when it is
unreachable, so the unit suite stays green anywhere:

```bash
docker compose up -d db
TEST_DATABASE_URL='postgresql://trendsentinel:trendsentinel@127.0.0.1:55432/trendsentinel_test' \
  uv run pytest -v
```

Migrations:

```bash
uv run alembic upgrade head
uv run alembic history
```

## Project layout

```
src/trendsentinel/
├── config.py       central configuration
├── logging.py      structured logging with secret redaction
├── main.py         FastAPI app factory
├── api/            routes, dependencies, error handling
├── schemas/        Pydantic request/response models
├── services/       collection orchestration and run lifecycle
├── collectors/     source adapters (API, RSS) + bounded HTTP/SSRF layer
├── db/             connection pool and native-SQL repositories
└── worker/         scheduler process, separate from the API
migrations/         Alembic schema + seeded sources
tests/              unit tests + integration tests against real PostgreSQL
docker-compose.yml  db, migrate, api and worker
```

## Design rules

- Native parameterized SQL only. No ORM.
- The API never schedules collection; the worker never serves HTTP. Scaling the API cannot
  duplicate a job.
- Manual triggers are queued as `pending` rows and executed by the worker, so manual and
  scheduled collection share one code path.
- Duplicate work is prevented by the database: a partial unique index on active runs plus a
  per-source advisory lock.
- Collectors never write to the database. They return normalized data and errors as values.
- No secret is ever logged, echoed in a response, or committed.
