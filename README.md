# Ingestion Tracker — Data Workflow Example

This repository demonstrates an enterprise-ready ingestion tracker used to manage file-based data pipelines. It focuses on reliable orchestration, idempotency, and recoverability rather than complex transformations. The codebase provides a minimal example (Ticketmaster events) and a re-usable `ingestion_tracker` pattern you can adapt to other sources.

Highlights
- Tracks every ingestion with a DB-backed audit trail (UUID, batch_id, content_hash, statuses, timestamps, durations, error message).
- Built-in recovery: retry failed bronze/silver jobs, reset jobs, and admin endpoints for reprocessing.
- Idempotent ingestion using `content_hash` and `batch_id` to prevent double-processing.
- Clear separation of concerns: extraction (bronze), transformation (silver), and write (gold) stages.

**Tech stack**
- Python 3.13, FastAPI
- Polars for efficient in-memory transforms
- Postgres (psycopg + psycopg-pool) for the ingestion tracker and analytical tables
- Supabase storage used in examples for object storage
- Docker Compose for local development

**Key features**
- `ingestion_tracker` table with bronze/silver status lifecycle and timing metrics.
- Automatic table creation and a simple upsert writer for silver-stage writes.
- Admin routes to list failed silver jobs and to retry by `batch_id` or `endpoint`.
- Local mock runner for fast testing without external services: `scripts/run_pipeline_local.py`.

Quick demo — run locally (Docker)
1. Copy `.env.template` to `.env` and populate secrets (do NOT commit `.env`).
2. From the project root:
```powershell
docker compose up --build
```
3. Trigger the sample pipeline (Ticketmaster events):
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/v1/ticketmaster/events" -Method POST
```

Useful endpoints
- Health: `GET /` — basic liveness.
- Swagger: `GET /docs` — API docs and manual testing.
- Admin: `GET /admin/failed/silver?endpoint=events` — list failed silver jobs.
- Admin retry: `POST /admin/retry/silver?batch_id=<BATCH_ID>` or `POST /admin/retry/silver?endpoint=events` — reprocess failed jobs.

Developer hints
- Run a local quick test without external services: `python scripts/run_pipeline_local.py` (mocks clients and tracker).
- The tracker ensures idempotency using `content_hash` and `batch_id`. Duplicate ingests trigger a check and can re-run the silver stage when appropriate.
- For production-grade throughput replace per-row upserts with a staging table + bulk COPY or queue-driven workers (SQS/Redis/Kafka).

Preparing for GitHub
- Ensure `.env` is excluded (see `.gitignore`) and keep only `.env.template` in the repo.
- Consider adding a brief `LICENSE` (MIT recommended for resume projects) and a short `CONTRIBUTING.md` if you plan public contributions.

Next steps (optional)
- Add CI (GitHub Actions) for linting and tests.
- Replace in-process retry with a durable queue + worker for scalability.
- Add sample unit/integration tests that run against a test Postgres instance.