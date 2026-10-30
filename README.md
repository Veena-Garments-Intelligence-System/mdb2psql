# MDB to PostgreSQL Sync Platform

Production-grade incremental synchronization service for replicating Microsoft Access (MDB) data to PostgreSQL.

## Features

- **Read-only MDB Access:** Guaranteed zero writes to source MDB.
- **Incremental Scanning:** Uses Primary Keys for efficient append detection.
- **Rolling Reconciliation:** Checksum-based update detection for recent records.
- **Deterministic Checksums:** SHA256 business field hashing.
- **Structured Logging:** JSON logs for production observability.
- **Resilient:** Automatic retries with exponential backoff.

## Prerequisites

- **Windows Environment:** Required for native MDB (Access) driver access.
- **Python 3.12+**
- **uv** (Python package manager)
- **Docker & Docker Compose** (for PostgreSQL and pgAdmin)
- **NSSM** (optional, for Windows Service deployment)

## Setup

1. **Infrastructure:**
   ```bash
   docker compose up -d
   ```
2. **Environment:**
   Copy `.env.example` to `.env` and configure your settings.
   Ensure `POSTGRES_URI`, `MDB_PATH`, and `MDB_DRIVER` are correct for your environment.

3. **Install Dependencies:**
   ```bash
   uv sync
   ```

## Running the Platform

### Local Execution
```bash
uv run python -m src.mdb_sync.main
```

### Windows Service
Use the provided PowerShell scripts in `./scripts/`:
- `install_service.ps1`: Deploys as a Windows Service using NSSM.

## Project Structure
- `src/mdb_sync/domain`: Core business models and checksum logic.
- `src/mdb_sync/application`: Sync engine orchestration and mapping.
- `src/mdb_sync/infrastructure`: MDB and PostgreSQL repository implementations.
- `src/mdb_sync/scheduler`: Continuous execution loop.

## Ingestion Philosophy
We sync curated columns from MDB into "raw" PostgreSQL tables. All processed records are permanently retained in PostgreSQL for historical auditing and VGIS event recalculation.

Raw Tables:
- `raw_customers`
- `raw_cities`
- `raw_sales`
- `raw_receipts`
- `raw_rg` (uses created_at as immutable forensic timestamp)

Tracking and Coordination:
- `sync_checkpoints`: Tracks independent cursors for fast-path incremental sync and background reconciliation.
- `sync_locks`: Distributed atomic advisory locking with instance ID lease tracking.
- `sync_failures`: Dead letter queue for resilient error capture without stopping sync cycles.
- `sync_runs`: Audit logging of each synchronization cycle.

## Database Migrations
We use Alembic for version-controlled database schema migrations.

### Applying Migrations
Apply all pending schema migrations:
```bash
uv run alembic upgrade head
```

### Creating New Migrations
When updating models in `src/mdb_sync/infrastructure/postgres/models.py`, autogenerate a revision:
```bash
uv run alembic revision --autogenerate -m "describe_change"
```

### Schema Initialization (Alternative)
For fresh manual deployments without Alembic tracking:
```bash
uv run python scripts/init_db.py
```
