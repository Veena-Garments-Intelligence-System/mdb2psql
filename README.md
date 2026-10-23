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
   Note: PostgreSQL is exposed on port **5433** to avoid collisions.

2. **Environment:**
   Copy `.env.example` to `.env` and configure your settings.
   Ensure `MDB_PATH` and `MDB_DRIVER` are correct for your system.

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
We sync only curated columns from MDB into "raw" PostgreSQL tables. No legacy noise, no locks, just cleaned business data.

- `raw_customers`
- `raw_cities`
- `raw_sales`
- `raw_receipts`
- `raw_rg`

Tracking is maintained in `sync_state` and `sync_fingerprints`.
