#!/usr/bin/env python3
"""
Database Schema Initialization Script for DBUpdater.
Applies scripts/init_raw_schema.sql idempotently to the target PostgreSQL database.
"""
import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from sqlalchemy import text  # noqa: E402
from src.mdb_sync.infrastructure.postgres.database import engine  # noqa: E402
from src.mdb_sync.logging_config import get_logger  # noqa: E402

logger = get_logger("dbupdater.init_db")

def init_schema():
    sql_path = Path(__file__).parent / "init_raw_schema.sql"
    if not sql_path.exists():
        logger.error("SQL file not found", path=str(sql_path))
        sys.exit(1)

    logger.info("Reading schema initialization SQL", path=str(sql_path))
    sql_content = sql_path.read_text(encoding="utf-8")

    logger.info("Applying schema to database...", db_url=engine.url.render_as_string(hide_password=True))
    with engine.begin() as conn:
        # Split statements by semicolon if needed, or execute directly
        conn.execute(text(sql_content))

    logger.info("Database schema initialized successfully.")

if __name__ == "__main__":
    init_schema()
