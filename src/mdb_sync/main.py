import sys
import signal
from src.mdb_sync.logging_config import configure_logging, get_logger
from src.mdb_sync.infrastructure.mdb.repository import MDBRepository
from src.mdb_sync.application.sync_engine import SyncEngine
from src.mdb_sync.scheduler.sync_scheduler import SyncScheduler
from src.mdb_sync.infrastructure.postgres.database import init_db, engine

logger = get_logger(__name__)

def handle_shutdown(signum, frame):
    logger.info("Shutdown signal received via signal handler. Closing database connections...")
    try:
        engine.dispose()
        logger.info("Database connection engine successfully disposed.")
    except Exception as e:
        logger.error("Error disposing database engine on signal shutdown", error=str(e))
    sys.exit(0)

def main():
    configure_logging()
    logger.info("Initializing MDB Sync Platform")

    # Set signal handlers for graceful shutdown (Requirement 10)
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        # 1. Start health checks and Prometheus server (Requirement 8)
        # Started early to provide visibility even if DB is down during startup
        from src.mdb_sync.utils.health import start_health_server
        start_health_server()

        # 2. Resilient startup connection (Requirement 1 & 12)
        # This will loop infinitely by default until connection is established
        init_db()
        
        # Cleanup stale runs from previous crashes
        with SessionLocal() as db:
            from src.mdb_sync.infrastructure.postgres.repository import PostgresRepository
            pg_repo = PostgresRepository(db)
            pg_repo.cleanup_stale_runs()
            db.commit()
            logger.info("Cleanup of stale sync runs completed.")
        
        # 3. Initialize scheduler loop
        mdb_repo = MDBRepository()
        sync_engine = SyncEngine(mdb_repo)
        scheduler = SyncScheduler(sync_engine)
        
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        try:
            engine.dispose()
        except Exception:
            pass
        sys.exit(0)
    except Exception:
        logger.exception("Application crashed")
        try:
            engine.dispose()
        except Exception:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()
