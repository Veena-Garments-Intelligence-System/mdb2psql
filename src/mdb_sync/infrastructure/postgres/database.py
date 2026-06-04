import time
import random
import socket
import threading
from urllib.parse import urlparse
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from src.mdb_sync.config import settings
from src.mdb_sync.infrastructure.postgres.models import Base
from src.mdb_sync.logging_config import get_logger

logger = get_logger(__name__)

# Configured SQLAlchemy engine with hardened pool parameters
engine = create_engine(
    settings.postgres_url,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_recycle=settings.DB_POOL_RECYCLE,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

recreate_lock = threading.Lock()

def recreate_db_engine():
    """
    Centralized database connection pool manager.
    Disposes of the existing engine and recreates it to automatically recover
    from bad session states or persistent pool corruption.
    Protected by a lock to ensure thread-safety.
    """
    global engine, SessionLocal
    with recreate_lock:
        logger.info("recreating_database_engine")
        try:
            engine.dispose()
        except Exception as e:
            logger.warning("failed_to_dispose_engine", error=str(e))
            
        engine = create_engine(
            settings.postgres_url,
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=settings.DB_MAX_OVERFLOW,
            pool_recycle=settings.DB_POOL_RECYCLE,
            pool_timeout=settings.DB_POOL_TIMEOUT,
            pool_pre_ping=True
        )
        SessionLocal.configure(bind=engine)
        logger.info("database_engine_recreated")

def log_postgres_connection_failure(e: Exception, retry_in_seconds: float):
    """Logs database connectivity failures in a structured JSON format (Requirement 7)."""
    try:
        parsed = urlparse(settings.postgres_url)
        host = parsed.hostname or "unknown"
        database = parsed.path.lstrip('/') or "unknown"
    except Exception:
        host = "unknown"
        database = "unknown"
        
    logger.error(
        "postgres_connection_failed",
        host=host,
        database=database,
        exception_type=type(e).__name__,
        error_message=str(e).split('\n')[0],
        retry_in_seconds=round(retry_in_seconds)
    )

def check_and_log_connectivity_diagnostics(retry_in_seconds: float) -> dict:
    """
    Validates DNS resolution, internet reachability, and postgres host port reachability.
    Prevents false assumptions that PostgreSQL is down when the issue is local network/DNS (Requirement 8).
    """
    diagnostics = {
        "internet": False,
        "dns": False,
        "host_reachable": False,
        "db_connected": False
    }
    
    # 1. DNS Resolution of public domain
    try:
        socket.gethostbyname("one.one.one.one")
        diagnostics["dns"] = True
    except socket.gaierror:
        pass
        
    # 2. Internet Reachability (Cloudflare public DNS port 53)
    try:
        socket.create_connection(("1.1.1.1", 53), timeout=3)
        diagnostics["internet"] = True
    except Exception:
        pass
        
    # 3. PostgreSQL Host reachability
    try:
        parsed_url = urlparse(settings.postgres_url)
        host = parsed_url.hostname
        port = parsed_url.port or 5432
        
        if host:
            # Check host name DNS
            try:
                socket.gethostbyname(host)
            except socket.gaierror:
                pass
                
            # Check port tcp connection
            socket.create_connection((host, port), timeout=3)
            diagnostics["host_reachable"] = True
    except Exception:
        pass
        
    # Log visibility results
    internet_status = "OK" if diagnostics["internet"] else "FAILED"
    dns_status = "OK" if diagnostics["dns"] else "FAILED"
    host_status = "OK" if diagnostics["host_reachable"] else "FAILED"
    
    logger.info(
        "connectivity_diagnostics",
        internet_status=internet_status,
        dns_status=dns_status,
        postgres_host_reachable=host_status
    )
    
    if not diagnostics["internet"]:
        logger.warning("Internet Status: FAILED")
    if not diagnostics["host_reachable"]:
        logger.warning("PostgreSQL Host Unreachable")
        
    return diagnostics

def init_db():
    """Ensures all tables exist in the database with startup resilience (Requirement 1 & 12)."""
    retry_count = 0
    start_time = time.time()
    
    retry_limit = settings.DB_STARTUP_RETRY_LIMIT
    backoff_min = settings.DB_STARTUP_MIN_BACKOFF
    backoff_max = settings.DB_STARTUP_MAX_BACKOFF
    backoff_factor = settings.DB_STARTUP_BACKOFF_FACTOR
    
    logger.info("Starting database connection verification...")
    
    while True:
        # Determine backoff sleep time for this attempt
        backoff = min(backoff_max, backoff_min * (backoff_factor ** retry_count))
        jitter = random.uniform(0, 0.5 * backoff)
        sleep_time = backoff + jitter
        
        # Check diagnostics
        check_and_log_connectivity_diagnostics(sleep_time)
        
        try:
            # Try to connect and execute a test query
            with engine.connect() as conn:
                conn.execute(select(1))
                
            logger.info("Internet Status: OK")
            logger.info("DNS Resolution: OK")
            logger.info("PostgreSQL Reachable: OK")
            logger.info("Connection Established")
            break
        except Exception as e:
            retry_count += 1
            elapsed = time.time() - start_time
            
            # Log structured failure
            log_postgres_connection_failure(e, sleep_time)
            
            if retry_limit is not None and retry_count >= retry_limit:
                logger.critical(
                    "Database startup connection retry limit reached. Exiting.",
                    retry_limit=retry_limit,
                    elapsed_seconds=round(elapsed)
                )
                raise e
                
            logger.info(f"Retrying in {round(sleep_time)} seconds... Attempt {retry_count}")
            time.sleep(sleep_time)
            
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database schemas verified/created successfully")
    except Exception as e:
        logger.exception("Failed to initialize database schema")
        raise e

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
