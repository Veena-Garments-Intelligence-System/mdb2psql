from prometheus_client import Counter, Gauge, Histogram
from src.mdb_sync.infrastructure.postgres.database import engine

SYNC_ROWS = Counter(
    "sync_rows_total",
    "Total number of processed and synchronized rows",
    ["table", "mode", "status"]
)

SYNC_ERRORS = Counter(
    "sync_errors_total",
    "Total number of sync row or batch failures",
    ["table", "mode", "error_type"]
)

SYNC_DURATION = Histogram(
    "sync_duration_seconds",
    "Time spent in a sync operation (seconds)",
    ["table", "mode"]
)

SYNC_LAG = Gauge(
    "sync_lag_minutes",
    "Synchronization delay in minutes comparing source and target timestamps",
    ["table"]
)

POSTGRES_LATENCY = Histogram(
    "postgres_latency_ms",
    "PostgreSQL query/upsert latency in milliseconds",
    ["operation"]
)

MDB_LATENCY = Histogram(
    "mdb_latency_ms",
    "MDB read latency in milliseconds",
    ["operation"]
)

RECONCILIATION_DURATION = Histogram(
    "reconcile_duration_seconds",
    "Time spent in reconciliation cycles",
    ["table"]
)

PRUNING_DURATION = Histogram(
    "pruning_duration_seconds",
    "Time spent in database pruning cycles",
    ["table"]
)

POOL_SIZE = Gauge("connection_pool_size", "SQLAlchemy connection pool configured size")
POOL_CHECKED_OUT = Gauge("connection_pool_checked_out", "SQLAlchemy connection pool checked out connections")
POOL_CHECKED_IN = Gauge("connection_pool_checked_in", "SQLAlchemy connection pool checked in connections")
POOL_OVERFLOW = Gauge("connection_pool_overflow", "SQLAlchemy connection pool overflow connections")

def update_pool_metrics():
    """Updates SQLAlchemy connection pool usage gauges."""
    try:
        POOL_SIZE.set(engine.pool.size())
        POOL_CHECKED_OUT.set(engine.pool.checkedout())
        POOL_CHECKED_IN.set(engine.pool.checkedin())
        if hasattr(engine.pool, "overflow"):
            POOL_OVERFLOW.set(engine.pool.overflow())
    except Exception:
        pass
