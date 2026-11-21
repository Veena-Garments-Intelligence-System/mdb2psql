from src.mdb_sync.config import settings
from src.mdb_sync.infrastructure.postgres.repository import PostgresRepository
from src.mdb_sync.scheduler.sync_scheduler import SyncScheduler
from src.mdb_sync.utils import metrics

def test_pruning_configuration_completely_removed():
    """Verify that pruning settings and flags have been completely eliminated."""
    assert not hasattr(settings, "PRUNE_ENABLED"), "PRUNE_ENABLED setting still exists"
    assert not hasattr(settings, "PRUNE_RETENTION_DAYS_PROCESSED"), "PRUNE_RETENTION_DAYS_PROCESSED still exists"
    assert not hasattr(settings, "PRUNE_INTERVAL_SECONDS"), "PRUNE_INTERVAL_SECONDS still exists"

def test_pruning_methods_completely_removed():
    """Verify that pruning methods are absent from PostgresRepository and SyncScheduler."""
    assert not hasattr(PostgresRepository, "prune_processed_rows"), "prune_processed_rows method still exists"
    assert not hasattr(SyncScheduler, "run_pruning"), "run_pruning method still exists"
    assert not hasattr(metrics, "PRUNING_DURATION"), "PRUNING_DURATION metric still exists"

def test_scheduler_init_no_prune_interval():
    """Verify SyncScheduler does not initialize pruning state."""
    scheduler = SyncScheduler(sync_engine=None)
    assert not hasattr(scheduler, "prune_interval")
    assert not hasattr(scheduler, "_last_prune_at")
