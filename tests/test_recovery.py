import time
import pytest
from unittest.mock import MagicMock, patch
from src.mdb_sync.utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException, CircuitState
from src.mdb_sync.infrastructure.postgres.database import init_db
from src.mdb_sync.config import settings
from src.mdb_sync.utils.mdb_check import verify_mdb_integrity
from src.mdb_sync.application.sync_engine import SyncEngine

# 1. Test Circuit Breaker transitions
def test_circuit_breaker_flow():
    cb = CircuitBreaker("TestCB", failure_threshold=2, recovery_timeout=0.05, success_threshold=1)
    
    # Start in CLOSED state
    assert cb.state == CircuitState.CLOSED
    
    def failing_call():
        raise ConnectionError("DB offline")
        
    # First failure
    with pytest.raises(ConnectionError):
        cb(failing_call)
    assert cb.state == CircuitState.CLOSED
    
    # Second failure -> transitions to OPEN
    with pytest.raises(ConnectionError):
        cb(failing_call)
    assert cb.state == CircuitState.OPEN
    
    # Immediately subsequent calls fail with CircuitBreakerOpenException
    with pytest.raises(CircuitBreakerOpenException):
        cb(lambda: "won't run")
        
    # Sleep to trigger cooldown -> transitions to HALF_OPEN on next call
    time.sleep(0.06)
    
    # Successful call transitions back to CLOSED
    result = cb(lambda: "ok")
    assert result == "ok"
    assert cb.state == CircuitState.CLOSED


# 2. Test DB Startup Resilience
@patch("src.mdb_sync.infrastructure.postgres.database.engine.connect")
@patch("src.mdb_sync.infrastructure.postgres.database.Base.metadata.create_all")
def test_db_startup_resilience_success(mock_create_all, mock_connect):
    # Mock settings for fast retries
    with patch.object(settings, "DB_STARTUP_RETRY_LIMIT", 5), \
         patch.object(settings, "DB_STARTUP_MIN_BACKOFF", 0.001), \
         patch.object(settings, "DB_STARTUP_MAX_BACKOFF", 0.01), \
         patch.object(settings, "DB_STARTUP_BACKOFF_FACTOR", 1.5):
        
        # Simulate: First 2 connection attempts raise an exception, 3rd succeeds
        mock_conn = MagicMock()
        mock_connect.side_effect = [
            ConnectionError("DB connection refused"),
            ConnectionError("DB starting up"),
            mock_conn
        ]
        
        init_db()
        
        # Verify engine.connect was called 3 times
        assert mock_connect.call_count == 3
        # Verify tables creation was called after success
        mock_create_all.assert_called_once()


# 3. Test MDB File Integrity Check behavior
def test_mdb_integrity_check_failure():
    with patch.object(settings, "MDB_PATH", "./non_existent_file.mdb"):
        # Verifies that it returns False when file doesn't exist
        assert verify_mdb_integrity() is False


# 4. Test Resilient DLQ & Batch Fallback to Row-by-Row
def test_batch_fallback_and_dlq():
    # Setup mocks
    mock_mdb = MagicMock()
    # Mock records in MDB: 2 rows
    mock_mdb.get_new_records.return_value = [
        {"Bill_ID": "B1", "CUSTOMER_ID": "C1", "BILL_DATE": "2023-01-01", "NET_AMOUNT": 100.0, "Dis_Amt": 5.0, "is_Ok": 1},
        {"Bill_ID": "B2", "CUSTOMER_ID": "C2", "BILL_DATE": "2023-01-02", "NET_AMOUNT": 200.0, "Dis_Amt": 10.0, "is_Ok": 1}
    ]
    
    mock_pg_repo = MagicMock()
    mock_pg_repo.get_checkpoint.return_value = None
    
    # Simulate first row successfully upserted, second row failing unique constraint/db error
    # During batch upsert, it fails globally:
    mock_pg_repo.upsert_batch.side_effect = Exception("Batch insert conflict")
    
    # During individual row-by-row fallback:
    # First row succeeds, second row fails
    mock_pg_repo.upsert.side_effect = [None, Exception("Invalid constraint on row B2")]
    
    engine = SyncEngine(mock_mdb)
    engine.set_pg_repo(mock_pg_repo)
    
    # Run incremental sync
    res = engine.sync_table_incremental("BILL_MASTER")
    
    # Check results
    assert res["scanned"] == 2
    assert res["upserted"] == 1  # 1 row succeeded in fallback
    assert res["errors"] == 1    # 1 row failed
    
    # Verify rollback was called for the batch failure
    assert mock_pg_repo.rollback.call_count >= 1
    
    # Verify DLQ (log_sync_failure) was called exactly once for B2
    mock_pg_repo.log_sync_failure.assert_called_once()
    args, kwargs = mock_pg_repo.log_sync_failure.call_args
    assert kwargs.get("source_table") == "BILL_MASTER"
    assert kwargs.get("primary_key") == "B2"
    assert "Invalid constraint" in kwargs.get("error")
