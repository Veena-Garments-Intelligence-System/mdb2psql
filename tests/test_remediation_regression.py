import uuid
import threading
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.mdb_sync.application.mapper import DataMapper
from src.mdb_sync.application.sync_engine import SyncEngine
from src.mdb_sync.infrastructure.mdb.repository import MDBRepository
from src.mdb_sync.infrastructure.postgres.models import (
    RawSale,
    RawRG,
    RawReceipt,
    RawCustomer,
    RawCity,
    SyncCheckpoint,
    ExternalLock,
)
from src.mdb_sync.infrastructure.postgres.repository import PostgresRepository
from src.mdb_sync.scheduler.sync_scheduler import SyncScheduler

# Test DB Connection URL
TEST_PG_URL = "postgresql://graphora:Harsh7777@127.0.0.1:5432/graphora"


# ---------------------------------------------------------------------------
# FINDING-001: Source PK Type Declarations & Numeric vs String Comparison
# ---------------------------------------------------------------------------

def test_source_pk_types_declared_in_mapper():
    """Verify that every mapped table specifies its source PK type as integer."""
    for table_name, mapping in DataMapper.MAPPING.items():
        assert "pk_type" in mapping, f"Missing pk_type in mapping for {table_name}"
        assert mapping["pk_type"] is int, f"Table {table_name} should have int pk_type"


def test_numeric_pk_comparison_boundaries():
    """Verify numeric comparisons across single-to-multi digit boundaries."""
    # 8 -> 9 -> 10 -> 11 boundary
    assert DataMapper.is_greater_pk(10, 9, int) is True
    assert DataMapper.is_greater_pk("10", "9", int) is True
    assert DataMapper.is_greater_pk("9", "10", int) is False
    assert DataMapper.is_greater_pk(11, 10, int) is True

    # 98 -> 99 -> 100 boundary
    assert DataMapper.is_greater_pk("100", "99", int) is True
    assert DataMapper.is_greater_pk("99", "100", int) is False

    # String comparison works when pk_type is str
    assert DataMapper.is_greater_pk("B10", "B09", str) is True
    assert DataMapper.is_greater_pk("A", "B", str) is False

    # Equal values are not greater
    assert DataMapper.is_greater_pk(10, 10, int) is False
    assert DataMapper.is_greater_pk("10", "10", int) is False


def test_get_new_records_casts_numeric_pk():
    """Verify get_new_records casts numeric last_pk to integer parameter."""
    repo = MDBRepository()
    repo.conn_str = "DRIVER={Test};DBQ=test.mdb"

    with patch.object(repo, "execute_query_yield") as mock_exec:
        list(repo.get_new_records("BILL_MASTER", "Bill_ID", "10", pk_type=int))
        mock_exec.assert_called_once()
        query, params = mock_exec.call_args[0]
        assert "Bill_ID > ?" in query
        assert params == (10,)
        assert isinstance(params[0], int)


# ---------------------------------------------------------------------------
# FINDING-004: Interrupted Reconciliation Cursor Preservation vs Natural Exhaustion
# ---------------------------------------------------------------------------

def test_reconciliation_preserves_cursor_on_interruption():
    """If reconciliation stops early due to stop_event, cursor MUST NOT reset to None."""
    mock_mdb = MagicMock()
    mock_pg = MagicMock()
    
    stop_event = threading.Event()
    engine = SyncEngine(mdb_repo=mock_mdb, cycle_id="test-cycle", stop_event=stop_event)
    mock_pg.upsert_batch.return_value = 1
    engine.set_pg_repo(mock_pg)

    # Initial checkpoint cursor is 100
    mock_checkpoint = MagicMock()
    mock_checkpoint.last_reconcile_key = "100"
    mock_pg.get_checkpoint.return_value = mock_checkpoint

    # Simulate MDB returning 2 rows, but stop_event is set on row 1
    rows = [
        {"Bill_ID": 101, "Customer_ID": "C1", "Bill_Date": "2024-01-01", "Net_Amount": 100.0, "Dis_Amt": 0.0, "is_ok": 0},
        {"Bill_ID": 102, "Customer_ID": "C1", "Bill_Date": "2024-01-02", "Net_Amount": 200.0, "Dis_Amt": 0.0, "is_ok": 0},
    ]

    def row_stream():
        for r in rows:
            yield r
            stop_event.set()

    mock_mdb.get_new_records.return_value = row_stream()

    result = engine.reconcile_table_chunk("BILL_MASTER", chunk_limit=5000)

    # Checkpoint should have been updated with "101" (last processed PK before stop event, not None!)
    mock_pg.update_checkpoint.assert_called_with("BILL_MASTER", last_reconcile_key="101", cycle_id="test-cycle")
    assert result["scanned"] == 1


def test_reconciliation_resets_cursor_on_natural_exhaustion():
    """If reconciliation naturally exhausts all rows, cursor wraps around to None."""
    mock_mdb = MagicMock()
    mock_pg = MagicMock()
    mock_pg.upsert_batch.return_value = 1
    
    engine = SyncEngine(mdb_repo=mock_mdb, cycle_id="test-cycle", stop_event=None)
    engine.set_pg_repo(mock_pg)

    mock_checkpoint = MagicMock()
    mock_checkpoint.last_reconcile_key = "500"
    mock_pg.get_checkpoint.return_value = mock_checkpoint

    # Rows finish naturally before chunk_limit
    rows = [
        {"Bill_ID": 501, "Customer_ID": "C1", "Bill_Date": "2024-01-01", "Net_Amount": 100.0, "Dis_Amt": 0.0, "is_ok": 0},
    ]
    mock_mdb.get_new_records.return_value = iter(rows)

    engine.reconcile_table_chunk("BILL_MASTER", chunk_limit=5000)

    # Naturally exhausted -> cursor wrapped to None
    mock_pg.update_checkpoint.assert_called_with("BILL_MASTER", last_reconcile_key=None, cycle_id="test-cycle")


# ---------------------------------------------------------------------------
# FINDING-005: Mid-Stream Failure Does Not Duplicate Yielded Rows
# ---------------------------------------------------------------------------

@patch("time.sleep", return_value=None)
@patch("pyodbc.connect")
def test_midstream_fetch_failure_raises_without_retry(mock_connect, mock_sleep):
    """Once rows are yielded, a subsequent fetch error must raise immediately and NOT restart from row 1."""
    repo = MDBRepository()
    repo.conn_str = "DRIVER={Test};DBQ=test.mdb"

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.description = [("Bill_ID",)]
    # First fetch succeeds with [('1',)], second fetch fails with HY000
    mock_cursor.fetchmany.side_effect = [
        [("1",)],
        pyodbc.Error("HY000", "Midstream connection dropped"),
    ]
    mock_connect.return_value = mock_conn

    generator = repo.execute_query_yield("SELECT Bill_ID FROM BILL_MASTER")
    
    first_row = next(generator)
    assert first_row["Bill_ID"] == "1"

    # Next item should raise exception immediately, NOT retry from beginning
    with pytest.raises(pyodbc.Error) as exc_info:
        next(generator)
    
    assert "HY000" in str(exc_info.value)
    # mock_connect should have been called only once (no restart)
    assert mock_connect.call_count == 1


# ---------------------------------------------------------------------------
# FINDING-006 & Distributed Lock Unit Verification
# ---------------------------------------------------------------------------

def test_sync_scheduler_has_unique_instance_id():
    """Verify each SyncScheduler instance generates a unique instance_id."""
    s1 = SyncScheduler(sync_engine=None)
    s2 = SyncScheduler(sync_engine=None)
    assert s1.instance_id != s2.instance_id
    assert "dbupdater-" in s1.instance_id


# ---------------------------------------------------------------------------
# FINDING-007 & 010: Dead Letter Recovery Columns and Raw Model Attributes
# ---------------------------------------------------------------------------

def test_raw_models_have_dead_letter_recovery_attributes():
    """Verify all raw models define processing_attempts and last_processing_error."""
    for model in [RawSale, RawReceipt, RawRG, RawCustomer, RawCity]:
        assert hasattr(model, "processing_attempts"), f"{model.__name__} missing processing_attempts"
        assert hasattr(model, "last_processing_error"), f"{model.__name__} missing last_processing_error"
        assert hasattr(model, "processed_at"), f"{model.__name__} missing processed_at"
        assert hasattr(model, "processing_batch_id"), f"{model.__name__} missing processing_batch_id"


# ---------------------------------------------------------------------------
# POSTGRESQL INTEGRATION TESTS (FINDING-002, FINDING-003, FINDING-006, FINDING-007)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pg_session():
    """Provides a live PostgreSQL session for integration verification, skipping if unreachable."""
    try:
        engine = create_engine(TEST_PG_URL, pool_pre_ping=True)
        with engine.connect():
            pass
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        session.close()
        engine.dispose()
    except Exception as e:
        pytest.skip(f"PostgreSQL test database not accessible: {e}")


def test_pg_distributed_lock_mutual_exclusion(pg_session):
    """Verify atomic distributed locking: instance A acquires, instance B is blocked, instance B cannot release."""
    repo = PostgresRepository(pg_session)
    lock_name = f"TEST_LOCK_{uuid.uuid4().hex[:8]}"
    instance_a = "worker-instance-A"
    instance_b = "worker-instance-B"

    try:
        # 1. Instance A acquires lock
        acquired = repo.acquire_lock(lock_name, locked_by=instance_a, timeout_minutes=5)
        pg_session.commit()
        assert acquired is True

        # 2. Instance B attempts to acquire same lock -> should be blocked
        blocked = repo.acquire_lock(lock_name, locked_by=instance_b, timeout_minutes=5)
        pg_session.commit()
        assert blocked is False

        # 3. Instance B attempts to release Instance A's lock -> should fail / not release
        repo.release_lock(lock_name, locked_by=instance_b)
        pg_session.commit()
        
        lock_rec = pg_session.get(ExternalLock, lock_name)
        assert lock_rec is not None
        assert lock_rec.locked_by == instance_a

        # 4. Instance A releases lock -> succeeds
        repo.release_lock(lock_name, locked_by=instance_a)
        pg_session.commit()
        assert pg_session.get(ExternalLock, lock_name) is None

    finally:
        pg_session.query(ExternalLock).filter_by(lock_name=lock_name).delete()
        pg_session.commit()


def test_pg_checkpoint_independence(pg_session):
    """Verify update_checkpoint updates last_sync_key and last_reconcile_key independently."""
    repo = PostgresRepository(pg_session)
    test_table = f"TEST_TABLE_{uuid.uuid4().hex[:8]}"

    try:
        # 1. Incremental sync sets last_sync_key = "100"
        repo.update_checkpoint(test_table, last_sync_key="100", cycle_id="cycle-1")
        pg_session.commit()

        cp = repo.get_checkpoint(test_table)
        assert cp.last_sync_key == "100"
        assert cp.last_reconcile_key is None

        # 2. Reconcile chunk sets last_reconcile_key = "50" WITHOUT touching last_sync_key
        repo.update_checkpoint(test_table, last_reconcile_key="50", cycle_id="cycle-2")
        pg_session.commit()

        cp = repo.get_checkpoint(test_table)
        assert cp.last_sync_key == "100", "last_sync_key was clobbered by reconcile update!"
        assert cp.last_reconcile_key == "50"

        # 3. Incremental sync advances last_sync_key = "200" WITHOUT touching last_reconcile_key
        repo.update_checkpoint(test_table, last_sync_key="200", cycle_id="cycle-3")
        pg_session.commit()

        cp = repo.get_checkpoint(test_table)
        assert cp.last_sync_key == "200"
        assert cp.last_reconcile_key == "50", "last_reconcile_key was clobbered by incremental update!"

    finally:
        pg_session.query(SyncCheckpoint).filter_by(table_name=test_table).delete()
        pg_session.commit()


def test_pg_null_safe_checksum_upsert_and_dead_letter_recovery(pg_session):
    """
    Verify FINDING-002 and FINDING-007:
    1. Row with stored NULL checksum is updated when new checksum arrives (NULL-safe is_distinct_from).
    2. Modifying row checksum resets processing_attempts = 0 and last_processing_error = None.
    3. Identical checksum row is NOT updated (no-op).
    """
    repo = PostgresRepository(pg_session)
    test_bill_id = f"TEST_BILL_{uuid.uuid4().hex[:8]}"

    try:
        # Step 1: Insert an existing row with NULL checksum and failed processing state
        raw_id = uuid.uuid4()
        initial_sale = RawSale(
            raw_id=raw_id,
            bill_id=test_bill_id,
            customer_id="C999",
            bill_date="2024-01-01",
            net_amount=1000.0,
            dis_amt=0.0,
            checksum=None,
            source_system="BILLING_MDB",
            is_processed=False,
            processing_attempts=3,
            last_processing_error="Simulated VGIS DLQ error",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        pg_session.add(initial_sale)
        pg_session.commit()

        # Step 2: DBUpdater upserts row with valid checksum 'CHECKSUM_V1'
        # FINDING-002: With IS DISTINCT FROM, existing NULL != 'CHECKSUM_V1', so it MUST update!
        update_data = {
            "raw_id": raw_id,
            "bill_id": test_bill_id,
            "customer_id": "C999",
            "bill_date": "2024-01-01",
            "net_amount": 1000.0,
            "dis_amt": 0.0,
            "checksum": "CHECKSUM_V1",
            "source_system": "BILLING_MDB",
            "is_processed": False,
        }
        repo.upsert(RawSale, update_data, "bill_id")
        pg_session.commit()

        # Verify update applied and processing_attempts was reset to 0 (FINDING-007)
        saved = pg_session.get(RawSale, raw_id)
        assert saved.checksum == "CHECKSUM_V1", "Row with NULL checksum failed to update!"
        assert saved.processing_attempts == 0, "processing_attempts was not reset on checksum change!"
        assert saved.last_processing_error is None, "last_processing_error was not cleared!"

        # Step 3: Simulate VGIS processing failure (processing_attempts = 3)
        saved.processing_attempts = 3
        saved.last_processing_error = "Permanent business validation failure"
        pg_session.commit()

        # Step 4: Upsert identical row (same checksum 'CHECKSUM_V1')
        # IS DISTINCT FROM should NOT trigger an update since checksums match
        repo.upsert(RawSale, update_data, "bill_id")
        pg_session.commit()

        saved = pg_session.get(RawSale, raw_id)
        # Should remain 3 because checksum did not change
        assert saved.processing_attempts == 3, "Unchanged row should not reset processing attempts!"

        # Step 5: Upsert modified row with new checksum 'CHECKSUM_V2'
        update_data_v2 = dict(update_data)
        update_data_v2["net_amount"] = 1200.0
        update_data_v2["checksum"] = "CHECKSUM_V2"
        
        repo.upsert(RawSale, update_data_v2, "bill_id")
        pg_session.commit()

        saved = pg_session.get(RawSale, raw_id)
        assert saved.checksum == "CHECKSUM_V2"
        assert saved.net_amount == 1200.0
        # Checksum changed -> dead letter recovery triggered!
        assert saved.processing_attempts == 0
        assert saved.last_processing_error is None

    finally:
        pg_session.rollback()
        pg_session.query(RawSale).filter_by(bill_id=test_bill_id).delete()
        pg_session.commit()
