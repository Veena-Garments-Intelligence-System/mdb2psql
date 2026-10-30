import uuid
from unittest.mock import MagicMock
from src.mdb_sync.domain.models import Sale
from src.mdb_sync.infrastructure.postgres.models import RawSale
from src.mdb_sync.infrastructure.postgres.repository import PostgresRepository
from src.mdb_sync.infrastructure.postgres import models

def test_sync_fingerprint_model_removed():
    """Verify SyncFingerprint model has been removed from postgres models."""
    assert not hasattr(models, "SyncFingerprint"), "SyncFingerprint model still exists in models"

def test_repository_fingerprint_methods_removed():
    """Verify fingerprint querying and updating methods are removed from PostgresRepository."""
    assert not hasattr(PostgresRepository, "get_fingerprint"), "get_fingerprint still exists in PostgresRepository"
    assert not hasattr(PostgresRepository, "update_fingerprints_batch"), "update_fingerprints_batch still exists in PostgresRepository"

def test_checksum_changes_on_field_modification():
    """Verify that any modification to record attributes alters the SHA-256 checksum."""
    sale_v1 = Sale(
        bill_id="B100",
        customer_id="C001",
        bill_date="2024-01-15",
        net_amount=5000.0,
        dis_amt=100.0,
        is_ok=0
    )
    checksum_v1 = sale_v1.checksum

    # Changing amount should alter checksum
    sale_v2 = Sale(
        bill_id="B100",
        customer_id="C001",
        bill_date="2024-01-15",
        net_amount=6000.0,
        dis_amt=100.0,
        is_ok=0
    )
    checksum_v2 = sale_v2.checksum
    assert checksum_v1 != checksum_v2, "Checksum did not change after net_amount alteration"

    # Changing date should alter checksum
    sale_v3 = Sale(
        bill_id="B100",
        customer_id="C001",
        bill_date="2024-01-20",
        net_amount=5000.0,
        dis_amt=100.0,
        is_ok=0
    )
    assert checksum_v1 != sale_v3.checksum, "Checksum did not change after bill_date alteration"

    # Changing customer_id (reassignment) should alter checksum
    sale_v4 = Sale(
        bill_id="B100",
        customer_id="C002",
        bill_date="2024-01-15",
        net_amount=5000.0,
        dis_amt=100.0,
        is_ok=0
    )
    assert checksum_v1 != sale_v4.checksum, "Checksum did not change after customer_id reassignment"

def test_upsert_batch_generates_where_checksum_clause():
    """Verify upsert_batch constructs ON CONFLICT DO UPDATE WHERE checksum != EXCLUDED.checksum."""
    mock_session = MagicMock()
    repo = PostgresRepository(mock_session)

    sale_data = [
        {
            "raw_id": uuid.uuid4(),
            "bill_id": "B100",
            "customer_id": "C001",
            "bill_date": "2024-01-15",
            "net_amount": 5000.0,
            "dis_amt": 100.0,
            "is_ok": 0,
            "checksum": "abc123hash",
            "source_system": "BILLING_MDB",
            "is_processed": False
        }
    ]

    repo.upsert_batch(RawSale, sale_data, "bill_id")

    assert mock_session.execute.called
    stmt = mock_session.execute.call_args[0][0]
    # Check that on_conflict_do_update was used with where clause
    assert stmt._post_values_clause is not None
    # Verify the where clause checks checksum != excluded.checksum
    where_clause = stmt._post_values_clause.update_whereclause
    assert where_clause is not None
    assert "checksum" in str(where_clause)
