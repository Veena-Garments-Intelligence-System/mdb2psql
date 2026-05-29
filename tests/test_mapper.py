import uuid
from datetime import timezone
from src.mdb_sync.application.mapper import DataMapper
from src.mdb_sync.domain.models import Sale, RG

def test_deterministic_raw_id():
    sale = Sale(
        bill_id="BILL001",
        customer_id="CUST001",
        bill_date="2023-01-01",
        net_amount=100.0,
        dis_amt=0.0
    )
    
    pg_data1 = DataMapper.map_to_pg("BILL_MASTER", sale, "TEST_SYSTEM")
    pg_data2 = DataMapper.map_to_pg("BILL_MASTER", sale, "TEST_SYSTEM")
    
    assert pg_data1["raw_id"] == pg_data2["raw_id"]
    assert isinstance(pg_data1["raw_id"], uuid.UUID)
    
    # Different entity ID should have different raw_id
    sale2 = Sale(
        bill_id="BILL002",
        customer_id="CUST001",
        bill_date="2023-01-01",
        net_amount=100.0,
        dis_amt=0.0
    )
    pg_data3 = DataMapper.map_to_pg("BILL_MASTER", sale2, "TEST_SYSTEM")
    assert pg_data1["raw_id"] != pg_data3["raw_id"]

def test_is_processed_flag():
    sale = Sale(
        bill_id="BILL001",
        customer_id="CUST001",
        bill_date="2023-01-01",
        net_amount=100.0,
        dis_amt=0.0
    )
    pg_data = DataMapper.map_to_pg("BILL_MASTER", sale, "TEST_SYSTEM")
    assert pg_data["is_processed"] is False

def test_created_at_mapping_sales():
    sale = Sale(
        bill_id="BILL001",
        customer_id="CUST001",
        bill_date="08/05/22 00:00:00",
        net_amount=100.0
    )
    pg_data = DataMapper.map_to_pg("BILL_MASTER", sale, "TEST_SYSTEM")
    
    assert "created_at" in pg_data
    assert pg_data["created_at"].year == 2022
    assert pg_data["created_at"].month == 5
    assert pg_data["created_at"].day == 8
    assert pg_data["created_at"].tzinfo == timezone.utc

def test_created_at_mapping_rg():
    rg = RG(
        rg_id="RG001",
        customer_id="CUST001",
        rgtype="REPLACEMENT",
        bill_date="2023-12-31",
        net_amount=50.0
    )
    pg_data = DataMapper.map_to_pg("ReturnGoods", rg, "TEST_SYSTEM")

    
    assert "created_at" in pg_data
    assert pg_data["created_at"].year == 2023
    assert pg_data["created_at"].month == 12
    assert pg_data["created_at"].day == 31
