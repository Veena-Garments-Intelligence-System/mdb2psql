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
        dis_amt=0.0,
        is_ok=0
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
        dis_amt=0.0,
        is_ok=0
    )
    pg_data3 = DataMapper.map_to_pg("BILL_MASTER", sale2, "TEST_SYSTEM")
    assert pg_data1["raw_id"] != pg_data3["raw_id"]

def test_is_processed_flag():
    sale = Sale(
        bill_id="BILL001",
        customer_id="CUST001",
        bill_date="2023-01-01",
        net_amount=100.0,
        dis_amt=0.0,
        is_ok=1
    )
    pg_data = DataMapper.map_to_pg("BILL_MASTER", sale, "TEST_SYSTEM")
    assert pg_data["is_processed"] is False
    assert pg_data["is_ok"] == 1

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

def test_new_columns_mapping():
    # Test Customer opening_balance
    mdb_customer = {
        "CUSTOMER_ID": "C001",
        "CUSTOMER_NAME": "Test Customer",
        "City_ID": "CITY1",
        "MOBILE1": "1234567890",
        "Opening_Balance": "1500.50"
    }
    customer = DataMapper.map_to_domain("CUSTOMER_MASTER", mdb_customer)
    assert customer.opening_balance == 1500.50
    
    pg_customer = DataMapper.map_to_pg("CUSTOMER_MASTER", customer, "TEST")
    assert pg_customer["opening_balance"] == 1500.50

    # Test Sale is_ok
    mdb_sale = {
        "Bill_ID": "B001",
        "CUSTOMER_ID": "C001",
        "BILL_DATE": "2023-01-01",
        "NET_AMOUNT": "100.0",
        "is_Ok": "1"
    }
    sale = DataMapper.map_to_domain("BILL_MASTER", mdb_sale)
    assert sale.is_ok == 1
    
    pg_sale = DataMapper.map_to_pg("BILL_MASTER", sale, "TEST")
    assert pg_sale["is_ok"] == 1

    # Test Receipt is_ok
    mdb_receipt = {
        "Receipt_ID": "R001",
        "Customer_ID": "C001",
        "is_Ok": "0"
    }
    receipt = DataMapper.map_to_domain("Receipt_Master", mdb_receipt)
    assert receipt.is_ok == 0

    # Test is_ok with boolean True
    mdb_sale_bool = {
        "Bill_ID": "B002",
        "is_Ok": True
    }
    sale_bool = DataMapper.map_to_domain("BILL_MASTER", mdb_sale_bool)
    assert sale_bool.is_ok == 1

    # Test is_ok with string "Yes"
    mdb_sale_yes = {
        "Bill_ID": "B003",
        "is_Ok": "Yes"
    }
    sale_yes = DataMapper.map_to_domain("BILL_MASTER", mdb_sale_yes)
    assert sale_yes.is_ok == 1

    # Test is_ok with -1 (common in Access)
    mdb_sale_neg = {
        "Bill_ID": "B004",
        "is_Ok": -1
    }
    sale_neg = DataMapper.map_to_domain("BILL_MASTER", mdb_sale_neg)
    assert sale_neg.is_ok == 1

    # Test is_ok with string "True"
    mdb_sale_str_true = {
        "Bill_ID": "B005",
        "is_Ok": "True"
    }
    sale_str_true = DataMapper.map_to_domain("BILL_MASTER", mdb_sale_str_true)
    assert sale_str_true.is_ok == 1

    # Test RG is_ok default
    mdb_rg = {
        "RG_ID": "RG001",
        "CUSTOMER_ID": "C001",
        # is_Ok missing
    }
    rg = DataMapper.map_to_domain("ReturnGoods", mdb_rg)
    assert rg.is_ok == 0
