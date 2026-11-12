-- Idempotent Schema Initialization for DBUpdater
-- Creates all raw ingestion tables, operational sync tracking tables, and necessary indexes.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Raw Sales Table
CREATE TABLE IF NOT EXISTS raw_sales (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    checksum TEXT NULL,
    source_system TEXT NOT NULL,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    processing_batch_id TEXT NULL,
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    last_processing_error TEXT NULL,
    bill_id TEXT UNIQUE NOT NULL,
    customer_id TEXT NULL,
    bill_date TEXT NULL,
    net_amount NUMERIC NULL,
    dis_amt NUMERIC NULL,
    is_ok INTEGER NOT NULL DEFAULT 0,
    bill_amount NUMERIC NULL,
    dis NUMERIC NULL,
    packing_case NUMERIC NULL,
    packing_amount NUMERIC NULL,
    transport_name TEXT NULL,
    transport_amount NUMERIC NULL,
    total_packs NUMERIC NULL,
    bdl NUMERIC NULL,
    dispatch_case INTEGER NULL,
    dispatch_bdl INTEGER NULL,
    fright NUMERIC NULL,
    gross_amt5 NUMERIC NULL,
    gross_amt12 NUMERIC NULL,
    total_tax5 NUMERIC NULL,
    total_tax12 NUMERIC NULL,
    is_gst BOOLEAN NOT NULL DEFAULT FALSE,
    gr_no TEXT NULL,
    is_cash TEXT NULL,
    customer_name TEXT NULL,
    city_id TEXT NULL,
    note TEXT NULL,
    user_name TEXT NULL,
    add_date TEXT NULL,
    bill_group TEXT NULL,
    ref_no NUMERIC NULL,
    company_id TEXT NULL,
    session_id TEXT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_raw_sales_is_processed ON raw_sales (is_processed);
CREATE INDEX IF NOT EXISTS idx_raw_sales_bill_id ON raw_sales (bill_id);
CREATE INDEX IF NOT EXISTS idx_raw_sales_customer_id ON raw_sales (customer_id);

-- 1b. Raw Bill Details Table
CREATE TABLE IF NOT EXISTS raw_bill_details (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    checksum TEXT NULL,
    source_system TEXT NOT NULL,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    processing_batch_id TEXT NULL,
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    last_processing_error TEXT NULL,
    bill_id TEXT NOT NULL,
    line_idx INTEGER NOT NULL,
    item_id TEXT NULL,
    item_name TEXT NULL,
    size_id TEXT NULL,
    size INTEGER NULL,
    qty NUMERIC NULL,
    rate NUMERIC NULL,
    scheme TEXT NULL,
    amount NUMERIC NULL,
    sr BOOLEAN NOT NULL DEFAULT FALSE,
    mtsr NUMERIC NULL,
    company_id TEXT NULL,
    group_name TEXT NULL,
    stock_id INTEGER NULL,
    unit TEXT NULL,
    netrate NUMERIC NULL,
    taxcode NUMERIC NULL,
    discount NUMERIC NULL,
    attend_by TEXT NULL,
    inc_per NUMERIC NULL,
    bill_type INTEGER NULL,
    hbr BOOLEAN NOT NULL DEFAULT FALSE,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT uq_raw_bill_details_bill_line UNIQUE (bill_id, line_idx)
);
CREATE INDEX IF NOT EXISTS idx_raw_bill_details_bill_id ON raw_bill_details (bill_id);
CREATE INDEX IF NOT EXISTS idx_raw_bill_details_item_id ON raw_bill_details (item_id);
CREATE INDEX IF NOT EXISTS idx_raw_bill_details_is_processed ON raw_bill_details (is_processed);

-- 1c. Raw LR Master Table
CREATE TABLE IF NOT EXISTS raw_lr_master (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    checksum TEXT NULL,
    source_system TEXT NOT NULL,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    processing_batch_id TEXT NULL,
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    last_processing_error TEXT NULL,
    lr_id TEXT NOT NULL,
    entry_idx INTEGER NOT NULL DEFAULT 1,
    bill_id TEXT NULL,
    company_id TEXT NULL,
    sno INTEGER NULL,
    customer_id TEXT NULL,
    entry_date TEXT NULL,
    customer_name TEXT NULL,
    city_id TEXT NULL,
    total_packs NUMERIC NULL,
    gr_no TEXT NULL,
    lr_date TEXT NULL,
    transport_name TEXT NULL,
    ps TEXT NULL,
    note TEXT NULL,
    printed BOOLEAN NOT NULL DEFAULT FALSE,
    is_lock BOOLEAN NOT NULL DEFAULT FALSE,
    packing_case NUMERIC NULL,
    packing_amount NUMERIC NULL,
    transport_amount NUMERIC NULL,
    is_update BOOLEAN NOT NULL DEFAULT FALSE,
    user_name TEXT NULL,
    add_date TEXT NULL,
    add_time TEXT NULL,
    is_edit_lock BOOLEAN NOT NULL DEFAULT FALSE,
    is_ok INTEGER NOT NULL DEFAULT 0,
    lock_date TEXT NULL,
    bdl NUMERIC NULL,
    bill_group TEXT NULL,
    ref_no NUMERIC NULL,
    is_dispatch BOOLEAN NOT NULL DEFAULT FALSE,
    clear_remark TEXT NULL,
    lock_user TEXT NULL,
    lock_user_date TEXT NULL,
    checked TEXT NULL,
    is_lr BOOLEAN NOT NULL DEFAULT FALSE,
    pickup_by TEXT NULL,
    is_bus BOOLEAN NOT NULL DEFAULT FALSE,
    is_sms_sent BOOLEAN NOT NULL DEFAULT FALSE,
    auto_freight NUMERIC NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT uq_raw_lr_master_lr_entry UNIQUE (lr_id, entry_idx)
);
CREATE INDEX IF NOT EXISTS idx_raw_lr_master_lr_id ON raw_lr_master (lr_id);
CREATE INDEX IF NOT EXISTS idx_raw_lr_master_bill_id ON raw_lr_master (bill_id);
CREATE INDEX IF NOT EXISTS idx_raw_lr_master_customer_id ON raw_lr_master (customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_lr_master_gr_no ON raw_lr_master (gr_no);
CREATE INDEX IF NOT EXISTS idx_raw_lr_master_is_processed ON raw_lr_master (is_processed);

-- 1d. Raw LR Details Table
CREATE TABLE IF NOT EXISTS raw_lr_details (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    checksum TEXT NULL,
    source_system TEXT NOT NULL,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    processing_batch_id TEXT NULL,
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    last_processing_error TEXT NULL,
    lr_id TEXT NOT NULL,
    bill_id TEXT NOT NULL,
    line_idx INTEGER NOT NULL DEFAULT 1,
    sno TEXT NULL,
    case_count INTEGER NULL,
    bdl NUMERIC NULL,
    dcase NUMERIC NULL,
    dbdl INTEGER NULL,
    rsno TEXT NULL,
    freight NUMERIC NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT uq_raw_lr_details_lr_bill_line UNIQUE (lr_id, bill_id, line_idx)
);
CREATE INDEX IF NOT EXISTS idx_raw_lr_details_lr_id ON raw_lr_details (lr_id);
CREATE INDEX IF NOT EXISTS idx_raw_lr_details_bill_id ON raw_lr_details (bill_id);
CREATE INDEX IF NOT EXISTS idx_raw_lr_details_is_processed ON raw_lr_details (is_processed);

-- 1e. Raw Delete Records Table
CREATE TABLE IF NOT EXISTS raw_delete_records (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    checksum TEXT NULL,
    source_system TEXT NOT NULL,
    bill_id TEXT UNIQUE NOT NULL,
    bill_no TEXT NULL,
    bill_date TEXT NULL,
    customer_name TEXT NULL,
    city_name TEXT NULL,
    net_amount NUMERIC NULL,
    user_name TEXT NULL,
    delete_date TEXT NULL,
    delete_time TEXT NULL,
    remark TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_raw_delete_records_bill_id ON raw_delete_records (bill_id);

-- 2. Raw Receipts Table
CREATE TABLE IF NOT EXISTS raw_receipts (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    checksum TEXT NULL,
    source_system TEXT NOT NULL,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    processing_batch_id TEXT NULL,
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    last_processing_error TEXT NULL,
    receipt_id TEXT UNIQUE NOT NULL,
    customer_id TEXT NULL,
    receipt_date TEXT NULL,
    amount NUMERIC NULL,
    discount NUMERIC NULL,
    bank_name TEXT NULL,
    receipt_type TEXT NULL,
    is_ok INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_raw_receipts_is_processed ON raw_receipts (is_processed);
CREATE INDEX IF NOT EXISTS idx_raw_receipts_receipt_id ON raw_receipts (receipt_id);
CREATE INDEX IF NOT EXISTS idx_raw_receipts_customer_id ON raw_receipts (customer_id);

-- 3. Raw Return Goods Table (RawRG intentionally preserves forensic created_at without updated_at)
CREATE TABLE IF NOT EXISTS raw_rg (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    checksum TEXT NULL,
    source_system TEXT NOT NULL,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    processing_batch_id TEXT NULL,
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    last_processing_error TEXT NULL,
    rg_id TEXT UNIQUE NOT NULL,
    customer_id TEXT NULL,
    rgtype TEXT NULL,
    bill_date TEXT NULL,
    net_amount NUMERIC NULL,
    is_ok INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_raw_rg_is_processed ON raw_rg (is_processed);
CREATE INDEX IF NOT EXISTS idx_raw_rg_rg_id ON raw_rg (rg_id);
CREATE INDEX IF NOT EXISTS idx_raw_rg_customer_id ON raw_rg (customer_id);

-- 4. Raw Customers Table
CREATE TABLE IF NOT EXISTS raw_customers (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    checksum TEXT NULL,
    source_system TEXT NOT NULL,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    processing_batch_id TEXT NULL,
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    last_processing_error TEXT NULL,
    customer_id TEXT UNIQUE NOT NULL,
    customer_name TEXT NULL,
    city_name TEXT NULL,
    mobile1 TEXT NULL,
    is_ok INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_raw_customers_is_processed ON raw_customers (is_processed);
CREATE INDEX IF NOT EXISTS idx_raw_customers_customer_id ON raw_customers (customer_id);

-- 5. Raw Cities Table
CREATE TABLE IF NOT EXISTS raw_cities (
    raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    checksum TEXT NULL,
    source_system TEXT NOT NULL,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    processing_batch_id TEXT NULL,
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    last_processing_error TEXT NULL,
    city_id TEXT UNIQUE NOT NULL,
    city_name TEXT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_cities_is_processed ON raw_cities (is_processed);
CREATE INDEX IF NOT EXISTS idx_raw_cities_city_id ON raw_cities (city_id);

-- 6. Sync Checkpoints Table
CREATE TABLE IF NOT EXISTS sync_checkpoints (
    table_name TEXT PRIMARY KEY,
    last_sync_key TEXT NULL,
    last_reconcile_key TEXT NULL,
    last_sync_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cycle_id TEXT NULL,
    status TEXT NOT NULL DEFAULT 'SUCCESS',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 7. External Locks Table
CREATE TABLE IF NOT EXISTS sync_locks (
    lock_name TEXT PRIMARY KEY,
    locked_by TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

-- 8. Dead Letter Queue / Sync Failures Table
CREATE TABLE IF NOT EXISTS sync_failures (
    id SERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    primary_key TEXT NOT NULL,
    error TEXT NOT NULL,
    payload TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sync_failures_source_table ON sync_failures (source_table);

-- 9. Sync Run Auditing Table
CREATE TABLE IF NOT EXISTS sync_runs (
    cycle_id TEXT PRIMARY KEY,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NULL,
    duration DOUBLE PRECISION NULL,
    rows_scanned INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    rows_failed INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL
);
