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
    is_ok INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_raw_sales_is_processed ON raw_sales (is_processed);
CREATE INDEX IF NOT EXISTS idx_raw_sales_bill_id ON raw_sales (bill_id);
CREATE INDEX IF NOT EXISTS idx_raw_sales_customer_id ON raw_sales (customer_id);

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
