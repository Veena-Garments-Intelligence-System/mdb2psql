import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import Boolean, DateTime, Numeric, Text, Integer, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, index=True)

class IngestionMixin(TimestampMixin):
    raw_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    checksum: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_system: Mapped[str] = mapped_column(Text, nullable=False)
    is_processed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    processing_batch_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    processing_attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_processing_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

class RawCustomer(Base, IngestionMixin):
    __tablename__ = "raw_customers"
    customer_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    customer_name: Mapped[Optional[str]] = mapped_column(Text)
    city_id: Mapped[Optional[str]] = mapped_column(Text)
    mobile1: Mapped[Optional[str]] = mapped_column(Text)
    opening_balance: Mapped[Optional[float]] = mapped_column(Numeric)

class RawCity(Base, IngestionMixin):
    __tablename__ = "raw_cities"
    city_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    city_name: Mapped[Optional[str]] = mapped_column(Text)
    group_id: Mapped[Optional[str]] = mapped_column(Text)

class RawSale(Base, IngestionMixin):
    __tablename__ = "raw_sales"
    bill_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    customer_id: Mapped[Optional[str]] = mapped_column(Text)
    bill_date: Mapped[Optional[str]] = mapped_column(Text)
    net_amount: Mapped[Optional[float]] = mapped_column(Numeric)
    dis_amt: Mapped[Optional[float]] = mapped_column(Numeric)
    is_ok: Mapped[int] = mapped_column(Integer, default=0)
    bill_amount: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    dis: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    packing_case: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    packing_amount: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    transport_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transport_amount: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    total_packs: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    bdl: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    dispatch_case: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    dispatch_bdl: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    fright: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    gross_amt5: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    gross_amt12: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    total_tax5: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    total_tax12: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    is_gst: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    gr_no: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_cash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    customer_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    city_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    user_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    add_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    bill_group: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ref_no: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    company_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    session_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

class RawBillDetail(Base, IngestionMixin):
    __tablename__ = "raw_bill_details"
    __table_args__ = (
        UniqueConstraint('bill_id', 'line_idx', name='uq_raw_bill_details_bill_line'),
    )
    bill_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    line_idx: Mapped[int] = mapped_column(Integer, nullable=False)
    item_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    item_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    size_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    qty: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    rate: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    scheme: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    amount: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    sr: Mapped[bool] = mapped_column(Boolean, default=False)
    mtsr: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    company_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    group_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stock_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    unit: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    netrate: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    taxcode: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    discount: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    attend_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    inc_per: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    bill_type: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hbr: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

class RawLRMaster(Base, IngestionMixin):
    __tablename__ = "raw_lr_master"
    __table_args__ = (
        UniqueConstraint('lr_id', 'entry_idx', name='uq_raw_lr_master_lr_entry'),
    )
    lr_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    entry_idx: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    bill_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    company_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sno: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    customer_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    entry_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    customer_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    city_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    total_packs: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    gr_no: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    lr_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transport_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ps: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    printed: Mapped[bool] = mapped_column(Boolean, default=False)
    is_lock: Mapped[bool] = mapped_column(Boolean, default=False)
    packing_case: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    packing_amount: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    transport_amount: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    is_update: Mapped[bool] = mapped_column(Boolean, default=False)
    user_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    add_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    add_time: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_edit_lock: Mapped[bool] = mapped_column(Boolean, default=False)
    is_ok: Mapped[int] = mapped_column(Integer, default=0)
    lock_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    bdl: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    bill_group: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ref_no: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    is_dispatch: Mapped[bool] = mapped_column(Boolean, default=False)
    clear_remark: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    lock_user: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    lock_user_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    checked: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_lr: Mapped[bool] = mapped_column(Boolean, default=False)
    pickup_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_bus: Mapped[bool] = mapped_column(Boolean, default=False)
    is_sms_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    auto_freight: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

class RawLRDetail(Base, IngestionMixin):
    __tablename__ = "raw_lr_details"
    __table_args__ = (
        UniqueConstraint('lr_id', 'bill_id', 'line_idx', name='uq_raw_lr_details_lr_bill_line'),
    )
    lr_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    bill_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    line_idx: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    sno: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    case_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    bdl: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    dcase: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    dbdl: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    rsno: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    freight: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

class RawDeleteRecord(Base):
    __tablename__ = "raw_delete_records"
    raw_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    checksum: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_system: Mapped[str] = mapped_column(Text, nullable=False)
    bill_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    bill_no: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    bill_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    customer_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    city_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    net_amount: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    user_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    delete_date: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    delete_time: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    remark: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class RawReceipt(Base, IngestionMixin):
    __tablename__ = "raw_receipts"
    receipt_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    customer_id: Mapped[Optional[str]] = mapped_column(Text)
    receipt_date: Mapped[Optional[str]] = mapped_column(Text)
    amount: Mapped[Optional[float]] = mapped_column(Numeric)
    discount: Mapped[Optional[float]] = mapped_column(Numeric)
    bank_name: Mapped[Optional[str]] = mapped_column(Text)
    receipt_type: Mapped[Optional[str]] = mapped_column(Text)
    is_ok: Mapped[int] = mapped_column(Integer, default=0)

class RawRG(Base):
    __tablename__ = "raw_rg"
    raw_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    checksum: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_system: Mapped[str] = mapped_column(Text, nullable=False)
    is_processed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    processing_batch_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    processing_attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_processing_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    rg_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    customer_id: Mapped[Optional[str]] = mapped_column(Text)
    rgtype: Mapped[Optional[str]] = mapped_column(Text)
    bill_date: Mapped[Optional[str]] = mapped_column(Text)
    net_amount: Mapped[Optional[float]] = mapped_column(Numeric)
    is_ok: Mapped[int] = mapped_column(Integer, default=0)


class ExternalLock(Base):
    __tablename__ = "sync_locks"
    lock_name: Mapped[str] = mapped_column(Text, primary_key=True)
    locked_by: Mapped[str] = mapped_column(Text, nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class SyncCheckpoint(Base):
    __tablename__ = "sync_checkpoints"
    table_name: Mapped[str] = mapped_column(Text, primary_key=True)
    last_sync_key: Mapped[Optional[str]] = mapped_column(Text)
    last_reconcile_key: Mapped[Optional[str]] = mapped_column(Text)
    last_sync_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    cycle_id: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="SUCCESS")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class SyncFailure(Base):
    __tablename__ = "sync_failures"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_table: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    primary_key: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    error: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)

class SyncRun(Base):
    __tablename__ = "sync_runs"
    cycle_id: Mapped[str] = mapped_column(Text, primary_key=True)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    duration: Mapped[Optional[float]] = mapped_column(Numeric)
    rows_scanned: Mapped[int] = mapped_column(Integer, default=0)
    rows_updated: Mapped[int] = mapped_column(Integer, default=0)
    rows_failed: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(Text, nullable=False) # RUNNING, SUCCESS, FAILED

