import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import Boolean, DateTime, Numeric, Text, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, index=True)

class IngestionMixin(TimestampMixin):
    raw_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    checksum: Mapped[str] = mapped_column(Text, nullable=False)
    source_system: Mapped[str] = mapped_column(Text, nullable=False)
    is_processed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)

class RawCustomer(Base, IngestionMixin):
    __tablename__ = "raw_customers"
    customer_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    customer_name: Mapped[Optional[str]] = mapped_column(Text)
    city_id: Mapped[Optional[str]] = mapped_column(Text)
    mobile1: Mapped[Optional[str]] = mapped_column(Text)

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

class RawReceipt(Base, IngestionMixin):
    __tablename__ = "raw_receipts"
    receipt_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    customer_id: Mapped[Optional[str]] = mapped_column(Text)
    receipt_date: Mapped[Optional[str]] = mapped_column(Text)
    amount: Mapped[Optional[float]] = mapped_column(Numeric)
    discount: Mapped[Optional[float]] = mapped_column(Numeric)
    bank_name: Mapped[Optional[str]] = mapped_column(Text)
    receipt_type: Mapped[Optional[str]] = mapped_column(Text)

class RawRG(Base):
    __tablename__ = "raw_rg"
    raw_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    checksum: Mapped[str] = mapped_column(Text, nullable=False)
    source_system: Mapped[str] = mapped_column(Text, nullable=False)
    is_processed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    rg_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    customer_id: Mapped[Optional[str]] = mapped_column(Text)
    rgtype: Mapped[Optional[str]] = mapped_column(Text)
    bill_date: Mapped[Optional[str]] = mapped_column(Text)
    net_amount: Mapped[Optional[float]] = mapped_column(Numeric)

class SyncFingerprint(Base):
    __tablename__ = "sync_fingerprints"
    table_name: Mapped[str] = mapped_column(Text, primary_key=True)
    entity_id: Mapped[str] = mapped_column(Text, primary_key=True)
    checksum: Mapped[str] = mapped_column(Text, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)
    last_changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    
    __table_args__ = (PrimaryKeyConstraint('table_name', 'entity_id'),)

class SyncState(Base):
    __tablename__ = "sync_state"
    table_name: Mapped[str] = mapped_column(Text, primary_key=True)
    last_pk: Mapped[Optional[str]] = mapped_column(Text)
    last_reconcile_pk: Mapped[Optional[str]] = mapped_column(Text)
    last_sync_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

class ExternalLock(Base):
    __tablename__ = "sync_locks"
    lock_name: Mapped[str] = mapped_column(Text, primary_key=True)
    locked_by: Mapped[str] = mapped_column(Text, nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
