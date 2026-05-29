from typing import Optional, Type, TypeVar
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from src.mdb_sync.infrastructure.postgres.models import Base, SyncState, SyncFingerprint, ExternalLock
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, delete

def utcnow():
    return datetime.now(timezone.utc)

T = TypeVar("T", bound=Base)

class PostgresRepository:
    def __init__(self, session: Session):
        self.session = session

    def acquire_lock(self, lock_name: str, locked_by: str, timeout_minutes: int = 10) -> bool:
        """Tries to acquire a named lock. Returns True if successful."""
        now = utcnow()
        expires_at = now + timedelta(minutes=timeout_minutes)
        
        # 1. Clean up expired locks first
        self.session.execute(
            delete(ExternalLock).where(ExternalLock.expires_at < now)
        )
        
        # 2. Check if lock is currently held
        existing = self.session.execute(
            select(ExternalLock).where(ExternalLock.lock_name == lock_name)
        ).scalar_one_or_none()
        
        if existing:
            if existing.locked_by == locked_by:
                # We already have it, extend it
                existing.expires_at = expires_at
                return True
            return False
            
        # 3. Try to insert
        try:
            new_lock = ExternalLock(
                lock_name=lock_name,
                locked_by=locked_by,
                acquired_at=now,
                expires_at=expires_at
            )
            self.session.add(new_lock)
            self.session.flush() # Check for integrity errors
            return True
        except Exception:
            self.session.rollback()
            return False

    def release_lock(self, lock_name: str, locked_by: str):
        """Releases a named lock if held by the caller."""
        self.session.execute(
            delete(ExternalLock).where(
                ExternalLock.lock_name == lock_name,
                ExternalLock.locked_by == locked_by
            )
        )

    def upsert_batch(self, model_class: Type[T], data_list: list[dict], unique_col: str):
        if not data_list:
            return

        stmt = insert(model_class).values(data_list)
        
        # Determine which columns to update
        # We use the first record to identify the fields
        first_row = data_list[0]
        
        # We don't update the unique col or raw_id
        # We ONLY exclude created_at if it's NOT in the provided data
        # (This allows us to "fix" dates in Postgres if they change in MDB)
        exclude = {unique_col, 'raw_id'}
        if 'created_at' not in first_row:
            exclude.add('created_at')
            
        update_cols = [
            k for k in first_row.keys()
            if k not in exclude
        ]
        
        update_dict = {col: getattr(stmt.excluded, col) for col in update_cols}
        
        if 'checksum' in update_dict:
            stmt = stmt.on_conflict_do_update(
                index_elements=[unique_col],
                set_=update_dict,
                where=(getattr(model_class, 'checksum') != stmt.excluded.checksum)
            )
        else:
            stmt = stmt.on_conflict_do_update(
                index_elements=[unique_col],
                set_=update_dict
            )
            
        self.session.execute(stmt)

    def upsert(self, model_class: Type[T], data: dict, unique_col: str):
        stmt = insert(model_class).values(**data)
        
        # We don't update the unique col or raw_id
        # We ONLY exclude created_at if it's NOT in the provided data
        exclude = {unique_col, 'raw_id'}
        if 'created_at' not in data:
            exclude.add('created_at')

        update_dict = {
            k: v for k, v in stmt.excluded.items() 
            if k not in exclude
        }
        
        # We only update if the checksums differ
        # Assuming the table has a checksum column if it's a raw ingestion table
        if 'checksum' in update_dict:
            stmt = stmt.on_conflict_do_update(
                index_elements=[unique_col],
                set_=update_dict,
                where=(getattr(model_class, 'checksum') != stmt.excluded.checksum)
            )
        else:
            stmt = stmt.on_conflict_do_update(
                index_elements=[unique_col],
                set_=update_dict
            )
            
        self.session.execute(stmt)

    def get_sync_state(self, table_name: str) -> Optional[SyncState]:
        return self.session.get(SyncState, table_name)

    def update_sync_state(self, table_name: str, last_pk: Optional[str] = None, last_reconcile_pk: Optional[str] = None):
        state = self.get_sync_state(table_name)
        if state:
            if last_pk is not None:
                state.last_pk = last_pk
            if last_reconcile_pk is not None:
                state.last_reconcile_pk = last_reconcile_pk
            state.last_sync_at = utcnow()
        else:
            state = SyncState(table_name=table_name, last_pk=last_pk, last_reconcile_pk=last_reconcile_pk, last_sync_at=utcnow())
            self.session.add(state)

    def get_fingerprint(self, table_name: str, entity_id: str) -> Optional[SyncFingerprint]:
        return self.session.get(SyncFingerprint, (table_name, entity_id))

    def update_fingerprints_batch(self, table_name: str, fingerprint_data: list[tuple[str, str]]):
        """Batch update fingerprints. data: list of (entity_id, checksum)"""
        if not fingerprint_data:
            return
            
        now = utcnow()
        # Using PostgreSQL specific upsert for fingerprints
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        
        values = [
            {
                "table_name": table_name,
                "entity_id": eid,
                "checksum": cs,
                "first_seen_at": now,
                "last_seen_at": now,
                "last_changed_at": now
            }
            for eid, cs in fingerprint_data
        ]
        
        stmt = pg_insert(SyncFingerprint).values(values)
        update_stmt = stmt.on_conflict_do_update(
            index_elements=['table_name', 'entity_id'],
            set_={
                "checksum": stmt.excluded.checksum,
                "last_seen_at": now,
                "last_changed_at": pg_insert(SyncFingerprint).excluded.last_changed_at # Default behavior
            },
            # Only update last_changed_at if checksum differs
            where=(SyncFingerprint.checksum != stmt.excluded.checksum)
        )
        
        # We need a custom way to handle last_changed_at because it should only update on diff
        # Simple approach for now: just update checksum and last_seen. 
        # If we need exact last_changed_at, we'd use a CASE statement in the SET clause.
        
        update_stmt = stmt.on_conflict_do_update(
            index_elements=['table_name', 'entity_id'],
            set_={
                "checksum": stmt.excluded.checksum,
                "last_seen_at": now,
                "last_changed_at": pg_insert(SyncFingerprint).excluded.last_changed_at
            }
        )
        # Re-simplifying for reliability
        self.session.execute(update_stmt)

    def prune_processed_rows(self, table_name: str, retention_days: int):
        from sqlalchemy import text
        # Only prune rows that are is_processed = TRUE and older than retention_days
        # For tables like raw_rg, we use created_at as updated_at was removed.
        ts_col = "updated_at"
        if table_name == "raw_rg":
            ts_col = "created_at"
            
        query = text(f"""
            DELETE FROM {table_name}
            WHERE is_processed = TRUE
            AND {ts_col} < NOW() - INTERVAL '{retention_days} days'
        """)
        result = self.session.execute(query)
        return result.rowcount

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

