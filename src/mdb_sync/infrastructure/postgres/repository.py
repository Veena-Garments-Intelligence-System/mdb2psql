from typing import Optional, Type, TypeVar
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from src.mdb_sync.infrastructure.postgres.models import Base, SyncState, SyncFingerprint, ExternalLock, SyncCheckpoint, SyncFailure, SyncRun
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, delete
import time
from src.mdb_sync.utils.metrics import POSTGRES_LATENCY
from src.mdb_sync.utils.circuit_breaker import postgres_breaker

def utcnow():
    return datetime.now(timezone.utc)

T = TypeVar("T", bound=Base)

class PostgresRepository:
    def __init__(self, session: Session):
        self.session = session

    def _execute_with_breaker(self, operation: str, func, *args, **kwargs):
        start = time.time()
        try:
            return postgres_breaker(func, *args, **kwargs)
        finally:
            elapsed_ms = (time.time() - start) * 1000
            POSTGRES_LATENCY.labels(operation=operation).observe(elapsed_ms)

    def acquire_lock(self, lock_name: str, locked_by: str, timeout_minutes: int = 10) -> bool:
        """Tries to acquire a named lock. Returns True if successful."""
        def _op():
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
        return self._execute_with_breaker("acquire_lock", _op)

    def release_lock(self, lock_name: str, locked_by: str):
        """Releases a named lock if held by the caller."""
        def _op():
            self.session.execute(
                delete(ExternalLock).where(
                    ExternalLock.lock_name == lock_name,
                    ExternalLock.locked_by == locked_by
                )
            )
        self._execute_with_breaker("release_lock", _op)

    def upsert_batch(self, model_class: Type[T], data_list: list[dict], unique_col: str):
        if not data_list:
            return

        def _op():
            stmt = insert(model_class).values(data_list)
            first_row = data_list[0]
            
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
        self._execute_with_breaker("upsert_batch", _op)

    def upsert(self, model_class: Type[T], data: dict, unique_col: str):
        def _op():
            stmt = insert(model_class).values(**data)
            
            exclude = {unique_col, 'raw_id'}
            if 'created_at' not in data:
                exclude.add('created_at')

            update_dict = {
                k: v for k, v in stmt.excluded.items() 
                if k not in exclude
            }
            
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
        self._execute_with_breaker("upsert", _op)

    def get_sync_state(self, table_name: str) -> Optional[SyncState]:
        def _op():
            return self.session.get(SyncState, table_name)
        return self._execute_with_breaker("get_sync_state", _op)

    def update_sync_state(self, table_name: str, last_pk: Optional[str] = None, last_reconcile_pk: Optional[str] = None):
        def _op():
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
        self._execute_with_breaker("update_sync_state", _op)

    def get_checkpoint(self, table_name: str) -> Optional[SyncCheckpoint]:
        def _op():
            return self.session.get(SyncCheckpoint, table_name)
        return self._execute_with_breaker("get_checkpoint", _op)

    def update_checkpoint(self, table_name: str, last_sync_key: Optional[str] = None, last_reconcile_key: Optional[str] = None, cycle_id: Optional[str] = None, status: str = "SUCCESS"):
        def _op():
            checkpoint = self.get_checkpoint(table_name)
            now = utcnow()
            if checkpoint:
                if last_sync_key is not None:
                    checkpoint.last_sync_key = last_sync_key
                if last_reconcile_key is not None:
                    checkpoint.last_reconcile_key = last_reconcile_key
                checkpoint.last_sync_timestamp = now
                if cycle_id is not None:
                    checkpoint.cycle_id = cycle_id
                checkpoint.status = status
            else:
                checkpoint = SyncCheckpoint(
                    table_name=table_name,
                    last_sync_key=last_sync_key,
                    last_reconcile_key=last_reconcile_key,
                    last_sync_timestamp=now,
                    cycle_id=cycle_id,
                    status=status
                )
                self.session.add(checkpoint)
        self._execute_with_breaker("update_checkpoint", _op)

    def log_sync_failure(self, source_table: str, primary_key: str, error: str, payload: str):
        def _op():
            failure = SyncFailure(
                source_table=source_table,
                primary_key=primary_key,
                error=error,
                payload=payload,
                timestamp=utcnow()
            )
            self.session.add(failure)
        self._execute_with_breaker("log_sync_failure", _op)

    def start_sync_run(self, cycle_id: str, start_time: datetime) -> SyncRun:
        def _op():
            run = SyncRun(
                cycle_id=cycle_id,
                start_time=start_time,
                status="RUNNING"
            )
            self.session.add(run)
            self.session.flush()
            return run
        return self._execute_with_breaker("start_sync_run", _op)

    def end_sync_run(self, cycle_id: str, end_time: datetime, duration: float, rows_scanned: int, rows_updated: int, rows_failed: int, status: str):
        def _op():
            run = self.session.get(SyncRun, cycle_id)
            if run:
                run.end_time = end_time
                run.duration = duration
                run.rows_scanned = rows_scanned
                run.rows_updated = rows_updated
                run.rows_failed = rows_failed
                run.status = status
        self._execute_with_breaker("end_sync_run", _op)

    def get_fingerprint(self, table_name: str, entity_id: str) -> Optional[SyncFingerprint]:
        def _op():
            return self.session.get(SyncFingerprint, (table_name, entity_id))
        return self._execute_with_breaker("get_fingerprint", _op)

    def update_fingerprints_batch(self, table_name: str, fingerprint_data: list[tuple[str, str]]):
        if not fingerprint_data:
            return
            
        def _op():
            now = utcnow()
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
            
            stmt = insert(SyncFingerprint).values(values)
            update_stmt = stmt.on_conflict_do_update(
                index_elements=['table_name', 'entity_id'],
                set_={
                    "checksum": stmt.excluded.checksum,
                    "last_seen_at": now,
                    "last_changed_at": stmt.excluded.last_changed_at
                }
            )
            self.session.execute(update_stmt)
        self._execute_with_breaker("update_fingerprints_batch", _op)

    def prune_processed_rows(self, table_name: str, retention_days: int):
        def _op():
            from sqlalchemy import text
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
        return self._execute_with_breaker("prune_processed_rows", _op)

    def cleanup_stale_runs(self):
        """Identifies and marks 'RUNNING' runs from previous instances as 'INTERRUPTED'."""
        def _op():
            from sqlalchemy import update
            self.session.execute(
                update(SyncRun)
                .where(SyncRun.status == "RUNNING")
                .values(status="INTERRUPTED", end_time=utcnow())
            )
        self._execute_with_breaker("cleanup_stale_runs", _op)
