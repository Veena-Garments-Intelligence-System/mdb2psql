from typing import Optional, Type, TypeVar, Any
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from src.mdb_sync.infrastructure.postgres.models import Base, ExternalLock, SyncCheckpoint, SyncFailure, SyncRun
from datetime import datetime, timezone, timedelta
from sqlalchemy import delete, func
import time
from src.mdb_sync.utils.metrics import POSTGRES_LATENCY
from src.mdb_sync.utils.circuit_breaker import postgres_breaker
from src.mdb_sync.logging_config import get_logger

logger = get_logger(__name__)

UNSET = object()

def utcnow():
    return datetime.now(timezone.utc)

T = TypeVar("T", bound=Base)

class PostgresRepository:
    def __init__(self, session: Session):
        self.session = session

    def commit(self):
        """Commits the current transaction on the session."""
        self.session.commit()

    def rollback(self):
        """Rolls back the current transaction on the session."""
        self.session.rollback()

    def _execute_with_breaker(self, operation: str, func, *args, **kwargs):
        start = time.time()
        try:
            return postgres_breaker(func, *args, **kwargs)
        finally:
            elapsed_ms = (time.time() - start) * 1000
            POSTGRES_LATENCY.labels(operation=operation).observe(elapsed_ms)

    def acquire_lock(self, lock_name: str, locked_by: str, timeout_minutes: int = 10) -> bool:
        """Atomically acquires or extends a named distributed lock. Returns True if successful."""
        def _op():
            now = utcnow()
            expires_at = now + timedelta(minutes=timeout_minutes)
            
            stmt = insert(ExternalLock).values(
                lock_name=lock_name,
                locked_by=locked_by,
                acquired_at=now,
                expires_at=expires_at
            ).on_conflict_do_update(
                index_elements=["lock_name"],
                set_={
                    "locked_by": locked_by,
                    "acquired_at": now,
                    "expires_at": expires_at
                },
                where=(ExternalLock.locked_by == locked_by) | (ExternalLock.expires_at < now)
            )
            res = self.session.execute(stmt)
            return (res.rowcount or 0) > 0
        return self._execute_with_breaker("acquire_lock", _op)

    def release_lock(self, lock_name: str, locked_by: str) -> bool:
        """Releases a named lock if held by the specified caller."""
        def _op():
            res = self.session.execute(
                delete(ExternalLock).where(
                    ExternalLock.lock_name == lock_name,
                    ExternalLock.locked_by == locked_by
                )
            )
            return (res.rowcount or 0) > 0
        return self._execute_with_breaker("release_lock", _op)

    def upsert_batch(self, model_class: Type[T], data_list: list[dict], unique_col: str) -> int:
        if not data_list:
            return 0

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
            if hasattr(model_class, 'updated_at') and 'updated_at' not in update_dict:
                update_dict['updated_at'] = func.now()
            
            # Reset processing attempts and errors for changed rows if columns exist
            if 'processing_attempts' in model_class.__table__.columns and 'processing_attempts' not in update_dict:
                update_dict['processing_attempts'] = 0
            if 'last_processing_error' in model_class.__table__.columns and 'last_processing_error' not in update_dict:
                update_dict['last_processing_error'] = None
            
            if 'checksum' in update_dict:
                stmt = stmt.on_conflict_do_update(
                    index_elements=[unique_col],
                    set_=update_dict,
                    where=(getattr(model_class, 'checksum').is_distinct_from(stmt.excluded.checksum))
                )
            else:
                stmt = stmt.on_conflict_do_update(
                    index_elements=[unique_col],
                    set_=update_dict
                )
                
            res = self.session.execute(stmt)
            return res.rowcount if res and res.rowcount is not None else len(data_list)
        return self._execute_with_breaker("upsert_batch", _op)

    def upsert(self, model_class: Type[T], data: dict, unique_col: str) -> int:
        def _op():
            stmt = insert(model_class).values(**data)
            
            exclude = {unique_col, 'raw_id'}
            if 'created_at' not in data:
                exclude.add('created_at')

            update_dict = {
                k: v for k, v in stmt.excluded.items() 
                if k not in exclude
            }
            if hasattr(model_class, 'updated_at') and 'updated_at' not in update_dict:
                update_dict['updated_at'] = func.now()
            
            # Reset processing attempts and errors for changed rows if columns exist
            if 'processing_attempts' in model_class.__table__.columns and 'processing_attempts' not in update_dict:
                update_dict['processing_attempts'] = 0
            if 'last_processing_error' in model_class.__table__.columns and 'last_processing_error' not in update_dict:
                update_dict['last_processing_error'] = None
            
            if 'checksum' in update_dict:
                stmt = stmt.on_conflict_do_update(
                    index_elements=[unique_col],
                    set_=update_dict,
                    where=(getattr(model_class, 'checksum').is_distinct_from(stmt.excluded.checksum))
                )
            else:
                stmt = stmt.on_conflict_do_update(
                    index_elements=[unique_col],
                    set_=update_dict
                )
                
            res = self.session.execute(stmt)
            return res.rowcount if res and res.rowcount is not None else 1
        return self._execute_with_breaker("upsert", _op)

    def get_checkpoint(self, table_name: str) -> Optional[SyncCheckpoint]:
        def _op():
            return self.session.get(SyncCheckpoint, table_name)
        return self._execute_with_breaker("get_checkpoint", _op)

    def update_checkpoint(
        self,
        table_name: str,
        last_sync_key: Any = UNSET,
        last_reconcile_key: Any = UNSET,
        cycle_id: Any = UNSET,
        status: Any = UNSET,
    ):
        def _op():
            now = utcnow()
            values = {
                "table_name": table_name,
                "last_sync_timestamp": now,
                "updated_at": now,
                "status": "SUCCESS" if status is UNSET else status,
            }
            set_dict = {
                "last_sync_timestamp": now,
                "updated_at": now,
            }
            
            if last_sync_key is not UNSET:
                val = str(last_sync_key) if last_sync_key is not None else None
                values["last_sync_key"] = val
                set_dict["last_sync_key"] = val
            if last_reconcile_key is not UNSET:
                val = str(last_reconcile_key) if last_reconcile_key is not None else None
                values["last_reconcile_key"] = val
                set_dict["last_reconcile_key"] = val
            if cycle_id is not UNSET:
                values["cycle_id"] = cycle_id
                set_dict["cycle_id"] = cycle_id
            if status is not UNSET:
                values["status"] = status
                set_dict["status"] = status
                
            stmt = insert(SyncCheckpoint).values(**values).on_conflict_do_update(
                index_elements=["table_name"],
                set_=set_dict
            )
            self.session.execute(stmt)
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
