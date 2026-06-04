import threading
import signal
import time
import uuid
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional
from src.mdb_sync.application.sync_engine import SyncEngine
from src.mdb_sync.logging_config import get_logger
from src.mdb_sync.config import settings
from src.mdb_sync.infrastructure.postgres.database import SessionLocal, recreate_db_engine
from src.mdb_sync.infrastructure.postgres.repository import PostgresRepository
from src.mdb_sync.utils.metrics import PRUNING_DURATION
from src.mdb_sync.utils.circuit_breaker import postgres_breaker

logger = get_logger(__name__)

class SyncScheduler:
    def __init__(self, sync_engine: SyncEngine):
        self.sync_engine = sync_engine
        self.interval = settings.SYNC_INTERVAL_SECONDS
        self.prune_interval = settings.PRUNE_INTERVAL_SECONDS
        self._stop_event = threading.Event()
        self._last_prune_at: Optional[datetime] = None
        self.max_workers = 2
        
        # Attach signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, signum=None, frame=None):
        logger.info("Shutdown signal received. Stopping gracefully...")
        self._stop_event.set()
        
        # Dispose connection pool on graceful shutdown (Requirement 10)
        try:
            from src.mdb_sync.infrastructure.postgres.database import engine
            engine.dispose()
            logger.info("PostgreSQL database connection pool disposed successfully.")
        except Exception as e:
            logger.warning("Failed to dispose connection pool on stop", error=str(e))

    def _sync_table_isolated(self, table: str, mode: str, cycle_id: str) -> dict:
        if self._stop_event.is_set():
            return {}
            
        logger.info("Syncing table", table=table, mode=mode, cycle_id=cycle_id)
        
        with SessionLocal() as db:
            try:
                pg_repo = PostgresRepository(db)
                local_engine = SyncEngine(self.sync_engine.mdb_repo, cycle_id=cycle_id, stop_event=self._stop_event)
                local_engine.set_pg_repo(pg_repo)
                
                result = {}
                if mode == "full":
                    result = local_engine.sync_table_full(table)
                elif mode == "incremental":
                    result = local_engine.sync_table_incremental(table)
                elif mode == "reconcile":
                    result = local_engine.reconcile_table_chunk(table, chunk_limit=settings.RECONCILIATION_CHUNK_LIMIT)
                
                if result:
                    logger.info("Table sync completed", 
                                table=table, 
                                mode=mode, 
                                scanned=result.get("scanned"), 
                                upserted=result.get("upserted", 0) + result.get("updated", 0),
                                errors=result.get("errors"))
                return result
            except Exception as e:
                db.rollback()
                err_msg = str(e).split('\n')[0]
                logger.error("Table sync failed", table=table, mode=mode, error=err_msg)
                
                # Table-Level Connection Recovery: Trigger engine recreation if connection lost (Requirement 6)
                from sqlalchemy.exc import OperationalError
                if isinstance(e, OperationalError) or "connection" in err_msg.lower() or "dns" in err_msg.lower() or "timeout" in err_msg.lower():
                    logger.warning("Operational connectivity error detected in worker thread. Resetting connection pool...", table=table)
                    recreate_db_engine()
                    
                return {"table": table, "mode": mode, "error": err_msg, "scanned": 0, "upserted": 0, "errors": 1}

    def run_pruning(self) -> dict:
        logger.info("Starting pruning cycle")
        prune_summary = {}
        retention = settings.PRUNE_RETENTION_DAYS_PROCESSED
        prune_targets = ["raw_sales", "raw_receipts", "raw_rg", "raw_customers", "raw_cities"]
        
        for table in prune_targets:
            if self._stop_event.is_set():
                break
            start_time = time.time()
            with SessionLocal() as db:
                try:
                      pg_repo = PostgresRepository(db)
                      count = pg_repo.prune_processed_rows(table, retention)
                      db.commit()
                      if count > 0:
                          logger.info("Pruned table", table=table, count=count)
                          prune_summary[table] = count
                except Exception as e:
                    db.rollback()
                    err_msg = str(e).split('\n')[0]
                    logger.error("Pruning failed", table=table, error=err_msg)
                    prune_summary[table] = f"Error: {err_msg}"
                finally:
                    duration = time.time() - start_time
                    PRUNING_DURATION.labels(table=table).observe(duration)
        
        self._cleanup_old_logs(days=7)
        self._last_prune_at = datetime.now(timezone.utc)
        return prune_summary

    def _cleanup_old_logs(self, days: int = 7):
        """Removes log files in the 'logs' directory older than the specified number of days."""
        import os
        log_dir = "logs"
        if not os.path.exists(log_dir):
            return

        now = time.time()
        cutoff = now - (days * 86400)
        
        try:
            for filename in os.listdir(log_dir):
                file_path = os.path.join(log_dir, filename)
                if os.path.isfile(file_path):
                    file_mtime = os.path.getmtime(file_path)
                    if file_mtime < cutoff:
                        os.remove(file_path)
                        logger.info("Removed old log file", filename=filename)
        except Exception as e:
            logger.error("Log cleanup failed", error=str(e))

    def run_once(self):
        cycle_id = str(uuid.uuid4())
        start_time = datetime.now(timezone.utc)
        from datetime import timedelta
        
        logger.info("Sync Cycle Started", cycle_id=cycle_id)
        
        # 1. Record the cycle start in sync_runs
        with SessionLocal() as db:
            pg_repo = PostgresRepository(db)
            pg_repo.start_sync_run(cycle_id, start_time)
            db.commit()

        lock_acquired = False
        wait_start = datetime.now(timezone.utc)
        max_wait = timedelta(minutes=settings.LOCK_MAX_WAIT_MINUTES)
        
        try:
            while not lock_acquired and not self._stop_event.is_set():
                with SessionLocal() as db:
                    pg_repo = PostgresRepository(db)
                    lock_acquired = pg_repo.acquire_lock("MDB_FILE_LOCK", "dbupdater", timeout_minutes=20)
                    db.commit()
                
                if not lock_acquired:
                    elapsed = datetime.now(timezone.utc) - wait_start
                    if elapsed > max_wait:
                        logger.warning("Timed out waiting for MDB lock. Skipping this cycle.", waited_sec=elapsed.total_seconds())
                        return
                    
                    logger.info("MDB file is currently locked. Retrying soon...", 
                                waited_sec=round(elapsed.total_seconds()),
                                retry_in=settings.LOCK_RETRY_INTERVAL_SECONDS)
                    self._stop_event.wait(settings.LOCK_RETRY_INTERVAL_SECONDS)

            if self._stop_event.is_set():
                return

            logger.debug("Starting parallel sync cycle", cycle_id=cycle_id)
            
            # 2. Pruning
            prune_summary = self.run_pruning()

            # 3. Prepare Sync Tasks
            tasks = []
            # Master Tables
            master_tables = ["City_Master", "CUSTOMER_MASTER"]
            for table in master_tables:
                tasks.append((table, "full"))

            # Transactional Tables
            transactional_tables = ["BILL_MASTER", "Receipt_Master", "ReturnGoods"]
            for table in transactional_tables:
                tasks.append((table, "incremental"))
                tasks.append((table, "reconcile"))

            # 4. Execute Sync Parallelly
            cycle_results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_task = {executor.submit(self._sync_table_isolated, t, m, cycle_id): (t, m) for t, m in tasks}
                
                for future in as_completed(future_to_task):
                    try:
                        result = future.result()
                        if result:
                            cycle_results.append(result)
                    except Exception as e:
                        t, m = future_to_task[future]
                        err_msg = str(e).split('\n')[0]
                        cycle_results.append({"table": t, "mode": m, "error": err_msg, "errors": 1})

            # Final Cycle Summary
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            total_scanned = sum(r.get("scanned", 0) for r in cycle_results)
            total_upserted = sum(r.get("upserted", 0) + r.get("updated", 0) for r in cycle_results)
            total_errors = sum(r.get("errors", 0) for r in cycle_results)
            
            failed_tables = [f"{r['table']} ({r['mode']})" for r in cycle_results if "error" in r]
            status = "SUCCESS" if total_errors == 0 and not failed_tables else "FAILED"
            
            # 5. Record the cycle completion in sync_runs
            with SessionLocal() as db:
                pg_repo = PostgresRepository(db)
                pg_repo.end_sync_run(
                    cycle_id=cycle_id,
                    end_time=end_time,
                    duration=duration,
                    rows_scanned=total_scanned,
                    rows_updated=total_upserted,
                    rows_failed=total_errors,
                    status=status
                )
                db.commit()

            rows_per_sec = total_scanned / duration if duration > 0 else 0
            log_data = {
                "cycle_id": cycle_id,
                "duration_sec": round(duration, 2),
                "rows_per_sec": round(rows_per_sec, 2),
                "scanned": total_scanned,
                "upserted": total_upserted,
                "errors": total_errors,
            }
            if failed_tables:
                log_data["failed_tasks"] = failed_tables
            if prune_summary:
                log_data["pruning"] = prune_summary

            if status == "FAILED":
                logger.warning("Sync cycle completed with issues", **log_data)
            else:
                logger.info("Sync cycle completed successfully", **log_data)
        
        finally:
            if lock_acquired:
                with SessionLocal() as db:
                    pg_repo = PostgresRepository(db)
                    pg_repo.release_lock("MDB_FILE_LOCK", "dbupdater")
                    db.commit()
                logger.debug("Released MDB lock")

    def start(self):
        logger.info("Scheduler Started")
        
        from src.mdb_sync.utils.health import scheduler_state
        scheduler_state.thread = threading.current_thread()
        
        backoff = 5.0
        max_backoff = 300.0
        
        while not self._stop_event.is_set():
            try:
                # A. Circuit Breaker Check
                if not postgres_breaker.is_available():
                    logger.warning("PostgreSQL circuit breaker is OPEN. Sync operations paused.")
                    self._stop_event.wait(30)
                    continue

                # B. MDB File Integrity Checks
                from src.mdb_sync.utils.mdb_check import verify_mdb_integrity
                if not verify_mdb_integrity():
                    logger.warning("MDB file integrity check failed. Sync cycle skipped.")
                    self._stop_event.wait(30)
                    continue

                self.run_once()
                
                # Reset backoff on successful cycle
                backoff = 5.0
                scheduler_state.last_cycle_status = "SUCCESS"
                scheduler_state.last_cycle_completed_at = datetime.now(timezone.utc)
                
            except Exception as e:
                scheduler_state.last_cycle_status = "FAILED"
                err_msg = str(e).split('\n')[0]
                
                # Centralized Automatic Connection Pool Recovery (Requirement 4 & 5)
                logger.error("Sync Cycle Failed. Attempting automatic session recovery...", error=err_msg)
                recreate_db_engine()
                
                # Trigger cycle failure alert
                from src.mdb_sync.utils.alerting import send_alert
                send_alert("sync failure", "Sync Cycle Failed", f"An error occurred in the sync cycle: {err_msg}")
                
                # Wait with exponential backoff and jitter
                jitter = random.uniform(0, 0.5 * backoff)
                sleep_time = backoff + jitter
                logger.info(f"Sync loop cooling down for {sleep_time:.2f} seconds...")
                
                self._stop_event.wait(sleep_time)
                backoff = min(max_backoff, backoff * 2.0)
                continue
            
            if self._stop_event.is_set():
                break
                
            logger.debug("Sleeping", seconds=self.interval)
            self._stop_event.wait(self.interval)
