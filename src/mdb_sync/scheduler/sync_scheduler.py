import threading
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional
from src.mdb_sync.application.sync_engine import SyncEngine
from src.mdb_sync.logging_config import get_logger
from src.mdb_sync.config import settings
from src.mdb_sync.infrastructure.postgres.database import SessionLocal
from src.mdb_sync.infrastructure.postgres.repository import PostgresRepository

logger = get_logger(__name__)

class SyncScheduler:
    def __init__(self, sync_engine: SyncEngine):
        self.sync_engine = sync_engine
        self.interval = settings.SYNC_INTERVAL_SECONDS
        self.prune_interval = settings.PRUNE_INTERVAL_SECONDS
        self._stop_event = threading.Event()
        self._last_prune_at: Optional[datetime] = None
        self.max_workers = 2 # Reduced for driver stability
        
        # Attach signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, signum=None, frame=None):
        logger.info("Shutdown signal received. Stopping gracefully...")
        self._stop_event.set()

    def _sync_table_isolated(self, table: str, mode: str) -> dict:
        if self._stop_event.is_set():
            return {}
            
        logger.info("Syncing table", table=table, mode=mode)
        
        # Strictly isolate session AND engine per thread
        with SessionLocal() as db:
            try:
                pg_repo = PostgresRepository(db)
                # Create a local engine instance for thread safety
                local_engine = SyncEngine(self.sync_engine.mdb_repo)
                local_engine.set_pg_repo(pg_repo)
                
                result = {}
                # We apply the mdb_lock only during the initialization of the generator
                # to prevent multiple concurrent pyodbc.connect() calls if needed.
                # However, many MDB drivers are not thread-safe during iteration.
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
                return {"table": table, "mode": mode, "error": err_msg, "scanned": 0, "upserted": 0, "errors": 1}

    def run_pruning(self) -> dict:
        logger.info("Starting pruning cycle")
        prune_results = {}
        # We now use a global retention for processed rows as requested
        retention = settings.PRUNE_RETENTION_DAYS_PROCESSED
        prune_targets = ["raw_sales", "raw_receipts", "raw_rg", "raw_customers", "raw_cities"]
        
        for table in prune_targets:
            if self._stop_event.is_set():
                break
            with SessionLocal() as db:
                try:
                    pg_repo = PostgresRepository(db)
                    count = pg_repo.prune_processed_rows(table, retention)
                    db.commit()
                    if count > 0:
                        logger.info("Pruned table", table=table, count=count)
                        prune_results[table] = count
                except Exception as e:
                    db.rollback()
                    err_msg = str(e).split('\n')[0]
                    logger.error("Pruning failed", table=table, error=err_msg)
                    prune_results[table] = f"Error: {err_msg}"
        
        self._last_prune_at = datetime.now(timezone.utc)
        return prune_results

    def run_once(self):
        start_time = datetime.now(timezone.utc)
        from datetime import timedelta
        
        # 1. ACQUIRE GLOBAL MDB LOCK (POSTGRES-BASED)
        # This prevents concurrent access between dbupdater and intelligence system
        lock_acquired = False
        wait_start = datetime.now(timezone.utc)
        max_wait = timedelta(minutes=settings.LOCK_MAX_WAIT_MINUTES)
        
        try:
            while not lock_acquired and not self._stop_event.is_set():
                with SessionLocal() as db:
                    pg_repo = PostgresRepository(db)
                    # We use a 20-minute timeout, but we should ideally refresh it 
                    # if the sync takes longer. For now, 20m is the hard limit.
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

            logger.debug("Starting parallel sync cycle")
            
            # 2. Pruning (Priority: Cleanup first)
            # We now run pruning on every cycle as requested by the user
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
                future_to_task = {executor.submit(self._sync_table_isolated, t, m): (t, m) for t, m in tasks}
                
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
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            total_scanned = sum(r.get("scanned", 0) for r in cycle_results)
            total_upserted = sum(r.get("upserted", 0) + r.get("updated", 0) for r in cycle_results)
            total_errors = sum(r.get("errors", 0) for r in cycle_results)
            
            failed_tables = [f"{r['table']} ({r['mode']})" for r in cycle_results if "error" in r]
            
            rows_per_sec = total_scanned / duration if duration > 0 else 0
            
            log_data = {
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

            if total_errors > 0 or failed_tables:
                logger.warning("Sync cycle completed with issues", **log_data)
            else:
                logger.info("Sync cycle completed successfully", **log_data)
        
        finally:
            # 5. RELEASE GLOBAL MDB LOCK
            if lock_acquired:
                with SessionLocal() as db:
                    pg_repo = PostgresRepository(db)
                    pg_repo.release_lock("MDB_FILE_LOCK", "dbupdater")
                    db.commit()
                logger.debug("Released MDB lock")

    def start(self):
        logger.info("Starting scheduler", interval=self.interval, prune_interval=self.prune_interval)
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as e:
                logger.critical("Critical error in sync loop", error=str(e))
            
            if self._stop_event.is_set():
                break
                
            logger.debug("Sleeping", seconds=self.interval)
            self._stop_event.wait(self.interval)
