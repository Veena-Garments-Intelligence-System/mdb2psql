import time
import json
from datetime import timezone
import threading
from typing import Optional
from src.mdb_sync.config import settings
from src.mdb_sync.infrastructure.mdb.repository import MDBRepository
from src.mdb_sync.infrastructure.postgres.repository import PostgresRepository
from src.mdb_sync.infrastructure.postgres.database import SessionLocal
from src.mdb_sync.application.mapper import DataMapper
from src.mdb_sync.logging_config import get_logger
from src.mdb_sync.utils.metrics import SYNC_DURATION, SYNC_ROWS, SYNC_ERRORS, SYNC_LAG, RECONCILIATION_DURATION

logger = get_logger(__name__)

class SyncEngine:
    def __init__(self, mdb_repo: MDBRepository, cycle_id: Optional[str] = None, stop_event: Optional[threading.Event] = None):
        self.mdb_repo = mdb_repo
        self.pg_repo: Optional[PostgresRepository] = None
        self.cycle_id = cycle_id or "UNKNOWN"
        self.stop_event = stop_event

    def set_pg_repo(self, pg_repo: PostgresRepository):
        self.pg_repo = pg_repo

    def update_lag_metric(self, table_name: str):
        """Calculates sync lag in minutes and alerts if threshold exceeded."""
        config = DataMapper.MAPPING[table_name]
        date_col = None
        for mdb_c, pg_c in config["fields"].items():
            if pg_c in ("bill_date", "receipt_date"):
                date_col = mdb_c
                break
        
        if not date_col:
            return

        if "MDBTools" in self.mdb_repo.conn_str:
            # MDBTools doesn't support aggregate functions like MAX() or sorting.
            # Skip lag check under MDBTools to prevent syntax errors and circuit breaker tripping.
            logger.debug("Skipping source max timestamp check under MDBTools", table=table_name)
            return

        try:
            # 1. Get max timestamp in MDB
            max_src_row = list(self.mdb_repo.execute_query_yield(f"SELECT MAX({date_col}) AS max_date FROM {table_name}"))
            if not max_src_row or not max_src_row[0].get("max_date"):
                return
                
            src_date_str = max_src_row[0]["max_date"]
            src_dt = DataMapper._parse_to_datetime(src_date_str)
            if not src_dt:
                return
                
            # 2. Get max timestamp in PostgreSQL
            pg_model = config["pg_model"]
            with SessionLocal() as db:
                from sqlalchemy import func
                target_max = db.query(func.max(pg_model.created_at)).scalar()
                
            if not target_max:
                return
                
            if target_max.tzinfo is None:
                target_max = target_max.replace(tzinfo=timezone.utc)
                
            # 3. Calculate difference
            lag_seconds = (src_dt - target_max).total_seconds()
            lag_minutes = max(0.0, lag_seconds / 60.0)
            
            SYNC_LAG.labels(table=table_name).set(lag_minutes)
            
            # 4. Generate alert if threshold exceeded
            if lag_minutes > settings.ALERT_LAG_THRESHOLD_MINUTES:
                from src.mdb_sync.utils.alerting import send_alert
                send_alert(
                    "lag threshold exceeded",
                    f"Sync Lag High for {table_name}",
                    f"Sync lag of {round(lag_minutes, 1)} minutes exceeded threshold of {settings.ALERT_LAG_THRESHOLD_MINUTES} minutes.",
                    {"table": table_name, "lag_minutes": lag_minutes, "threshold": settings.ALERT_LAG_THRESHOLD_MINUTES}
                )
        except Exception as e:
            logger.error("Failed to calculate sync lag metric", table=table_name, error=str(e))

    def sync_table_incremental(self, table_name: str) -> dict:
        """Fast path: Appends new records using batch processing."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
            
        start_time = time.time()
        config = DataMapper.MAPPING[table_name]
        
        # Load checkpoint
        checkpoint = self.pg_repo.get_checkpoint(table_name)
        last_pk = checkpoint.last_sync_key if checkpoint else None

        logger.debug("Starting incremental sync", table=table_name, last_pk=last_pk)
        
        row_generator = self.mdb_repo.get_new_records(table_name, config["pk"], last_pk)
        
        first_row_logged = False
        scanned_count = 0
        upserted_count = 0
        error_count = 0
        new_last_pk = last_pk
        batch_size = 1000
        
        data_batch = []
        fingerprint_batch = []

        def flush_batch():
            nonlocal upserted_count, error_count
            if not data_batch:
                return
            batch_start = time.time()
            try:
                # Propose batch insert
                self.pg_repo.upsert_batch(config["pg_model"], data_batch, config["pg_pk"])
                self.pg_repo.update_fingerprints_batch(table_name, fingerprint_batch)
                upserted_count += len(data_batch)
                
                batch_dur_ms = (time.time() - batch_start) * 1000
                logger.info("Batch flush completed", 
                            cycle_id=self.cycle_id, 
                            table=table_name, 
                            batch="incremental_sync", 
                            rows=len(data_batch), 
                            duration_ms=round(batch_dur_ms, 2))
            except Exception as e:
                # Roll back batch and fall back to row-by-row processing (Resilient DLQ)
                self.pg_repo.rollback()
                logger.warning("Batch flush failed. Processing records individually...", table=table_name, error=str(e))
                
                for idx, pg_data in enumerate(data_batch):
                    entity_id = pg_data.get(config["pg_pk"])
                    try:
                        self.pg_repo.upsert(config["pg_model"], pg_data, config["pg_pk"])
                        self.pg_repo.update_fingerprints_batch(table_name, [fingerprint_batch[idx]])
                        self.pg_repo.commit()
                        upserted_count += 1
                    except Exception as row_err:
                        self.pg_repo.rollback()
                        error_count += 1
                        logger.error("Individual row sync failed (sent to DLQ)", table=table_name, pk=entity_id, error=str(row_err))
                        
                        # Write row to dead letter queue
                        try:
                            payload_str = json.dumps(pg_data, default=str)
                            self.pg_repo.log_sync_failure(
                                source_table=table_name,
                                primary_key=str(entity_id),
                                error=str(row_err),
                                payload=payload_str
                            )
                            self.pg_repo.commit()
                        except Exception as dlq_err:
                            logger.error("Failed to write to DLQ", error=str(dlq_err))
                            self.pg_repo.rollback()
            finally:
                data_batch.clear()
                fingerprint_batch.clear()

        for row in row_generator:
            if self.stop_event and self.stop_event.is_set():
                logger.warning("Stop event detected. Stopping incremental sync early...", table=table_name)
                break
            try:
                if not first_row_logged:
                    logger.debug("TABLE SAMPLE", table=table_name, columns=list(row.keys()), sample=str(row))
                    first_row_logged = True

                current_pk = str(row[config["pk"]])
                if last_pk and not (current_pk > last_pk):
                    continue
                    
                scanned_count += 1
                domain_model = DataMapper.map_to_domain(table_name, row)
                pg_data = DataMapper.map_to_pg(table_name, domain_model, settings.SOURCE_SYSTEM)
                
                data_batch.append(pg_data)
                fingerprint_batch.append((str(getattr(domain_model, config["pg_pk"])), domain_model.checksum))
                
                new_last_pk = current_pk
                
                if scanned_count % 1000 == 0:
                    logger.info("Syncing in progress...", table=table_name, scanned=scanned_count, upserted=upserted_count)

                if len(data_batch) >= batch_size:
                    flush_batch()
                    last_rec_key = checkpoint.last_reconcile_key if checkpoint else None
                    self.pg_repo.update_checkpoint(table_name, last_sync_key=new_last_pk, last_reconcile_key=last_rec_key, cycle_id=self.cycle_id)
                    self.pg_repo.commit()

            except Exception as e:
                error_count += 1
                entity_id = row.get(config["pk"]) if row else "unknown"
                logger.error("Failed to parse row (sent to DLQ)", table=table_name, error=str(e), pk=entity_id)
                
                try:
                    payload_str = json.dumps(row, default=str)
                    self.pg_repo.log_sync_failure(
                        source_table=table_name,
                        primary_key=str(entity_id),
                        error=str(e),
                        payload=payload_str
                    )
                    self.pg_repo.commit()
                except Exception as dlq_err:
                    logger.error("Failed to write mapping failure to DLQ", error=str(dlq_err))
                    self.pg_repo.rollback()
                
                data_batch.clear()
                fingerprint_batch.clear()
                continue

        # Final flush
        if data_batch:
            flush_batch()
            last_rec_key = checkpoint.last_reconcile_key if checkpoint else None
            self.pg_repo.update_checkpoint(table_name, last_sync_key=new_last_pk, last_reconcile_key=last_rec_key, cycle_id=self.cycle_id)
            self.pg_repo.commit()
            
        duration = time.time() - start_time
        
        # Expose Metrics
        SYNC_ROWS.labels(table=table_name, mode="incremental", status="success").inc(upserted_count)
        if error_count > 0:
            SYNC_ROWS.labels(table=table_name, mode="incremental", status="failed").inc(error_count)
            SYNC_ERRORS.labels(table=table_name, mode="incremental", error_type="row_failure").inc(error_count)
        SYNC_DURATION.labels(table=table_name, mode="incremental").observe(duration)
        
        self.update_lag_metric(table_name)
            
        return {"table": table_name, "mode": "incremental", "scanned": scanned_count, "upserted": upserted_count, "errors": error_count}

    def reconcile_table_chunk(self, table_name: str, chunk_limit: int = 5000) -> dict:
        """Slow path: Continuous background reconciliation using batch processing."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
            
        start_time = time.time()
        config = DataMapper.MAPPING[table_name]
        
        # Load checkpoint
        checkpoint = self.pg_repo.get_checkpoint(table_name)
        last_reconcile_pk = checkpoint.last_reconcile_key if checkpoint else None

        row_generator = self.mdb_repo.get_new_records(table_name, config["pk"], last_reconcile_pk)
        
        first_row_logged = False
        scanned_count = 0
        updated_count = 0
        error_count = 0
        new_last_reconcile_pk = last_reconcile_pk
        batch_size = 1000
        
        data_batch = []
        fingerprint_batch = []

        def flush_batch():
            nonlocal updated_count, error_count
            if not data_batch:
                return
            batch_start = time.time()
            try:
                self.pg_repo.upsert_batch(config["pg_model"], data_batch, config["pg_pk"])
                self.pg_repo.update_fingerprints_batch(table_name, fingerprint_batch)
                updated_count += len(data_batch)
                
                batch_dur_ms = (time.time() - batch_start) * 1000
                logger.info("Batch flush completed", 
                            cycle_id=self.cycle_id, 
                            table=table_name, 
                            batch="reconcile", 
                            rows=len(data_batch), 
                            duration_ms=round(batch_dur_ms, 2))
            except Exception as e:
                self.pg_repo.rollback()
                logger.warning("Reconciliation batch flush failed. Processing records individually...", table=table_name, error=str(e))
                
                for idx, pg_data in enumerate(data_batch):
                    entity_id = pg_data.get(config["pg_pk"])
                    try:
                        self.pg_repo.upsert(config["pg_model"], pg_data, config["pg_pk"])
                        self.pg_repo.update_fingerprints_batch(table_name, [fingerprint_batch[idx]])
                        self.pg_repo.commit()
                        updated_count += 1
                    except Exception as row_err:
                        self.pg_repo.rollback()
                        error_count += 1
                        logger.error("Individual reconciliation row failed (sent to DLQ)", table=table_name, pk=entity_id, error=str(row_err))
                        
                        try:
                            payload_str = json.dumps(pg_data, default=str)
                            self.pg_repo.log_sync_failure(
                                source_table=table_name,
                                primary_key=str(entity_id),
                                error=str(row_err),
                                payload=payload_str
                            )
                            self.pg_repo.commit()
                        except Exception as dlq_err:
                            logger.error("Failed to write reconcile failure to DLQ", error=str(dlq_err))
                            self.pg_repo.rollback()
            finally:
                data_batch.clear()
                fingerprint_batch.clear()

        for row in row_generator:
            if self.stop_event and self.stop_event.is_set():
                logger.warning("Stop event detected. Stopping reconciliation early...", table=table_name)
                break
            try:
                if not first_row_logged:
                    logger.debug("TABLE SAMPLE", table=table_name, columns=list(row.keys()), sample=str(row))
                    first_row_logged = True
                current_pk = str(row[config["pk"]])
                if last_reconcile_pk and not (current_pk > last_reconcile_pk):
                    continue
                        
                scanned_count += 1
                domain_model = DataMapper.map_to_domain(table_name, row)
                pg_data = DataMapper.map_to_pg(table_name, domain_model, settings.SOURCE_SYSTEM)
                
                data_batch.append(pg_data)
                fingerprint_batch.append((str(getattr(domain_model, config["pg_pk"])), domain_model.checksum))
                
                new_last_reconcile_pk = current_pk
                
                if scanned_count % 1000 == 0:
                    logger.info("Reconciliation in progress...", table=table_name, scanned=scanned_count, updated=updated_count)

                if len(data_batch) >= batch_size:
                    flush_batch()
                    last_s_key = checkpoint.last_sync_key if checkpoint else None
                    self.pg_repo.update_checkpoint(table_name, last_sync_key=last_s_key, last_reconcile_key=new_last_reconcile_pk, cycle_id=self.cycle_id)
                    self.pg_repo.commit()
                
                if scanned_count >= chunk_limit:
                    break

            except Exception as e:
                error_count += 1
                entity_id = row.get(config["pk"]) if row else "unknown"
                logger.error("Failed to process reconciliation row (sent to DLQ)", table=table_name, error=str(e), pk=entity_id)
                
                try:
                    payload_str = json.dumps(row, default=str)
                    self.pg_repo.log_sync_failure(
                        source_table=table_name,
                        primary_key=str(entity_id),
                        error=str(e),
                        payload=payload_str
                    )
                    self.pg_repo.commit()
                except Exception as dlq_err:
                    logger.error("Failed to write reconcile failure to DLQ", error=str(dlq_err))
                    self.pg_repo.rollback()
                    
                data_batch.clear()
                fingerprint_batch.clear()
                continue

        if data_batch:
            flush_batch()

        if scanned_count < chunk_limit:
            new_last_reconcile_pk = None

        if scanned_count > 0:
            last_s_key = checkpoint.last_sync_key if checkpoint else None
            self.pg_repo.update_checkpoint(table_name, last_sync_key=last_s_key, last_reconcile_key=new_last_reconcile_pk, cycle_id=self.cycle_id)
            self.pg_repo.commit()
            
        duration = time.time() - start_time
        
        # Expose Metrics
        SYNC_ROWS.labels(table=table_name, mode="reconcile", status="success").inc(updated_count)
        if error_count > 0:
            SYNC_ROWS.labels(table=table_name, mode="reconcile", status="failed").inc(error_count)
            SYNC_ERRORS.labels(table=table_name, mode="reconcile", error_type="row_failure").inc(error_count)
        RECONCILIATION_DURATION.labels(table=table_name).observe(duration)
            
        return {"table": table_name, "mode": "reconcile", "scanned": scanned_count, "updated": updated_count, "errors": error_count}

    def sync_table_full(self, table_name: str) -> dict:
        """For Master tables that must be fully scanned every time using batch processing."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
            
        start_time = time.time()
        config = DataMapper.MAPPING[table_name]
        row_generator = self.mdb_repo.get_full_scan(table_name, config["pk"])
        
        first_row_logged = False
        scanned_count = 0
        upserted_count = 0
        error_count = 0
        batch_size = 1000
        
        data_batch = []
        fingerprint_batch = []

        def flush_batch():
            nonlocal upserted_count, error_count
            if not data_batch:
                return
            batch_start = time.time()
            try:
                self.pg_repo.upsert_batch(config["pg_model"], data_batch, config["pg_pk"])
                self.pg_repo.update_fingerprints_batch(table_name, fingerprint_batch)
                upserted_count += len(data_batch)
                
                batch_dur_ms = (time.time() - batch_start) * 1000
                logger.info("Batch flush completed", 
                            cycle_id=self.cycle_id, 
                            table=table_name, 
                            batch="full_sync", 
                            rows=len(data_batch), 
                            duration_ms=round(batch_dur_ms, 2))
            except Exception as e:
                self.pg_repo.rollback()
                logger.warning("Master batch flush failed. Processing records individually...", table=table_name, error=str(e))
                
                for idx, pg_data in enumerate(data_batch):
                    entity_id = pg_data.get(config["pg_pk"])
                    try:
                        self.pg_repo.upsert(config["pg_model"], pg_data, config["pg_pk"])
                        self.pg_repo.update_fingerprints_batch(table_name, [fingerprint_batch[idx]])
                        self.pg_repo.commit()
                        upserted_count += 1
                    except Exception as row_err:
                        self.pg_repo.rollback()
                        error_count += 1
                        logger.error("Individual master row failed (sent to DLQ)", table=table_name, pk=entity_id, error=str(row_err))
                        
                        try:
                            payload_str = json.dumps(pg_data, default=str)
                            self.pg_repo.log_sync_failure(
                                source_table=table_name,
                                primary_key=str(entity_id),
                                error=str(row_err),
                                payload=payload_str
                            )
                            self.pg_repo.commit()
                        except Exception as dlq_err:
                            logger.error("Failed to write master failure to DLQ", error=str(dlq_err))
                            self.pg_repo.rollback()
            finally:
                data_batch.clear()
                fingerprint_batch.clear()

        for row in row_generator:
            if self.stop_event and self.stop_event.is_set():
                logger.warning("Stop event detected. Stopping full sync early...", table=table_name)
                break
            try:
                if not first_row_logged:
                    logger.debug("TABLE SAMPLE", table=table_name, columns=list(row.keys()), sample=str(row))
                    first_row_logged = True
                scanned_count += 1
                domain_model = DataMapper.map_to_domain(table_name, row)
                pg_data = DataMapper.map_to_pg(table_name, domain_model, settings.SOURCE_SYSTEM)
                
                data_batch.append(pg_data)
                fingerprint_batch.append((str(getattr(domain_model, config["pg_pk"])), domain_model.checksum))
                
                if scanned_count % 1000 == 0:
                    logger.info("Full scan in progress...", table=table_name, scanned=scanned_count, upserted=upserted_count)

                if len(data_batch) >= batch_size:
                    flush_batch()
                    self.pg_repo.commit()

            except Exception as e:
                error_count += 1
                entity_id = row.get(config["pk"]) if row else "unknown"
                logger.error("Failed to process master row (sent to DLQ)", table=table_name, error=str(e), pk=entity_id)
                
                try:
                    payload_str = json.dumps(row, default=str)
                    self.pg_repo.log_sync_failure(
                        source_table=table_name,
                        primary_key=str(entity_id),
                        error=str(e),
                        payload=payload_str
                    )
                    self.pg_repo.commit()
                except Exception as dlq_err:
                    logger.error("Failed to write master failure to DLQ", error=str(dlq_err))
                    self.pg_repo.rollback()
                    
                data_batch.clear()
                fingerprint_batch.clear()
                continue

        if data_batch:
            flush_batch()
            self.pg_repo.commit()

        # Update checkpoint for full scan
        self.pg_repo.update_checkpoint(table_name, last_sync_key=None, last_reconcile_key=None, cycle_id=self.cycle_id)
        self.pg_repo.commit()
            
        duration = time.time() - start_time
        
        # Expose Metrics
        SYNC_ROWS.labels(table=table_name, mode="full", status="success").inc(upserted_count)
        if error_count > 0:
            SYNC_ROWS.labels(table=table_name, mode="full", status="failed").inc(error_count)
            SYNC_ERRORS.labels(table=table_name, mode="full", error_type="row_failure").inc(error_count)
        SYNC_DURATION.labels(table=table_name, mode="full").observe(duration)
        
        return {"table": table_name, "mode": "full", "scanned": scanned_count, "upserted": upserted_count, "errors": error_count}
