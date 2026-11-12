import time
import json
from datetime import timezone, datetime
import threading
from typing import Optional, List, Dict, Any
from sqlalchemy import text, func

from src.mdb_sync.config import settings
from src.mdb_sync.infrastructure.mdb.repository import MDBRepository
from src.mdb_sync.infrastructure.postgres.repository import PostgresRepository
from src.mdb_sync.infrastructure.postgres.database import SessionLocal
from src.mdb_sync.infrastructure.postgres.models import (
    RawSale,
    RawBillDetail,
    RawLRMaster,
    RawLRDetail,
    RawDeleteRecord,
)
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
        config = DataMapper.MAPPING.get(table_name)
        if not config:
            return
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

    def _get_entity_id(self, data: Dict[str, Any], key_field: Any) -> str:
        if isinstance(key_field, (list, tuple)):
            return ":".join(str(data.get(k, "")) for k in key_field)
        return str(data.get(key_field, ""))

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
        
        pk_field = config["pk"][0] if isinstance(config["pk"], (list, tuple)) else config["pk"]
        pk_type = config.get("pk_type", str)
        row_generator = self.mdb_repo.get_new_records(table_name, config["pk"], last_pk, pk_type)
        
        first_row_logged = False
        scanned_count = 0
        upserted_count = 0
        error_count = 0
        new_last_pk = last_pk
        batch_size = 1000
        
        data_batch = []
        extra_counters: Dict[Any, int] = {}

        def flush_batch():
            nonlocal upserted_count, error_count
            if not data_batch:
                return
            batch_start = time.time()
            try:
                # Propose batch insert
                affected = self.pg_repo.upsert_batch(config["pg_model"], data_batch, config["pg_pk"])
                upserted_count += (affected if affected is not None else len(data_batch))
                
                # If syncing BILL_MASTER, trigger line details sync for newly upserted bills
                if table_name == "BILL_MASTER":
                    bill_ids = [str(item["bill_id"]) for item in data_batch if item.get("bill_id")]
                    if bill_ids:
                        try:
                            self.sync_bill_details_for_bills(bill_ids)
                        except Exception as det_err:
                            logger.error("Failed to sync bill details for new bills", error=str(det_err), bill_count=len(bill_ids))

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
                    entity_id = self._get_entity_id(pg_data, config["pg_pk"])
                    try:
                        affected = self.pg_repo.upsert(config["pg_model"], pg_data, config["pg_pk"])
                        self.pg_repo.commit()
                        if affected or affected is None:
                            upserted_count += 1
                        if table_name == "BILL_MASTER" and pg_data.get("bill_id"):
                            try:
                                self.sync_bill_details_for_bills([str(pg_data["bill_id"])])
                            except Exception as det_err:
                                logger.error("Failed to sync bill details for individual bill", error=str(det_err), bill_id=pg_data["bill_id"])
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

        for row in row_generator:
            if self.stop_event and self.stop_event.is_set():
                logger.warning("Stop event detected. Stopping incremental sync early...", table=table_name)
                break
            try:
                if not first_row_logged:
                    logger.debug("TABLE SAMPLE", table=table_name, columns=list(row.keys()), sample=str(row))
                    first_row_logged = True

                current_pk = row[pk_field]
                if last_pk is not None and str(last_pk).strip() != "":
                    if not DataMapper.is_greater_pk(current_pk, last_pk, pk_type):
                        continue
                    
                scanned_count += 1

                # Extra sequential indexes for composite entities
                extra_args = {}
                if table_name == "LR_MASTER":
                    lr_id = str(row.get("LR_ID", ""))
                    idx = extra_counters.get(lr_id, 0) + 1
                    extra_counters[lr_id] = idx
                    extra_args["entry_idx"] = idx
                elif table_name == "LR_Detail":
                    key = (str(row.get("LR_Id", "")), str(row.get("Bill_ID", "")))
                    idx = extra_counters.get(key, 0) + 1
                    extra_counters[key] = idx
                    extra_args["line_idx"] = idx
                elif table_name == "Bill_Detail":
                    b_id = str(row.get("Bill_ID", ""))
                    idx = extra_counters.get(b_id, 0) + 1
                    extra_counters[b_id] = idx
                    extra_args["line_idx"] = idx

                domain_model = DataMapper.map_to_domain(table_name, row, **extra_args)
                pg_data = DataMapper.map_to_pg(table_name, domain_model, settings.SOURCE_SYSTEM)
                
                data_batch.append(pg_data)
                new_last_pk = str(current_pk)
                
                if scanned_count % 1000 == 0:
                    logger.info("Syncing in progress...", table=table_name, scanned=scanned_count, upserted=upserted_count)

                if len(data_batch) >= batch_size:
                    flush_batch()
                    self.pg_repo.update_checkpoint(table_name, last_sync_key=new_last_pk, cycle_id=self.cycle_id)
                    self.pg_repo.commit()

            except Exception as e:
                error_count += 1
                entity_id = self._get_entity_id(row, config["pk"]) if row else "unknown"
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
                continue

        # Final flush
        if data_batch:
            flush_batch()
            self.pg_repo.update_checkpoint(table_name, last_sync_key=new_last_pk, cycle_id=self.cycle_id)
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

        pk_field = config["pk"][0] if isinstance(config["pk"], (list, tuple)) else config["pk"]
        pk_type = config.get("pk_type", str)
        row_generator = self.mdb_repo.get_new_records(table_name, config["pk"], last_reconcile_pk, pk_type)
        
        first_row_logged = False
        scanned_count = 0
        updated_count = 0
        error_count = 0
        new_last_reconcile_pk = last_reconcile_pk
        batch_size = 1000
        stopped_early = False
        reached_chunk_limit = False
        row_exhausted = False
        
        data_batch = []
        extra_counters: Dict[Any, int] = {}

        def flush_batch():
            nonlocal updated_count, error_count
            if not data_batch:
                return
            batch_start = time.time()
            changed_bill_ids = []
            if table_name == "BILL_MASTER":
                batch_bill_ids = [str(item["bill_id"]) for item in data_batch if item.get("bill_id")]
                if batch_bill_ids:
                    try:
                        existing = {
                            r[0]: r[1] for r in self.pg_repo.session.query(RawSale.bill_id, RawSale.checksum)
                            .filter(RawSale.bill_id.in_(batch_bill_ids))
                            .all()
                        }
                        changed_bill_ids = [
                            item["bill_id"] for item in data_batch
                            if existing.get(item["bill_id"]) != item.get("checksum")
                        ]
                    except Exception as chk_err:
                        logger.warning("Failed to check existing checksums during reconciliation", error=str(chk_err))

            try:
                affected = self.pg_repo.upsert_batch(config["pg_model"], data_batch, config["pg_pk"])
                updated_count += (affected if affected is not None else len(data_batch))
                
                # Sync lines for any bills whose checksum changed
                if changed_bill_ids:
                    try:
                        self.sync_bill_details_for_bills(changed_bill_ids)
                    except Exception as det_err:
                        logger.error("Failed to sync bill details for reconciled changed bills", error=str(det_err), bill_count=len(changed_bill_ids))

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
                    entity_id = self._get_entity_id(pg_data, config["pg_pk"])
                    try:
                        affected = self.pg_repo.upsert(config["pg_model"], pg_data, config["pg_pk"])
                        self.pg_repo.commit()
                        if affected or affected is None:
                            updated_count += 1
                        if table_name == "BILL_MASTER" and pg_data.get("bill_id") in changed_bill_ids:
                            try:
                                self.sync_bill_details_for_bills([str(pg_data["bill_id"])])
                            except Exception as det_err:
                                logger.error("Failed to sync bill details for reconciled individual bill", error=str(det_err), bill_id=pg_data["bill_id"])
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

        for row in row_generator:
            if self.stop_event and self.stop_event.is_set():
                logger.warning("Stop event detected. Stopping reconciliation early...", table=table_name)
                stopped_early = True
                break
            try:
                if not first_row_logged:
                    logger.debug("TABLE SAMPLE", table=table_name, columns=list(row.keys()), sample=str(row))
                    first_row_logged = True
                current_pk = row[pk_field]
                if last_reconcile_pk is not None and str(last_reconcile_pk).strip() != "":
                    if not DataMapper.is_greater_pk(current_pk, last_reconcile_pk, pk_type):
                        continue
                        
                scanned_count += 1

                extra_args = {}
                if table_name == "LR_MASTER":
                    lr_id = str(row.get("LR_ID", ""))
                    idx = extra_counters.get(lr_id, 0) + 1
                    extra_counters[lr_id] = idx
                    extra_args["entry_idx"] = idx
                elif table_name == "LR_Detail":
                    key = (str(row.get("LR_Id", "")), str(row.get("Bill_ID", "")))
                    idx = extra_counters.get(key, 0) + 1
                    extra_counters[key] = idx
                    extra_args["line_idx"] = idx
                elif table_name == "Bill_Detail":
                    b_id = str(row.get("Bill_ID", ""))
                    idx = extra_counters.get(b_id, 0) + 1
                    extra_counters[b_id] = idx
                    extra_args["line_idx"] = idx

                domain_model = DataMapper.map_to_domain(table_name, row, **extra_args)
                pg_data = DataMapper.map_to_pg(table_name, domain_model, settings.SOURCE_SYSTEM)
                
                data_batch.append(pg_data)
                new_last_reconcile_pk = str(current_pk)
                
                if scanned_count % 1000 == 0:
                    logger.info("Reconciliation in progress...", table=table_name, scanned=scanned_count, updated=updated_count)

                if len(data_batch) >= batch_size:
                    flush_batch()
                    self.pg_repo.update_checkpoint(table_name, last_reconcile_key=new_last_reconcile_pk, cycle_id=self.cycle_id)
                    self.pg_repo.commit()
                
                if scanned_count >= chunk_limit:
                    reached_chunk_limit = True
                    break

            except Exception as e:
                error_count += 1
                entity_id = self._get_entity_id(row, config["pk"]) if row else "unknown"
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
                continue
        else:
            row_exhausted = True

        if data_batch:
            flush_batch()

        # If naturally exhausted, wrap around for next reconciliation cycle
        if row_exhausted and not stopped_early and not reached_chunk_limit:
            new_last_reconcile_pk = None

        if scanned_count > 0 or row_exhausted:
            self.pg_repo.update_checkpoint(table_name, last_reconcile_key=new_last_reconcile_pk, cycle_id=self.cycle_id)
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
        pk_field = config["pk"][0] if isinstance(config["pk"], (list, tuple)) else config["pk"]
        row_generator = self.mdb_repo.get_full_scan(table_name, pk_field)
        
        first_row_logged = False
        scanned_count = 0
        upserted_count = 0
        error_count = 0
        batch_size = 1000
        
        data_batch = []
        extra_counters: Dict[Any, int] = {}

        def flush_batch():
            nonlocal upserted_count, error_count
            if not data_batch:
                return
            batch_start = time.time()
            try:
                affected = self.pg_repo.upsert_batch(config["pg_model"], data_batch, config["pg_pk"])
                upserted_count += (affected if affected is not None else len(data_batch))
                
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
                    entity_id = self._get_entity_id(pg_data, config["pg_pk"])
                    try:
                        affected = self.pg_repo.upsert(config["pg_model"], pg_data, config["pg_pk"])
                        self.pg_repo.commit()
                        if affected or affected is None:
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

        for row in row_generator:
            if self.stop_event and self.stop_event.is_set():
                logger.warning("Stop event detected. Stopping full sync early...", table=table_name)
                break
            try:
                if not first_row_logged:
                    logger.debug("TABLE SAMPLE", table=table_name, columns=list(row.keys()), sample=str(row))
                    first_row_logged = True
                scanned_count += 1

                extra_args = {}
                if table_name == "LR_MASTER":
                    lr_id = str(row.get("LR_ID", ""))
                    idx = extra_counters.get(lr_id, 0) + 1
                    extra_counters[lr_id] = idx
                    extra_args["entry_idx"] = idx
                elif table_name == "LR_Detail":
                    key = (str(row.get("LR_Id", "")), str(row.get("Bill_ID", "")))
                    idx = extra_counters.get(key, 0) + 1
                    extra_counters[key] = idx
                    extra_args["line_idx"] = idx
                elif table_name == "Bill_Detail":
                    b_id = str(row.get("Bill_ID", ""))
                    idx = extra_counters.get(b_id, 0) + 1
                    extra_counters[b_id] = idx
                    extra_args["line_idx"] = idx

                domain_model = DataMapper.map_to_domain(table_name, row, **extra_args)
                pg_data = DataMapper.map_to_pg(table_name, domain_model, settings.SOURCE_SYSTEM)
                
                data_batch.append(pg_data)
                
                if scanned_count % 1000 == 0:
                    logger.info("Full scan in progress...", table=table_name, scanned=scanned_count, upserted=upserted_count)

                if len(data_batch) >= batch_size:
                    flush_batch()
                    self.pg_repo.commit()

            except Exception as e:
                error_count += 1
                entity_id = self._get_entity_id(row, config["pk"]) if row else "unknown"
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
                continue

        if data_batch:
            flush_batch()
            self.pg_repo.commit()

        # Update checkpoint for full scan without clobbering keys
        self.pg_repo.update_checkpoint(table_name, cycle_id=self.cycle_id)
        self.pg_repo.commit()
            
        duration = time.time() - start_time
        
        # Expose Metrics
        SYNC_ROWS.labels(table=table_name, mode="full", status="success").inc(upserted_count)
        if error_count > 0:
            SYNC_ROWS.labels(table=table_name, mode="full", status="failed").inc(error_count)
            SYNC_ERRORS.labels(table=table_name, mode="full", error_type="row_failure").inc(error_count)
        SYNC_DURATION.labels(table=table_name, mode="full").observe(duration)
        
        return {"table": table_name, "mode": "full", "scanned": scanned_count, "upserted": upserted_count, "errors": error_count}

    def sync_bill_details_for_bills(self, bill_ids: List[str]) -> dict:
        """Synchronizes line items from Bill_Detail for a specific list of Bill_IDs."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
        if not bill_ids:
            return {"scanned": 0, "upserted": 0, "errors": 0}

        valid_int_ids = []
        for b in bill_ids:
            try:
                valid_int_ids.append(int(float(str(b).strip())))
            except (ValueError, TypeError):
                pass

        rows_by_bill: Dict[str, list] = {str(b): [] for b in bill_ids}

        if valid_int_ids:
            valid_int_ids = sorted(list(set(valid_int_ids)))
            min_id = valid_int_ids[0]
            max_id = valid_int_ids[-1]
            span = max_id - min_id

            # If range is compact, do a single range query
            if span <= 200 or len(valid_int_ids) >= span * 0.5:
                q = f"SELECT * FROM Bill_Detail WHERE Bill_ID >= {min_id} AND Bill_ID <= {max_id}"
                for row in self.mdb_repo.execute_query_yield(q):
                    b_str = str(row.get("Bill_ID", ""))
                    if b_str in rows_by_bill:
                        rows_by_bill[b_str].append(row)
            else:
                for b_id in valid_int_ids:
                    q = f"SELECT * FROM Bill_Detail WHERE Bill_ID = {b_id}"
                    for row in self.mdb_repo.execute_query_yield(q):
                        b_str = str(row.get("Bill_ID", ""))
                        if b_str in rows_by_bill:
                            rows_by_bill[b_str].append(row)

        data_batch = []
        scanned_count = 0
        upserted_count = 0
        error_count = 0

        for b_str, mdb_lines in rows_by_bill.items():
            max_line = len(mdb_lines)
            for idx, row in enumerate(mdb_lines, start=1):
                scanned_count += 1
                try:
                    domain_model = DataMapper.map_to_domain("Bill_Detail", row, line_idx=idx)
                    pg_data = DataMapper.map_to_pg("Bill_Detail", domain_model, settings.SOURCE_SYSTEM)
                    data_batch.append(pg_data)
                except Exception as e:
                    error_count += 1
                    logger.error("Failed to map bill line", bill_id=b_str, line_idx=idx, error=str(e))

            # Mark any obsolete lines as deleted
            try:
                self.pg_repo.session.query(RawBillDetail).filter(
                    RawBillDetail.bill_id == b_str,
                    RawBillDetail.line_idx > max_line
                ).update({"is_deleted": True}, synchronize_session=False)
            except Exception as e:
                logger.warning("Failed to clean up excess bill lines", bill_id=b_str, error=str(e))

        if data_batch:
            affected = self.pg_repo.upsert_batch(RawBillDetail, data_batch, ["bill_id", "line_idx"])
            self.pg_repo.commit()
            upserted_count = affected if affected is not None else len(data_batch)

        return {"scanned": scanned_count, "upserted": upserted_count, "errors": error_count}

    def sync_deletions(self) -> dict:
        """Syncs Delete_Record from MDB and marks records deleted across raw tables in Postgres."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")

        start_time = time.time()
        logger.info("Starting sync_deletions...", cycle_id=self.cycle_id)

        row_generator = self.mdb_repo.get_full_scan("Delete_Record", "Bill_ID")
        data_batch = []
        scanned_count = 0
        upserted_count = 0
        error_count = 0

        for row in row_generator:
            scanned_count += 1
            try:
                domain_model = DataMapper.map_to_domain("Delete_Record", row)
                pg_data = DataMapper.map_to_pg("Delete_Record", domain_model, settings.SOURCE_SYSTEM)
                data_batch.append(pg_data)
            except Exception as e:
                error_count += 1
                logger.error("Failed to map Delete_Record row", error=str(e), row=row)

            if len(data_batch) >= 1000:
                affected = self.pg_repo.upsert_batch(RawDeleteRecord, data_batch, "bill_id")
                upserted_count += (affected if affected is not None else len(data_batch))
                self.pg_repo.commit()
                data_batch.clear()

        if data_batch:
            affected = self.pg_repo.upsert_batch(RawDeleteRecord, data_batch, "bill_id")
            upserted_count += (affected if affected is not None else len(data_batch))
            self.pg_repo.commit()
            data_batch.clear()

        # Mark deleted records across all raw tables in Postgres
        try:
            res_sales = self.pg_repo.session.execute(text("""
                UPDATE raw_sales
                SET is_deleted = TRUE, is_ok = 0
                FROM raw_delete_records
                WHERE raw_sales.bill_id = raw_delete_records.bill_id
                  AND (raw_sales.is_deleted = FALSE OR raw_sales.is_ok != 0)
            """))
            res_details = self.pg_repo.session.execute(text("""
                UPDATE raw_bill_details
                SET is_deleted = TRUE
                FROM raw_delete_records
                WHERE raw_bill_details.bill_id = raw_delete_records.bill_id
                  AND raw_bill_details.is_deleted = FALSE
            """))
            res_lr_details = self.pg_repo.session.execute(text("""
                UPDATE raw_lr_details
                SET is_deleted = TRUE
                FROM raw_delete_records
                WHERE raw_lr_details.bill_id = raw_delete_records.bill_id
                  AND raw_lr_details.is_deleted = FALSE
            """))
            res_lr_master = self.pg_repo.session.execute(text("""
                UPDATE raw_lr_master
                SET is_deleted = TRUE
                FROM raw_delete_records
                WHERE raw_lr_master.bill_id = raw_delete_records.bill_id
                  AND raw_lr_master.is_deleted = FALSE
            """))
            self.pg_repo.session.commit()
            logger.info("Marked deleted records in Postgres",
                        sales_affected=res_sales.rowcount,
                        details_affected=res_details.rowcount,
                        lr_details_affected=res_lr_details.rowcount,
                        lr_master_affected=res_lr_master.rowcount)
        except Exception as e:
            self.pg_repo.session.rollback()
            logger.error("Failed to mark deleted records in Postgres", error=str(e))

        self.pg_repo.update_checkpoint("Delete_Record", cycle_id=self.cycle_id)
        self.pg_repo.commit()

        duration = time.time() - start_time
        logger.info("Completed sync_deletions", duration_sec=round(duration, 2), scanned=scanned_count, upserted=upserted_count)
        return {"table": "Delete_Record", "scanned": scanned_count, "upserted": upserted_count, "errors": error_count}

    def sync_lr_master_full(self, batch_size: int = 5000) -> dict:
        """Full backfill / sync for LR_MASTER."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
        start_time = time.time()
        logger.info("Starting sync_lr_master_full...", cycle_id=self.cycle_id)

        entry_counters: Dict[str, int] = {}
        row_generator = self.mdb_repo.get_full_scan("LR_MASTER", "LR_ID")
        data_batch = []
        scanned = 0
        upserted = 0
        errors = 0

        for row in row_generator:
            scanned += 1
            lr_id = str(row.get("LR_ID", ""))
            entry_idx = entry_counters.get(lr_id, 0) + 1
            entry_counters[lr_id] = entry_idx
            try:
                domain_model = DataMapper.map_to_domain("LR_MASTER", row, entry_idx=entry_idx)
                pg_data = DataMapper.map_to_pg("LR_MASTER", domain_model, settings.SOURCE_SYSTEM)
                data_batch.append(pg_data)
            except Exception as e:
                errors += 1
                logger.error("Failed to map LR_MASTER row", error=str(e), lr_id=lr_id)

            if len(data_batch) >= batch_size:
                affected = self.pg_repo.upsert_batch(RawLRMaster, data_batch, ["lr_id", "entry_idx"])
                upserted += (affected if affected is not None else len(data_batch))
                self.pg_repo.commit()
                data_batch.clear()

        if data_batch:
            affected = self.pg_repo.upsert_batch(RawLRMaster, data_batch, ["lr_id", "entry_idx"])
            upserted += (affected if affected is not None else len(data_batch))
            self.pg_repo.commit()
            data_batch.clear()

        self.pg_repo.update_checkpoint("LR_MASTER", cycle_id=self.cycle_id)
        self.pg_repo.commit()
        logger.info("Completed sync_lr_master_full", scanned=scanned, upserted=upserted, errors=errors)
        return {"table": "LR_MASTER", "scanned": scanned, "upserted": upserted, "errors": errors}

    def sync_lr_details_full(self, batch_size: int = 5000) -> dict:
        """Full backfill / sync for LR_Detail."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
        start_time = time.time()
        logger.info("Starting sync_lr_details_full...", cycle_id=self.cycle_id)

        line_counters: Dict[tuple, int] = {}
        row_generator = self.mdb_repo.get_full_scan("LR_Detail", "LR_Id")
        data_batch = []
        scanned = 0
        upserted = 0
        errors = 0

        for row in row_generator:
            scanned += 1
            lr_id = str(row.get("LR_Id", ""))
            bill_id = str(row.get("Bill_ID", ""))
            key = (lr_id, bill_id)
            line_idx = line_counters.get(key, 0) + 1
            line_counters[key] = line_idx
            try:
                domain_model = DataMapper.map_to_domain("LR_Detail", row, line_idx=line_idx)
                pg_data = DataMapper.map_to_pg("LR_Detail", domain_model, settings.SOURCE_SYSTEM)
                data_batch.append(pg_data)
            except Exception as e:
                errors += 1
                logger.error("Failed to map LR_Detail row", error=str(e), lr_id=lr_id, bill_id=bill_id)

            if len(data_batch) >= batch_size:
                affected = self.pg_repo.upsert_batch(RawLRDetail, data_batch, ["lr_id", "bill_id", "line_idx"])
                upserted += (affected if affected is not None else len(data_batch))
                self.pg_repo.commit()
                data_batch.clear()

        if data_batch:
            affected = self.pg_repo.upsert_batch(RawLRDetail, data_batch, ["lr_id", "bill_id", "line_idx"])
            upserted += (affected if affected is not None else len(data_batch))
            self.pg_repo.commit()
            data_batch.clear()

        self.pg_repo.update_checkpoint("LR_Detail", cycle_id=self.cycle_id)
        self.pg_repo.commit()
        logger.info("Completed sync_lr_details_full", scanned=scanned, upserted=upserted, errors=errors)
        return {"table": "LR_Detail", "scanned": scanned, "upserted": upserted, "errors": errors}

    def backfill_sales_headers(self, batch_size: int = 5000) -> dict:
        """Backfills missing bill header fields on raw_sales from BILL_MASTER while preserving processing state."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
        start_time = time.time()
        logger.info("Starting backfill_sales_headers...", cycle_id=self.cycle_id)

        # Get existing processing states to prevent re-triggering downstream pipelines
        processed_state = {}
        res = self.pg_repo.session.execute(text("SELECT bill_id, is_processed, processed_at FROM raw_sales"))
        for b_id, is_p, p_at in res:
            processed_state[str(b_id)] = (is_p, p_at)

        row_generator = self.mdb_repo.get_full_scan("BILL_MASTER", "Bill_ID")
        data_batch = []
        scanned = 0
        upserted = 0
        errors = 0

        for row in row_generator:
            scanned += 1
            try:
                domain_model = DataMapper.map_to_domain("BILL_MASTER", row)
                pg_data = DataMapper.map_to_pg("BILL_MASTER", domain_model, settings.SOURCE_SYSTEM)
                b_id = str(pg_data.get("bill_id", ""))
                # Retain existing processing state if previously processed
                if b_id in processed_state:
                    prev_is_p, prev_p_at = processed_state[b_id]
                    if prev_is_p:
                        pg_data["is_processed"] = prev_is_p
                        pg_data["processed_at"] = prev_p_at

                data_batch.append(pg_data)
            except Exception as e:
                errors += 1
                logger.error("Failed to map BILL_MASTER row for header backfill", error=str(e), pk=row.get("Bill_ID"))

            if len(data_batch) >= batch_size:
                affected = self.pg_repo.upsert_batch(RawSale, data_batch, "bill_id")
                upserted += (affected if affected is not None else len(data_batch))
                self.pg_repo.commit()
                data_batch.clear()
                if scanned % 20000 == 0:
                    logger.info("Sales headers backfill in progress...", scanned=scanned, upserted=upserted)

        if data_batch:
            affected = self.pg_repo.upsert_batch(RawSale, data_batch, "bill_id")
            upserted += (affected if affected is not None else len(data_batch))
            self.pg_repo.commit()
            data_batch.clear()

        logger.info("Completed backfill_sales_headers", scanned=scanned, upserted=upserted, errors=errors)
        return {"table": "BILL_MASTER_headers", "scanned": scanned, "upserted": upserted, "errors": errors}

    def sync_bill_details_all(self, batch_size: int = 10000) -> dict:
        """High-throughput, resumable backfill of all Bill_Detail records into PostgreSQL."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
        start_time = time.time()
        logger.info("Starting sync_bill_details_all...", cycle_id=self.cycle_id)

        checkpoint = self.pg_repo.get_checkpoint("Bill_Detail")
        last_key = checkpoint.last_sync_key if checkpoint else None
        
        if last_key == "DONE":
            logger.info("Bill_Detail sync already marked DONE. Skipping full backfill.")
            return {"table": "Bill_Detail", "status": "ALREADY_DONE", "scanned": 0, "upserted": 0}

        resume_offset = 0
        if last_key and str(last_key).isdigit():
            resume_offset = int(last_key)
            logger.info("Resuming Bill_Detail sync from offset", resume_offset=resume_offset)

        # Pre-seed counters if resuming, otherwise start fresh
        bill_line_counters: Dict[str, int] = {}
        if resume_offset > 0:
            logger.info("Pre-seeding bill line counters from PostgreSQL...")
            res = self.pg_repo.session.execute(text("SELECT bill_id, MAX(line_idx) FROM raw_bill_details GROUP BY bill_id"))
            for b_id, max_l in res:
                bill_line_counters[str(b_id)] = int(max_l)
            logger.info("Pre-seeded line counters for bills", count=len(bill_line_counters))

        row_generator = self.mdb_repo.get_full_scan("Bill_Detail", "Bill_ID")
        data_batch = []
        scanned = 0
        upserted = 0
        errors = 0

        for row in row_generator:
            scanned += 1
            if scanned <= resume_offset:
                continue

            b_id = str(row.get("Bill_ID", ""))
            line_idx = bill_line_counters.get(b_id, 0) + 1
            bill_line_counters[b_id] = line_idx

            try:
                domain_model = DataMapper.map_to_domain("Bill_Detail", row, line_idx=line_idx)
                pg_data = DataMapper.map_to_pg("Bill_Detail", domain_model, settings.SOURCE_SYSTEM)
                data_batch.append(pg_data)
            except Exception as e:
                errors += 1
                logger.error("Failed to map Bill_Detail row", error=str(e), bill_id=b_id, line_idx=line_idx)

            if len(data_batch) >= batch_size:
                affected = self.pg_repo.upsert_batch(RawBillDetail, data_batch, ["bill_id", "line_idx"])
                upserted += (affected if affected is not None else len(data_batch))
                self.pg_repo.update_checkpoint("Bill_Detail", last_sync_key=str(scanned), cycle_id=self.cycle_id)
                self.pg_repo.commit()
                data_batch.clear()

                if scanned % 50000 == 0:
                    elapsed = time.time() - start_time
                    rps = scanned / elapsed if elapsed > 0 else 0
                    logger.info("Bill_Detail sync in progress...", scanned=scanned, upserted=upserted, rows_per_sec=round(rps, 1))

        if data_batch:
            affected = self.pg_repo.upsert_batch(RawBillDetail, data_batch, ["bill_id", "line_idx"])
            upserted += (affected if affected is not None else len(data_batch))
            data_batch.clear()

        self.pg_repo.update_checkpoint("Bill_Detail", last_sync_key="DONE", cycle_id=self.cycle_id)
        self.pg_repo.commit()
        logger.info("Completed sync_bill_details_all", scanned=scanned, upserted=upserted, errors=errors)
        return {"table": "Bill_Detail", "scanned": scanned, "upserted": upserted, "errors": errors}
