from typing import Optional
from src.mdb_sync.config import settings
from src.mdb_sync.infrastructure.mdb.repository import MDBRepository
from src.mdb_sync.infrastructure.postgres.repository import PostgresRepository
from src.mdb_sync.application.mapper import DataMapper
from src.mdb_sync.logging_config import get_logger

logger = get_logger(__name__)

class SyncEngine:
    def __init__(self, mdb_repo: MDBRepository):
        self.mdb_repo = mdb_repo
        self.pg_repo: Optional[PostgresRepository] = None

    def set_pg_repo(self, pg_repo: PostgresRepository):
        self.pg_repo = pg_repo

    def sync_table_incremental(self, table_name: str) -> dict:
        """Fast path: Appends new records using batch processing."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
        config = DataMapper.MAPPING[table_name]
        state = self.pg_repo.get_sync_state(table_name)
        last_pk = state.last_pk if state else None

        logger.debug("Starting incremental sync", table=table_name, last_pk=last_pk)
        
        row_generator = self.mdb_repo.get_new_records(table_name, config["pk"], last_pk)
        
        # DIAGNOSTIC: Log column names for the first row
        first_row_logged = False

        scanned_count = 0
        upserted_count = 0
        error_count = 0
        new_last_pk = last_pk
        batch_size = 1000
        
        data_batch = []
        fingerprint_batch = []

        def flush_batch():
            nonlocal upserted_count
            if not data_batch:
                return
            try:
                self.pg_repo.upsert_batch(config["pg_model"], data_batch, config["pg_pk"])
                self.pg_repo.update_fingerprints_batch(table_name, fingerprint_batch)
                upserted_count += len(data_batch)
            except Exception as e:
                self.pg_repo.rollback()
                logger.error("Batch flush failed", table=table_name, error=str(e))
                raise
            finally:
                data_batch.clear()
                fingerprint_batch.clear()

        for row in row_generator:
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
                    rec_pk = state.last_reconcile_pk if state else None
                    self.pg_repo.update_sync_state(table_name, last_pk=new_last_pk, last_reconcile_pk=rec_pk)
                    self.pg_repo.commit()

            except Exception as e:
                error_count += 1
                logger.error("Failed to process row or batch", table=table_name, error=str(e), pk=row.get(config["pk"]))
                # If flush_batch raised, it already rolled back and cleared.
                # If mapping failed, we don't strictly need rollback but it's safe.
                self.pg_repo.rollback()
                data_batch.clear()
                fingerprint_batch.clear()
                continue

        # Final flush
        if data_batch:
            try:
                flush_batch()
                rec_pk = state.last_reconcile_pk if state else None
                self.pg_repo.update_sync_state(table_name, last_pk=new_last_pk, last_reconcile_pk=rec_pk)
                self.pg_repo.commit()
            except Exception as e:
                error_count += len(data_batch)
                logger.error("Final batch flush failed", table=table_name, error=str(e))
                self.pg_repo.rollback()
            
        return {"table": table_name, "mode": "incremental", "scanned": scanned_count, "upserted": upserted_count, "errors": error_count}

    def reconcile_table_chunk(self, table_name: str, chunk_limit: int = 5000) -> dict:
        """Slow path: Continuous background reconciliation using batch processing."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
        config = DataMapper.MAPPING[table_name]
        state = self.pg_repo.get_sync_state(table_name)
        last_reconcile_pk = state.last_reconcile_pk if state else None

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
            nonlocal updated_count
            if not data_batch:
                return
            try:
                self.pg_repo.upsert_batch(config["pg_model"], data_batch, config["pg_pk"])
                self.pg_repo.update_fingerprints_batch(table_name, fingerprint_batch)
                updated_count += len(data_batch)
            except Exception as e:
                self.pg_repo.rollback()
                logger.error("Reconciliation batch flush failed", table=table_name, error=str(e))
                raise
            finally:
                data_batch.clear()
                fingerprint_batch.clear()

        for row in row_generator:
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
                    self.pg_repo.update_sync_state(table_name, last_pk=state.last_pk if state else None, last_reconcile_pk=new_last_reconcile_pk)
                    self.pg_repo.commit()
                
                if scanned_count >= chunk_limit:
                    break

            except Exception as e:
                error_count += 1
                logger.debug("Failed to process reconciliation row or batch", table=table_name, error=str(e), pk=row.get(config["pk"]))
                self.pg_repo.rollback()
                data_batch.clear()
                fingerprint_batch.clear()
                continue

        if data_batch:
            try:
                flush_batch()
            except Exception as e:
                error_count += len(data_batch)
                logger.error("Reconciliation final batch flush failed", table=table_name, error=str(e))
                self.pg_repo.rollback()

        if scanned_count < chunk_limit:
            new_last_reconcile_pk = None

        if scanned_count > 0:
            self.pg_repo.update_sync_state(table_name, last_pk=state.last_pk if state else None, last_reconcile_pk=new_last_reconcile_pk)
            self.pg_repo.commit()
            
        return {"table": table_name, "mode": "reconcile", "scanned": scanned_count, "updated": updated_count, "errors": error_count}

    def sync_table_full(self, table_name: str) -> dict:
        """For Master tables that must be fully scanned every time using batch processing."""
        if not self.pg_repo:
            raise RuntimeError("PG Repo not set")
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
            nonlocal upserted_count
            if not data_batch:
                return
            try:
                self.pg_repo.upsert_batch(config["pg_model"], data_batch, config["pg_pk"])
                self.pg_repo.update_fingerprints_batch(table_name, fingerprint_batch)
                upserted_count += len(data_batch)
            except Exception as e:
                self.pg_repo.rollback()
                logger.error("Master batch flush failed", table=table_name, error=str(e))
                raise
            finally:
                data_batch.clear()
                fingerprint_batch.clear()

        for row in row_generator:
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
                logger.debug("Failed to process master row or batch", table=table_name, error=str(e), pk=row.get(config["pk"]))
                self.pg_repo.rollback()
                data_batch.clear()
                fingerprint_batch.clear()
                continue

        if data_batch:
            try:
                flush_batch()
                self.pg_repo.commit()
            except Exception as e:
                error_count += len(data_batch)
                logger.error("Master final batch flush failed", table=table_name, error=str(e))
                self.pg_repo.rollback()

        return {"table": table_name, "mode": "full", "scanned": scanned_count, "upserted": upserted_count, "errors": error_count}
