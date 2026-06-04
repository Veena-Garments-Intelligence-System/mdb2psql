import pyodbc
import time
from typing import Dict, Any, Optional, Iterator
from src.mdb_sync.config import settings
from src.mdb_sync.logging_config import get_logger
from src.mdb_sync.utils.circuit_breaker import mdb_breaker
from src.mdb_sync.utils.metrics import MDB_LATENCY

logger = get_logger(__name__)

class MDBRepository:
    def __init__(self):
        self.conn_str = settings.mdb_connection_string
        self.chunk_size = 1000

    def execute_query_yield(self, query: str, params: tuple = ()) -> Iterator[Dict[str, Any]]:
        from src.mdb_sync.concurrency import mdb_lock
        
        def robust_date_handler(value):
            if value is None:
                return None
            try:
                return str(value)
            except Exception:
                return repr(value)

        def _read_data():
            rows_fetched = []
            with mdb_lock:
                with pyodbc.connect(self.conn_str, readonly=True) as conn:
                    if "MDBTools" in self.conn_str:
                        conn.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP, robust_date_handler)
                        conn.add_output_converter(pyodbc.SQL_TYPE_DATE, robust_date_handler)
                    
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    columns = [column[0].strip() for column in cursor.description]
                    
                    while True:
                        try:
                            row = cursor.fetchone()
                            if row is None:
                                break
                            rows_fetched.append(dict(zip(columns, row)))
                        except Exception as e:
                            logger.error("Failed to fetch individual row from MDB", error=str(e))
                            continue
            return rows_fetched

        start_time = time.time()
        try:
            # Execute with breaker
            rows = mdb_breaker(_read_data)
            
            # Record latency
            elapsed_ms = (time.time() - start_time) * 1000
            MDB_LATENCY.labels(operation="execute_query").observe(elapsed_ms)
            
            # Yield outside the lock
            for row_dict in rows:
                yield row_dict

        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            MDB_LATENCY.labels(operation="execute_query_error").observe(elapsed_ms)
            logger.error("MDB query failed", error=str(e), query=query)
            raise

    def get_new_records(self, table: str, pk_col: str, last_pk: Optional[str]) -> Iterator[Dict[str, Any]]:
        if "MDBTools" in self.conn_str:
            query = f"SELECT * FROM {table}"
            return self.execute_query_yield(query)
        
        if last_pk:
            query = f"SELECT * FROM {table} WHERE {pk_col} > ? ORDER BY {pk_col} ASC"
            return self.execute_query_yield(query, (last_pk,))
        else:
            query = f"SELECT * FROM {table} ORDER BY {pk_col} ASC"
            return self.execute_query_yield(query)

    def get_full_scan(self, table: str, pk_col: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        if "MDBTools" in self.conn_str:
            query = f"SELECT * FROM {table}"
            return self.execute_query_yield(query)
            
        if pk_col:
            query = f"SELECT * FROM {table} ORDER BY {pk_col} ASC"
        else:
            query = f"SELECT * FROM {table}"
        return self.execute_query_yield(query)
