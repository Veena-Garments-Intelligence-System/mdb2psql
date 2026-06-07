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

        # Optimization: Use a generator to avoid materializing large lists in memory.
        # We handle retries at the connection level. If a fetch fails mid-query, 
        # we retry the entire query (relying on upserts to handle duplicates).
        max_retries = 3
        retry_delay = 2
        
        start_time = time.time()
        
        for attempt in range(max_retries):
            try:
                with mdb_lock:
                    # We wrap the connection and execution in the breaker via a helper
                    def _connect_and_execute():
                        conn = pyodbc.connect(self.conn_str, readonly=True)
                        if "MDBTools" in self.conn_str:
                            conn.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP, robust_date_handler)
                            conn.add_output_converter(pyodbc.SQL_TYPE_DATE, robust_date_handler)
                        
                        cursor = conn.cursor()
                        cursor.execute(query, params)
                        return conn, cursor

                    conn, cursor = mdb_breaker(_connect_and_execute)
                    
                    try:
                        columns = [column[0].strip() for column in cursor.description]
                        while True:
                            # We don't swallow all exceptions here to avoid infinite loops.
                            # HY000 will be caught by the outer block and trigger a retry.
                            row = cursor.fetchone()
                            if row is None:
                                break
                            yield dict(zip(columns, row))
                        
                        # Record success latency
                        elapsed_ms = (time.time() - start_time) * 1000
                        MDB_LATENCY.labels(operation="execute_query").observe(elapsed_ms)
                        return # Successfully finished
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass

            except Exception as e:
                err_msg = str(e)
                is_last_attempt = (attempt == max_retries - 1)
                
                # Specifically handle HY000 and connection errors for retry
                if ("HY000" in err_msg or "connection" in err_msg.lower()) and not is_last_attempt:
                    logger.debug(
                        "MDB retryable error detected. Retrying entire query...",
                        attempt=attempt + 1,
                        error=err_msg.split('\n')[0]
                    )
                    time.sleep(retry_delay)
                    continue
                
                # Record error latency
                elapsed_ms = (time.time() - start_time) * 1000
                MDB_LATENCY.labels(operation="execute_query_error").observe(elapsed_ms)
                logger.error("MDB query failed permanently", error=err_msg, query=query)
                raise e

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
