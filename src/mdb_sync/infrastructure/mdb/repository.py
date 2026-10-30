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

        # Retries up to max_retries on connection or transient MDB lock errors (HY000)
        # ONLY IF no rows have been yielded yet.
        # Once records have begun streaming to the caller, mid-stream failures raise immediately
        # to prevent duplicate generator yields (FINDING-005).
        # mdb_lock is acquired only for fetching batches, not held during downstream yields (FINDING-011).
        max_retries = 3
        retry_delay = 2
        start_time = time.time()
        has_yielded = False
        
        for attempt in range(max_retries):
            conn = None
            cursor = None
            try:
                def _connect_and_execute():
                    nonlocal conn, cursor
                    with mdb_lock:
                        c = pyodbc.connect(self.conn_str, readonly=True)
                        if "MDBTools" in self.conn_str:
                            c.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP, robust_date_handler)
                            c.add_output_converter(pyodbc.SQL_TYPE_DATE, robust_date_handler)
                        
                        cur = c.cursor()
                        cur.execute(query, params)
                        return c, cur

                conn, cursor = mdb_breaker(_connect_and_execute)
                columns = [column[0].strip() for column in cursor.description]

                while True:
                    rows = []
                    with mdb_lock:
                        if hasattr(cursor, "fetchmany"):
                            res = cursor.fetchmany(self.chunk_size)
                            if isinstance(res, list):
                                rows = res
                            else:
                                row = cursor.fetchone()
                                if row is not None:
                                    rows = [row]
                        else:
                            row = cursor.fetchone()
                            if row is not None:
                                rows = [row]

                    if not rows:
                        break

                    for row in rows:
                        has_yielded = True
                        yield dict(zip(columns, row))

                elapsed_ms = (time.time() - start_time) * 1000
                MDB_LATENCY.labels(operation="execute_query").observe(elapsed_ms)
                return

            except Exception as e:
                err_msg = str(e)
                # CRITICAL: Prevent duplicate yields on mid-stream failure (FINDING-005)
                if has_yielded:
                    elapsed_ms = (time.time() - start_time) * 1000
                    MDB_LATENCY.labels(operation="execute_query_error").observe(elapsed_ms)
                    logger.error("MDB mid-stream query failure after yielding rows; aborting without retry to prevent duplicates",
                                 error=err_msg, query=query)
                    raise e

                is_last_attempt = (attempt == max_retries - 1)
                if ("HY000" in err_msg or "connection" in err_msg.lower()) and not is_last_attempt:
                    logger.debug(
                        "MDB retryable error detected before any rows yielded. Retrying...",
                        attempt=attempt + 1,
                        error=err_msg.split('\n')[0]
                    )
                    time.sleep(retry_delay)
                    continue

                elapsed_ms = (time.time() - start_time) * 1000
                MDB_LATENCY.labels(operation="execute_query_error").observe(elapsed_ms)
                logger.error("MDB query failed permanently", error=err_msg, query=query)
                raise e
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def get_new_records(self, table: str, pk_col: str, last_pk: Optional[Any], pk_type: type = str) -> Iterator[Dict[str, Any]]:
        if "MDBTools" in self.conn_str:
            query = f"SELECT * FROM {table}"
            return self.execute_query_yield(query)
        
        if last_pk is not None and str(last_pk).strip() != "":
            try:
                param = int(float(str(last_pk).strip())) if pk_type is int else str(last_pk).strip()
            except (ValueError, TypeError):
                param = str(last_pk).strip()
            query = f"SELECT * FROM {table} WHERE {pk_col} > ? ORDER BY {pk_col} ASC"
            return self.execute_query_yield(query, (param,))
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

