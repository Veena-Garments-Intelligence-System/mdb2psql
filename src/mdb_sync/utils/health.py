import os
import json
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from src.mdb_sync.config import settings
from src.mdb_sync.infrastructure.postgres.database import SessionLocal
from sqlalchemy import select
from src.mdb_sync.logging_config import get_logger
from src.mdb_sync.utils.metrics import update_pool_metrics

logger = get_logger(__name__)

# Shared state to track scheduler health
class SchedulerState:
    last_cycle_completed_at = None
    last_cycle_status = "UNKNOWN"
    thread = None

scheduler_state = SchedulerState()

def check_postgres() -> bool:
    try:
        with SessionLocal() as db:
            db.execute(select(1))
            return True
    except Exception:
        return False

def check_mdb() -> tuple[bool, str]:
    path = settings.MDB_PATH
    if not os.path.exists(path):
        return False, "File does not exist"
    if not os.access(path, os.R_OK):
        return False, "File is not readable"
    
    try:
        size = os.path.getsize(path)
        if size == 0:
            return False, "File is empty"
        return True, "OK"
    except Exception as e:
        return False, f"Error: {e}"

def check_checkpoints() -> tuple[bool, str]:
    from src.mdb_sync.infrastructure.postgres.models import SyncCheckpoint
    try:
        with SessionLocal() as db:
            count = db.query(SyncCheckpoint).count()
            return True, f"{count} checkpoints found"
    except Exception as e:
        return False, f"Error: {e}"

def perform_health_checks():
    pg_ok = check_postgres()
    mdb_ok, mdb_msg = check_mdb()
    checkpoint_ok, cp_msg = check_checkpoints()
    
    sched_ok = False
    if scheduler_state.thread and scheduler_state.thread.is_alive():
        sched_ok = True
        
    is_healthy = pg_ok and mdb_ok and checkpoint_ok and sched_ok
    
    status_data = {
        "status": "UP" if is_healthy else "DOWN",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {
            "postgres": "UP" if pg_ok else "DOWN",
            "mdb": {
                "status": "UP" if mdb_ok else "DOWN",
                "message": mdb_msg
            },
            "checkpoints": {
                "status": "UP" if checkpoint_ok else "DOWN",
                "message": cp_msg
            },
            "scheduler": {
                "status": "UP" if sched_ok else "DOWN",
                "last_cycle_completed_at": scheduler_state.last_cycle_completed_at.isoformat() if scheduler_state.last_cycle_completed_at else None,
                "last_cycle_status": scheduler_state.last_cycle_status
            }
        }
    }
    return is_healthy, status_data

class HealthAndMetricsHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Prevent spamming console with metrics poll requests
        pass

    def do_GET(self):
        update_pool_metrics()
        
        if self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(generate_latest())
            
        elif self.path == "/health":
            is_healthy, status_data = perform_health_checks()
            self.send_response(200 if is_healthy else 500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(status_data).encode("utf-8"))
            
        elif self.path == "/readiness":
            pg_ok = check_postgres()
            mdb_ok, _ = check_mdb()
            is_ready = pg_ok and mdb_ok
            
            self.send_response(200 if is_ready else 503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ready": is_ready,
                "postgres": "UP" if pg_ok else "DOWN",
                "mdb": "UP" if mdb_ok else "DOWN"
            }).encode("utf-8"))
            
        elif self.path == "/liveness":
            sched_alive = scheduler_state.thread is not None and scheduler_state.thread.is_alive()
            self.send_response(200 if sched_alive else 500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "live": sched_alive,
                "scheduler_thread_alive": sched_alive
            }).encode("utf-8"))
            
        else:
            self.send_response(404)
            self.end_headers()

def start_health_server():
    host = settings.HEALTH_HOST
    port = settings.HEALTH_PORT
    
    server = HTTPServer((host, port), HealthAndMetricsHandler)
    logger.info("Starting Health and Metrics HTTP Server", host=host, port=port)
    
    t = threading.Thread(target=server.serve_forever, daemon=True, name="HealthMetricsServer")
    t.start()
    return server
