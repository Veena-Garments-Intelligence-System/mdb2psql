import os
import hashlib
from src.mdb_sync.config import settings
from src.mdb_sync.logging_config import get_logger
from src.mdb_sync.utils.alerting import send_alert

logger = get_logger(__name__)

# Keep track of previous file state to detect changes
_last_state = {
    "sha256": None,
    "size": None,
    "mtime": None
}

def verify_mdb_integrity() -> bool:
    """
    Performs comprehensive verification of the MDB file before each sync cycle.
    Calculates sha256, size, mtime and logs any changes.
    """
    path = settings.MDB_PATH
    
    # 1. Existence check
    if not os.path.exists(path):
        logger.error("MDB file does not exist", path=path)
        send_alert("mdb unavailable", "MDB File Missing", f"The MDB file at '{path}' could not be found.")
        return False
        
    # 2. Readability check
    if not os.access(path, os.R_OK):
        logger.error("MDB file is not readable (permission denied)", path=path)
        send_alert("mdb unavailable", "MDB File Unreadable", f"Permission denied while reading '{path}'.")
        return False

    # 3. Lock files check (informational)
    base, ext = os.path.splitext(path)
    # Access lock file formats: .ldb or .laccdb
    for lock_ext in [".ldb", ".laccdb"]:
        lock_path = base + lock_ext
        if os.path.exists(lock_path):
            logger.debug("MDB lock file detected (Access may be active)", lock_path=lock_path)
            
    # 4. Check corruption and calculate details
    try:
        size = os.path.getsize(path)
        mtime = os.path.getmtime(path)
        
        if size == 0:
            logger.error("MDB file is empty (0 bytes)", path=path)
            send_alert("mdb unavailable", "MDB File Empty", f"The file at '{path}' is empty.")
            return False
            
        # Calculate SHA256 (read in chunks of 64KB)
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            chunk = f.read(65536)
            while chunk:
                hasher.update(chunk)
                chunk = f.read(65536)
        sha256 = hasher.hexdigest()
        
        # 5. Detect and log changes
        changed = False
        change_details = []
        
        if _last_state["sha256"] is not None and _last_state["sha256"] != sha256:
            changed = True
            change_details.append(f"hash changed (from {_last_state['sha256'][:8]} to {sha256[:8]})")
        if _last_state["size"] is not None and _last_state["size"] != size:
            changed = True
            change_details.append(f"size changed (from {_last_state['size']} to {size} bytes)")
        if _last_state["mtime"] is not None and _last_state["mtime"] != mtime:
            changed = True
            change_details.append("modified time changed")

        if changed:
            logger.info("MDB file changes detected", details=", ".join(change_details), path=path, sha256=sha256, size=size)
        else:
            logger.debug("MDB file integrity verified (no changes)", path=path, sha256=sha256, size=size)
            
        # Update state
        _last_state["sha256"] = sha256
        _last_state["size"] = size
        _last_state["mtime"] = mtime
        
        return True
    except Exception as e:
        logger.exception("MDB integrity check failed with exception (corrupted or locked exclusively)", path=path)
        send_alert("mdb unavailable", "MDB Corruption/Lock Error", f"Failed checking file integrity: {e}")
        return False
