import json
import urllib.request
from src.mdb_sync.config import settings
from src.mdb_sync.logging_config import get_logger

logger = get_logger(__name__)

def send_alert(alert_type: str, title: str, message: str, metadata: dict = None) -> bool:
    """
    Dispatches alerts to configured channels (Slack, Webhook).
    Does not raise exceptions on delivery failures to keep the sync platform running.
    """
    logger.warning("ALERT TRIGGERED", alert_type=alert_type, title=title, message=message, metadata=metadata)
    
    payload = {
        "alert_type": alert_type,
        "title": title,
        "message": message,
        "metadata": metadata or {}
    }
    
    success = True
    
    # 1. Slack Alert
    if settings.SLACK_WEBHOOK_URL:
        try:
            req_data = json.dumps({
                "text": f"*[{alert_type}] {title}*\n{message}\nMetadata: {json.dumps(metadata or {})}"
            }).encode("utf-8")
            
            req = urllib.request.Request(
                settings.SLACK_WEBHOOK_URL,
                data=req_data,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status not in (200, 204):
                    logger.error("Slack alert failed", status=response.status)
                    success = False
        except Exception as e:
            logger.error("Failed sending Slack alert", error=str(e))
            success = False
            
    # 2. Generic Webhook Alert
    if settings.WEBHOOK_URL:
        try:
            req_data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                settings.WEBHOOK_URL,
                data=req_data,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status not in (200, 201, 204):
                    logger.error("Webhook alert failed", status=response.status)
                    success = False
        except Exception as e:
            logger.error("Failed sending Webhook alert", error=str(e))
            success = False
            
    return success
