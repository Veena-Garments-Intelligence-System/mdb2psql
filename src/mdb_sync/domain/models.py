import hashlib
import json
import unicodedata
from datetime import datetime, timezone, date
from typing import Optional, Any
from pydantic import BaseModel, ConfigDict, computed_field

def canonicalize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        # Normalize unicode, strip whitespace, uppercase
        s = unicodedata.normalize('NFKC', v).strip().upper()
        return s if s else None
    if isinstance(v, float):
        # Fixed precision for floats to avoid representation variations
        return f"{v:.4f}"
    if isinstance(v, datetime):
        # Ensure UTC, ISO format
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        else:
            v = v.astimezone(timezone.utc)
        return v.isoformat()
    if isinstance(v, date):
        # ISO format for date (YYYY-MM-DD)
        return v.isoformat()
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)

class BaseEntity(BaseModel):
    model_config = ConfigDict(from_attributes=True, coerce_numbers_to_str=True)

    @computed_field
    def checksum(self) -> str:
        # Get all fields except excluded ones for checksum
        data = self.model_dump(exclude={"checksum", "raw_id", "source_system", "is_processed", "created_at", "updated_at"})
        
        normalized_data = {}
        for k, v in data.items():
            normalized_data[k] = canonicalize_value(v)

        # Ensure deterministic JSON for hashing
        encoded = json.dumps(normalized_data, sort_keys=True, separators=(',', ':')).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

class Customer(BaseEntity):
    customer_id: str
    customer_name: Optional[str] = None
    city_id: Optional[str] = None
    mobile1: Optional[str] = None

class City(BaseEntity):
    city_id: str
    city_name: Optional[str] = None
    group_id: Optional[str] = None

class Sale(BaseEntity):
    bill_id: str
    customer_id: Optional[str] = None
    bill_date: Optional[str] = None
    net_amount: Optional[float] = None
    dis_amt: Optional[float] = None

class Receipt(BaseEntity):
    receipt_id: str
    customer_id: Optional[str] = None
    receipt_date: Optional[str] = None
    amount: Optional[float] = None
    discount: Optional[float] = None
    bank_name: Optional[str] = None
    receipt_type: Optional[str] = None

class RG(BaseEntity):
    rg_id: str
    customer_id: Optional[str] = None
    rgtype: Optional[str] = None
    bill_date: Optional[str] = None
    net_amount: Optional[float] = None

