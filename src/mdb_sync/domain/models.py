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
    opening_balance: Optional[float] = None

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
    is_ok: Optional[int] = 0
    bill_amount: Optional[float] = None
    dis: Optional[float] = None
    packing_case: Optional[float] = None
    packing_amount: Optional[float] = None
    transport_name: Optional[str] = None
    transport_amount: Optional[float] = None
    total_packs: Optional[float] = None
    bdl: Optional[float] = None
    dispatch_case: Optional[int] = None
    dispatch_bdl: Optional[int] = None
    fright: Optional[float] = None
    gross_amt5: Optional[float] = None
    gross_amt12: Optional[float] = None
    total_tax5: Optional[float] = None
    total_tax12: Optional[float] = None
    is_gst: Optional[bool] = False
    gr_no: Optional[str] = None
    is_cash: Optional[str] = None
    customer_name: Optional[str] = None
    city_id: Optional[str] = None
    note: Optional[str] = None
    user_name: Optional[str] = None
    add_date: Optional[str] = None
    bill_group: Optional[str] = None
    ref_no: Optional[float] = None
    company_id: Optional[str] = None
    session_id: Optional[str] = None
    is_lock: Optional[bool] = False
    is_deleted: Optional[bool] = False

class BillDetail(BaseEntity):
    bill_id: str
    line_idx: int
    item_id: Optional[str] = None
    item_name: Optional[str] = None
    size_id: Optional[str] = None
    size: Optional[int] = None
    qty: Optional[float] = None
    rate: Optional[float] = None
    scheme: Optional[str] = None
    amount: Optional[float] = None
    sr: Optional[bool] = False
    mtsr: Optional[float] = None
    company_id: Optional[str] = None
    group_name: Optional[str] = None
    stock_id: Optional[int] = None
    unit: Optional[str] = None
    netrate: Optional[float] = None
    taxcode: Optional[float] = None
    discount: Optional[float] = None
    attend_by: Optional[str] = None
    inc_per: Optional[float] = None
    bill_type: Optional[int] = None
    hbr: Optional[bool] = False
    is_deleted: Optional[bool] = False

class LRMaster(BaseEntity):
    lr_id: str
    entry_idx: Optional[int] = 1
    bill_id: Optional[str] = None
    company_id: Optional[str] = None
    sno: Optional[int] = None
    customer_id: Optional[str] = None
    entry_date: Optional[str] = None
    customer_name: Optional[str] = None
    city_id: Optional[str] = None
    total_packs: Optional[float] = None
    gr_no: Optional[str] = None
    lr_date: Optional[str] = None
    transport_name: Optional[str] = None
    ps: Optional[str] = None
    note: Optional[str] = None
    printed: Optional[bool] = False
    is_lock: Optional[bool] = False
    packing_case: Optional[float] = None
    packing_amount: Optional[float] = None
    transport_amount: Optional[float] = None
    is_update: Optional[bool] = False
    user_name: Optional[str] = None
    add_date: Optional[str] = None
    add_time: Optional[str] = None
    is_edit_lock: Optional[bool] = False
    is_ok: Optional[int] = 0
    lock_date: Optional[str] = None
    bdl: Optional[float] = None
    bill_group: Optional[str] = None
    ref_no: Optional[float] = None
    is_dispatch: Optional[bool] = False
    clear_remark: Optional[str] = None
    lock_user: Optional[str] = None
    lock_user_date: Optional[str] = None
    checked: Optional[str] = None
    is_lr: Optional[bool] = False
    pickup_by: Optional[str] = None
    is_bus: Optional[bool] = False
    is_sms_sent: Optional[bool] = False
    auto_freight: Optional[float] = None
    is_deleted: Optional[bool] = False

class LRDetail(BaseEntity):
    lr_id: str
    bill_id: str
    line_idx: Optional[int] = 1
    sno: Optional[str] = None
    case_count: Optional[int] = None
    bdl: Optional[float] = None
    dcase: Optional[float] = None
    dbdl: Optional[int] = None
    rsno: Optional[str] = None
    freight: Optional[float] = None
    is_deleted: Optional[bool] = False

class DeleteRecord(BaseEntity):
    bill_id: str
    bill_no: Optional[str] = None
    bill_date: Optional[str] = None
    customer_name: Optional[str] = None
    city_name: Optional[str] = None
    net_amount: Optional[float] = None
    user_name: Optional[str] = None
    delete_date: Optional[str] = None
    delete_time: Optional[str] = None
    remark: Optional[str] = None

class Receipt(BaseEntity):
    receipt_id: str
    customer_id: Optional[str] = None
    receipt_date: Optional[str] = None
    amount: Optional[float] = None
    discount: Optional[float] = None
    bank_name: Optional[str] = None
    receipt_type: Optional[str] = None
    is_ok: Optional[int] = 0

class RG(BaseEntity):
    rg_id: str
    customer_id: Optional[str] = None
    rgtype: Optional[str] = None
    bill_date: Optional[str] = None
    net_amount: Optional[float] = None
    is_ok: Optional[int] = 0

