import uuid
from typing import Dict, Any, Optional
from datetime import datetime, date, timezone
from src.mdb_sync.domain import models
from src.mdb_sync.infrastructure.postgres import models as pg_models
from src.mdb_sync.logging_config import get_logger

logger = get_logger(__name__)

class DataMapper:
    MAPPING = {
        "BILL_MASTER": {
            "model": models.Sale,
            "pg_model": pg_models.RawSale,
            "pk": "Bill_ID",
            "pg_pk": "bill_id",
            "pk_type": int,
            "fields": {
                "Bill_ID": "bill_id",
                "CUSTOMER_ID": "customer_id",
                "BILL_DATE": "bill_date",
                "NET_AMOUNT": "net_amount",
                "Dis_Amt": "dis_amt",
                "is_Ok": "is_ok",
                "BILL_AMOUNT": "bill_amount",
                "Dis": "dis",
                "Packing_Case": "packing_case",
                "Packing_Amount": "packing_amount",
                "TransportName": "transport_name",
                "Transport_Amount": "transport_amount",
                "TotalPacks": "total_packs",
                "BDL": "bdl",
                "Dispatch_Case": "dispatch_case",
                "Dispatch_BDL": "dispatch_bdl",
                "Fright": "fright",
                "GrossAmt5": "gross_amt5",
                "GrossAmt12": "gross_amt12",
                "TotalTax5": "total_tax5",
                "TotalTax12": "total_tax12",
                "Is_GST": "is_gst",
                "Gr_No": "gr_no",
                "Is_Cash": "is_cash",
                "Customer_Name": "customer_name",
                "City_ID": "city_id",
                "Note": "note",
                "User_Name": "user_name",
                "Add_Date": "add_date",
                "Bill_Group": "bill_group",
                "Ref_no": "ref_no",
                "Company_ID": "company_id",
                "Session_Id": "session_id",
            }
        },
        "Bill_Detail": {
            "model": models.BillDetail,
            "pg_model": pg_models.RawBillDetail,
            "pk": ["Bill_ID", "line_idx"],
            "pg_pk": ["bill_id", "line_idx"],
            "pk_type": int,
            "fields": {
                "Bill_ID": "bill_id",
                "line_idx": "line_idx",
                "Item_ID": "item_id",
                "Item_Name": "item_name",
                "Size_ID": "size_id",
                "Size": "size",
                "Qty": "qty",
                "RATE": "rate",
                "Scheme": "scheme",
                "AMOUNT": "amount",
                "SR": "sr",
                "MTSR": "mtsr",
                "Company_ID": "company_id",
                "Group": "group_name",
                "Stock_Id": "stock_id",
                "UNIT": "unit",
                "NETRATE": "netrate",
                "TaxCode": "taxcode",
                "DISCOUNT": "discount",
                "Attend_By": "attend_by",
                "Inc_per": "inc_per",
                "Bill_Type": "bill_type",
                "HBR": "hbr",
            }
        },
        "LR_MASTER": {
            "model": models.LRMaster,
            "pg_model": pg_models.RawLRMaster,
            "pk": ["LR_ID", "entry_idx"],
            "pg_pk": ["lr_id", "entry_idx"],
            "pk_type": int,
            "fields": {
                "LR_ID": "lr_id",
                "entry_idx": "entry_idx",
                "Bill_ID": "bill_id",
                "Company_ID": "company_id",
                "Sno": "sno",
                "CUSTOMER_ID": "customer_id",
                "Entry_Date": "entry_date",
                "Customer_Name": "customer_name",
                "City_ID": "city_id",
                "TotalPacks": "total_packs",
                "Gr_No": "gr_no",
                "LR_Date": "lr_date",
                "TransportName": "transport_name",
                "PS": "ps",
                "Note": "note",
                "Printed": "printed",
                "Is_Lock": "is_lock",
                "Packing_Case": "packing_case",
                "Packing_Amount": "packing_amount",
                "Transport_Amount": "transport_amount",
                "Is_Update": "is_update",
                "User_Name": "user_name",
                "Add_Date": "add_date",
                "Add_Time": "add_time",
                "Is_editLock": "is_edit_lock",
                "is_Ok": "is_ok",
                "Lock_Date": "lock_date",
                "BDL": "bdl",
                "Bill_Group": "bill_group",
                "Ref_no": "ref_no",
                "Is_Dispatch": "is_dispatch",
                "Clear_Remark": "clear_remark",
                "Lock_User": "lock_user",
                "Lock_User_Date": "lock_user_date",
                "Checked": "checked",
                "IS_LR": "is_lr",
                "PickupBy": "pickup_by",
                "IS_BUS": "is_bus",
                "Is_SMSSent": "is_sms_sent",
                "AUTO_FREIGHT": "auto_freight",
            }
        },
        "LR_Detail": {
            "model": models.LRDetail,
            "pg_model": pg_models.RawLRDetail,
            "pk": ["LR_Id", "Bill_ID", "line_idx"],
            "pg_pk": ["lr_id", "bill_id", "line_idx"],
            "pk_type": int,
            "fields": {
                "LR_Id": "lr_id",
                "Bill_ID": "bill_id",
                "line_idx": "line_idx",
                "SNO": "sno",
                "Case": "case_count",
                "BDL": "bdl",
                "DCase": "dcase",
                "DBDL": "dbdl",
                "RSNO": "rsno",
                "FREIGHT": "freight",
            }
        },
        "Delete_Record": {
            "model": models.DeleteRecord,
            "pg_model": pg_models.RawDeleteRecord,
            "pk": "Bill_ID",
            "pg_pk": "bill_id",
            "pk_type": int,
            "fields": {
                "Bill_ID": "bill_id",
                "Bill_No": "bill_no",
                "Bill_Date": "bill_date",
                "Customer_Name": "customer_name",
                "City_Name": "city_name",
                "Net_Amount": "net_amount",
                "User_Name": "user_name",
                "Delete_Date": "delete_date",
                "Delete_Time": "delete_time",
                "Remark": "remark",
            }
        },
        "Receipt_Master": {
            "model": models.Receipt,
            "pg_model": pg_models.RawReceipt,
            "pk": "Receipt_ID",
            "pg_pk": "receipt_id",
            "pk_type": int,
            "fields": {
                "Receipt_ID": "receipt_id",
                "Customer_ID": "customer_id",
                "Receipt_Date": "receipt_date",
                "Amount": "amount",
                "Discount": "discount",
                "Bank_Name": "bank_name",
                "Receipt_Type": "receipt_type",
                "is_Ok": "is_ok",
            }
        },
        "ReturnGoods": {
            "model": models.RG,
            "pg_model": pg_models.RawRG,
            "pk": "RG_ID",
            "pg_pk": "rg_id",
            "pk_type": int,
            "fields": {
                "RG_ID": "rg_id",
                "CUSTOMER_ID": "customer_id",
                "RGTYPE": "rgtype",
                "BILL_DATE": "bill_date",
                "NET_AMOUNT": "net_amount",
                "is_Ok": "is_ok",
            }
        },

        "CUSTOMER_MASTER": {
            "model": models.Customer,
            "pg_model": pg_models.RawCustomer,
            "pk": "CUSTOMER_ID",
            "pg_pk": "customer_id",
            "pk_type": int,
            "fields": {
                "CUSTOMER_ID": "customer_id",
                "CUSTOMER_NAME": "customer_name",
                "City_ID": "city_id",
                "MOBILE1": "mobile1",
                "Opening_Balance": "opening_balance",
            }
        },
        "City_Master": {
            "model": models.City,
            "pg_model": pg_models.RawCity,
            "pk": "City_ID",
            "pg_pk": "city_id",
            "pk_type": int,
            "fields": {
                "City_ID": "city_id",
                "City_Name": "city_name",
                "Group_ID": "group_id",
            }
        }
    }

    @staticmethod
    def compare_pks(pk1: Any, pk2: Any, pk_type: type = str) -> int:
        """
        Compares two primary keys according to declared pk_type.
        Returns:
            1 if pk1 > pk2
            -1 if pk1 < pk2
            0 if pk1 == pk2
        Raises ValueError if values cannot be coerced to declared pk_type.
        """
        if pk_type is int:
            try:
                v1 = int(float(str(pk1).strip()))
                v2 = int(float(str(pk2).strip()))
                return 1 if v1 > v2 else (-1 if v1 < v2 else 0)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Cannot compare primary keys as int: pk1={pk1!r}, pk2={pk2!r}") from e
        else:
            s1 = str(pk1).strip()
            s2 = str(pk2).strip()
            return 1 if s1 > s2 else (-1 if s1 < s2 else 0)

    @staticmethod
    def is_greater_pk(pk1: Any, pk2: Any, pk_type: type = str) -> bool:
        """Returns True if pk1 > pk2 using declared pk_type."""
        return DataMapper.compare_pks(pk1, pk2, pk_type) > 0


    @staticmethod
    def _parse_date(val: Any) -> Optional[date]:
        if val is None or val == "":
            return None

        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val

        # Convert to string and strip
        s_val = str(val).strip()

        # REQUIREMENT: split by space and use [0] index (date part), ignoring time
        parts = s_val.split()
        if not parts:
            return None

        raw_date = parts[0]

        # Robust cleaning: extract just the date pattern
        import re
        # Support /, -, and . as separators
        date_match = re.search(r'(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})|(\d{4}-\d{1,2}-\d{1,2})', raw_date)
        if date_match:
            raw_date = date_match.group(0)

        # Normalize separators to / for easier parsing if needed, 
        # but strptime needs exact matches for the format string.
        # We'll just add the dot-based formats.
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d/%m/%y",
            "%m/%d/%Y",
            "%m/%d/%y",
            "%d-%m-%Y",
            "%d-%m-%y",
            "%m-%d-%Y",
            "%m-%d-%y",
            "%d.%m.%Y",
            "%d.%m.%y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(raw_date, fmt)
                return dt.date()
            except Exception:
                continue

        logger.debug("Date parsing failed", original=val, extracted=raw_date)
        return None

    @staticmethod
    def _parse_to_datetime(val: Any) -> Optional[datetime]:
        if val is None or val == "":
            return None
        
        if isinstance(val, datetime):
            return val
        if isinstance(val, (date, datetime)):
            try:
                # Handle cases where it might be a date object
                if isinstance(val, date) and not isinstance(val, datetime):
                    return datetime.combine(val, datetime.min.time()).replace(tzinfo=timezone.utc)
                return val
            except Exception:
                pass

        # Try to parse string
        s_val = str(val).strip()
        
        # User requested support for "08/05/22 00:00:00"
        # We'll try common patterns
        formats = [
            "%d/%m/%y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d-%m-%y %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%m/%d/%y %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%d/%m/%y", # Just date part
            "%d/%m/%Y",
            "%Y-%m-%d",
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(s_val, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
        
        # Fallback to existing date parser
        d = DataMapper._parse_date(val)
        if d:
            return datetime.combine(d, datetime.min.time()).replace(tzinfo=timezone.utc)
            
        return None

    @staticmethod
    def _parse_float(val: Any) -> Optional[float]:
        if val is None or val == "":
            return None
        try:
            s_val = str(val).strip()
            import re
            numeric_match = re.search(r'[-+]?\d*\.?\d+', s_val)
            if numeric_match:
                return float(numeric_match.group(0))
            return None
        except Exception:
            return None

    @staticmethod
    def _parse_int(val: Any) -> Optional[int]:
        if val is None or val == "":
            return None
        
        if isinstance(val, bool):
            return 1 if val else 0
            
        try:
            s_val = str(val).strip().lower()
            if s_val in ["true", "yes", "y", "-1"]:
                return 1
            if s_val in ["false", "no", "n"]:
                return 0
                
            import re
            numeric_match = re.search(r'[-+]?\d+', s_val)
            if numeric_match:
                return int(numeric_match.group(0))
            return None
        except Exception:
            return None

    @staticmethod
    def _parse_bool(val: Any) -> bool:
        if val is None or val == "":
            return False
        if isinstance(val, bool):
            return val
        s_val = str(val).strip().lower()
        if s_val in ["true", "yes", "y", "1", "-1"]:
            return True
        return False

    @staticmethod
    def _decode_binary_timestamp(val: Any) -> Optional[str]:
        """Decodes 16-byte MDB TIMESTAMP_STRUCT binary data."""
        if not val:
            return None
        
        raw_bytes = None
        if isinstance(val, bytes):
            raw_bytes = val
        elif isinstance(val, str):
            s_val = val.strip()
            if s_val.startswith("b'") or s_val.startswith('b"'):
                # Try to evaluate the bytes literal safely
                import ast
                try:
                    raw_bytes = ast.literal_eval(s_val)
                except Exception:
                    pass
            elif s_val.startswith("\\x"):
                # Handle hex strings
                try:
                    raw_bytes = bytes.fromhex(s_val[2:])
                except Exception:
                    pass

        if raw_bytes and len(raw_bytes) == 16:
            import struct
            try:
                # SQL TIMESTAMP_STRUCT: year(h), month(h), day(h), hour(h), minute(h), second(h), fraction(I)
                # All are little-endian
                year, month, day, hour, minute, second, _ = struct.unpack("<hhhhhhI", raw_bytes)
                if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                    return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
            except Exception:
                pass
        
        return None

    @staticmethod
    def map_to_domain(table_name: str, mdb_row: Dict[str, Any], **extra_fields) -> models.BaseEntity:
        config = DataMapper.MAPPING[table_name]
        mapped_data = {}
        
        row_lower = {k.lower(): v for k, v in mdb_row.items()}
        row_keys_lower = [k.lower() for k in mdb_row.keys()]
        
        date_cols = {"bill_date", "receipt_date", "entry_date", "lr_date", "delete_date", "add_date", "lock_date", "lock_user_date"}
        float_cols = {
            "net_amount", "amount", "discount", "dis_amt", "opening_balance", "bill_amount",
            "dis", "packing_case", "packing_amount", "transport_amount", "total_packs", "bdl",
            "fright", "gross_amt5", "gross_amt12", "total_tax5", "total_tax12", "ref_no",
            "qty", "rate", "mtsr", "netrate", "taxcode", "inc_per", "auto_freight", "dcase", "freight"
        }
        int_cols = {"is_ok", "dispatch_case", "dispatch_bdl", "size", "stock_id", "bill_type", "sno", "case_count", "dbdl", "line_idx", "entry_idx"}
        bool_cols = {"is_gst", "sr", "hbr", "printed", "is_lock", "is_update", "is_edit_lock", "is_dispatch", "is_lr", "is_bus", "is_sms_sent", "is_deleted"}

        for mdb_col, domain_col in config["fields"].items():
            val = None
            if mdb_col.lower() in row_keys_lower:
                val = mdb_row.get(mdb_col)
                if val is None:
                    val = row_lower.get(mdb_col.lower())
            elif domain_col in extra_fields:
                val = extra_fields[domain_col]
            else:
                logger.debug("COLUMN MISSING IN MDB ROW", table=table_name, column=mdb_col, available_columns=list(mdb_row.keys()))
            
            # Binary/Byte detection for dates
            if domain_col in date_cols and val is not None:
                decoded = DataMapper._decode_binary_timestamp(val)
                if decoded:
                    val = decoded

            if domain_col in bool_cols:
                mapped_data[domain_col] = DataMapper._parse_bool(val)
                continue

            if domain_col == "is_ok":
                val = DataMapper._parse_int(val)
                mapped_data[domain_col] = val if val is not None else 0
                continue

            if domain_col in int_cols:
                mapped_data[domain_col] = DataMapper._parse_int(val)
                continue

            if domain_col in float_cols:
                mapped_data[domain_col] = DataMapper._parse_float(val)
                continue

            if val is not None:
                if isinstance(val, bytes):
                    try:
                        val = val.decode('utf-8', errors='ignore')
                    except Exception:
                        val = str(val)

                s_val = str(val).strip()
                if s_val.startswith("b'") or s_val.startswith('b"') or s_val.startswith("\\x"):
                    import re
                    val = re.sub(r'[^\x20-\x7E]', '', s_val)
                    val = re.sub(r"^b['\"](.*)['\"]$", r"\1", val)
                else:
                    val = s_val

                if isinstance(val, str):
                    val = val.strip()
                    if val == "" or val.lower() in ["none", "null", "undefined"]:
                        val = None

            mapped_data[domain_col] = val
        
        # Merge extra fields explicitly (e.g. line_idx, entry_idx)
        for k, v in extra_fields.items():
            if k in int_cols:
                mapped_data[k] = DataMapper._parse_int(v)
            elif k in bool_cols:
                mapped_data[k] = DataMapper._parse_bool(v)
            else:
                mapped_data[k] = v

        return config["model"](**mapped_data)

    @staticmethod
    def map_to_pg(table_name: str, domain_model: models.BaseEntity, source_system: str) -> Dict[str, Any]:
        data = domain_model.model_dump()
        config = DataMapper.MAPPING[table_name]
        
        pg_pk = config["pg_pk"]
        if isinstance(pg_pk, list):
            entity_id = ":".join(str(data.get(col, "")) for col in pg_pk)
        else:
            entity_id = str(data.get(pg_pk, ""))
        
        namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        data["raw_id"] = uuid.uuid5(namespace, f"{table_name}:{entity_id}")
        
        data["checksum"] = domain_model.checksum
        data["source_system"] = source_system
        if hasattr(config["pg_model"], "is_processed"):
            data["is_processed"] = False
        if hasattr(config["pg_model"], "updated_at"):
            data["updated_at"] = datetime.now(timezone.utc)

        # POPULATE created_at FROM TRANSACTION DATE
        transactional_date = None
        for d_field in ["bill_date", "receipt_date", "entry_date", "lr_date", "delete_date", "add_date"]:
            if hasattr(domain_model, d_field) and getattr(domain_model, d_field):
                transactional_date = getattr(domain_model, d_field)
                break
            
        if transactional_date:
            dt = DataMapper._parse_to_datetime(transactional_date)
            if dt:
                data["created_at"] = dt
                
        return data
