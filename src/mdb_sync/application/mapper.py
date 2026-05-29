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
            "fields": {
                "Bill_ID": "bill_id",
                "CUSTOMER_ID": "customer_id",
                "BILL_DATE": "bill_date",
                "NET_AMOUNT": "net_amount",
                "Dis_Amt": "dis_amt",
            }
        },
        "Receipt_Master": {
            "model": models.Receipt,
            "pg_model": pg_models.RawReceipt,
            "pk": "Receipt_ID",
            "pg_pk": "receipt_id",
            "fields": {
                "Receipt_ID": "receipt_id",
                "Customer_ID": "customer_id",
                "Receipt_Date": "receipt_date",
                "Amount": "amount",
                "Discount": "discount",
                "Bank_Name": "bank_name",
                "Receipt_Type": "receipt_type",
            }
        },
        "ReturnGoods": {
            "model": models.RG,
            "pg_model": pg_models.RawRG,
            "pk": "RG_ID",
            "pg_pk": "rg_id",
            "fields": {
                "RG_ID": "rg_id",
                "CUSTOMER_ID": "customer_id",
                "RGTYPE": "rgtype",
                "BILL_DATE": "bill_date",
                "NET_AMOUNT": "net_amount",
            }
        },

        "CUSTOMER_MASTER": {
            "model": models.Customer,
            "pg_model": pg_models.RawCustomer,
            "pk": "CUSTOMER_ID",
            "pg_pk": "customer_id",
            "fields": {
                "CUSTOMER_ID": "customer_id",
                "CUSTOMER_NAME": "customer_name",
                "City_ID": "city_id",
                "MOBILE1": "mobile1",
            }
        },
        "City_Master": {
            "model": models.City,
            "pg_model": pg_models.RawCity,
            "pk": "City_ID",
            "pg_pk": "city_id",
            "fields": {
                "City_ID": "city_id",
                "City_Name": "city_name",
                "Group_ID": "group_id",
            }
        }
    }

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
    def map_to_domain(table_name: str, mdb_row: Dict[str, Any]) -> models.BaseEntity:
        config = DataMapper.MAPPING[table_name]
        mapped_data = {}
        
        row_lower = {k.lower(): v for k, v in mdb_row.items()}
        row_keys_lower = [k.lower() for k in mdb_row.keys()]
        
        for mdb_col, domain_col in config["fields"].items():
            if mdb_col.lower() not in row_keys_lower:
                logger.debug("COLUMN MISSING IN MDB ROW", table=table_name, column=mdb_col, available_columns=list(mdb_row.keys()))
                mapped_data[domain_col] = None
                continue

            val = mdb_row.get(mdb_col)
            if val is None:
                val = row_lower.get(mdb_col.lower())
            
            # Binary/Byte detection for dates
            if domain_col in ["bill_date", "receipt_date"]:
                decoded = DataMapper._decode_binary_timestamp(val)
                if decoded:
                    mapped_data[domain_col] = decoded
                    continue

            if val is not None:
                if isinstance(val, bytes):
                    # Fallback for other bytes
                    try:
                        val = val.decode('utf-8', errors='ignore')
                    except Exception:
                        val = str(val)

                s_val = str(val).strip()
                if s_val.startswith("b'") or s_val.startswith('b"') or s_val.startswith("\\x"):
                    # Only strip if it's not a binary date we care about (already handled above)
                    import re
                    # Keep alphanumeric and common punctuation
                    val = re.sub(r'[^\x20-\x7E]', '', s_val)
                    val = re.sub(r"^b['\"](.*)['\"]$", r"\1", val)
                else:
                    val = s_val

                if isinstance(val, str):
                    val = val.strip()
                    if val == "" or val.lower() in ["none", "null", "undefined"]:
                        val = None

            if domain_col in ["bill_date", "receipt_date"]:
                # User requested to just copy paste the dates as they are
                pass
            elif domain_col in ["net_amount", "amount", "discount", "dis_amt"]:
                val = DataMapper._parse_float(val)
                
            mapped_data[domain_col] = val
        
        return config["model"](**mapped_data)

    @staticmethod
    def map_to_pg(table_name: str, domain_model: models.BaseEntity, source_system: str) -> Dict[str, Any]:
        data = domain_model.model_dump()
        config = DataMapper.MAPPING[table_name]
        entity_id = str(getattr(domain_model, config["pg_pk"]))
        
        namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        data["raw_id"] = uuid.uuid5(namespace, f"{table_name}:{entity_id}")
        
        data["checksum"] = domain_model.checksum
        data["source_system"] = source_system
        data["is_processed"] = False

        # POPULATE created_at FROM TRANSACTION DATE
        # This applies to Sales, Receipts, and ReturnGoods as requested
        transactional_date = None
        if hasattr(domain_model, "bill_date"):
            transactional_date = domain_model.bill_date
        elif hasattr(domain_model, "receipt_date"):
            transactional_date = domain_model.receipt_date
            
        if transactional_date:
            dt = DataMapper._parse_to_datetime(transactional_date)
            if dt:
                data["created_at"] = dt
                
        return data
