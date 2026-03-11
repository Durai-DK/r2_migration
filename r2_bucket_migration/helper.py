import json
from decimal import Decimal
from datetime import datetime, date
from typing import Dict, Any, List, Tuple
from .creds import r2_client, fetch_mysql
from .log_settings import error_log, success_log


def log_failed_task(stage: str, key: str, imei: str, error: Exception):
    error_log.error(
        f"❌ FAILED | stage={stage} | imei={imei} | key={key} | error={str(error)}"
    )


def safe_float(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return 0.0


def make_json_data(record: Dict[str, Any]) -> Dict[str, Any]:
    clean = {}
    for k, v in record.items():
        if isinstance(v, (datetime, date)):
            clean[k] = v.isoformat()
        elif isinstance(v, Decimal):
            clean[k] = float(v)
        elif isinstance(v, bytes):
            clean[k] = v.decode("utf-8", errors="ignore")
        else:
            clean[k] = v
    return clean


def store_json_data(bucket, data, key, metadata=None):
    if not key or data is None:
        raise ValueError("Invalid summary data or key")

    try:
        r2 = r2_client()

        body = json.dumps(data, indent=2, ensure_ascii=False)

        r2.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
            Metadata=metadata or {}
        )
        # print(f"file uploaded: {key}")
        return True

    except Exception as e:
        log_failed_task(stage="UPLOAD", key=key, imei=(metadata or {}).get("imei", "NA"), error=e)
        return False
