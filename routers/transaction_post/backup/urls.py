from .key_mapping import *
from fastapi import APIRouter
from .get_pri_id import fetch_and_update_count


router = APIRouter(prefix="/api/pos", tags=["Post bucket"])


count_key = f"count/old_pri_id.json"

@router.post("/create")
def create_record(invoice: dict):

    try:
        pri_id = fetch_and_update_count(pos_bucket, count_key)

        push_to_bucket(invoice, pri_id)

        results = {
            "status": "success",
            "message": "POS record created",
            "records": pri_id,
        }
        return JSONResponse(status_code=200, content=results)

    except Exception as e:
        error_log.exception("❌ Failed to create POS record")
        return error_response(500, "POS creation failed", str(e))
