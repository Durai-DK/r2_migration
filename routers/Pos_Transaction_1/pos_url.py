from fastapi import APIRouter
from pos_creation import *
from get_pri_id import fetch_and_update_count

router = APIRouter(prefix="/api/pos-bucket", tags=["POS Bucket Data Management"])

mysql_db_name = "Transaction"

mob_bucket = "pos-transaction-test"
imei_bucket = "pos-transaction-imei-test"
mob_prefix = "mobile"
imei_prefix = "imei"


@router.post("/records/")
def create_records(invoice: Dict[str, Any]):
    # Define counter key
    count_key = "pri_id_counter.json"
    
    try:
        # 1. Fetch and Increment PRI_ID
        pri_id = fetch_and_update_count(pos_bucket, count_key)

        # 2. Build the Payload for the Queue
        payload = {
            "invoice": invoice,
            "pri_id": pri_id,
            "bucket_name": pos_bucket,
        }
        
        # 3. ENQUEUE the Payload
        # This sends the message to the Cloudflare Queue. It's a fast operation.
        from core.queue_client import queue_client
        
        queue_client.send([payload])

        results = {
            "status": "success",
            "message": "POS record accepted and queued for processing",
            "pri_id": pri_id,
        }
        return JSONResponse(status_code=200, content=results)

    except Exception as e:
        error_log.exception("❌ Failed to queue POS record")
        return error_response(500, "POS intake failed", str(e))
