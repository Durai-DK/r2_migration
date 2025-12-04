from fastapi import APIRouter
from .transaction_post_api import R2Manager

router = APIRouter(prefix="/api/pos", tags=["Post bucket"])

pos_bucket = "pos-transaction"
pos_imei_bucket = "pos-transaction-imei"

count_key = f"count/old_pri_id.json"

mobile_prefix = "mobile"
imei_prefix = "imei_numbers"

@router.post("/create")
def create_record(request: dict):

    r2 = R2Manager()

    # 1. Read old pri_id
    old_count, etag = r2.get_count(pos_bucket, count_key)

    # # 2. Create your main response with this pri_id
    # response_body = {
    #     "pri_id": old_count + 1, "data": request, "timestamp": r2.ts()
    # }
    #
    # # 3. Store main record in another bucket
    # r2.write_json(
    #     pos_bucket, f"{old_count+1}.json", response_body
    # )

    # 4. Update count bucket (atomic)
    r2.increment_count(pos_bucket, count_key, old_count, etag)

    return {
        "status": "success", "pri_id": old_count + 1
    }
