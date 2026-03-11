import json
import time
from core.creds import r2_client


def collect_and_store_mobile_numbers(bucket: str):
    start = time.time()

    r2 = r2_client()

    mobile_set = set()
    continuation_token = None

    try:
        while True:
            kwargs = {
                "Bucket": bucket,
                "Prefix": "mobile/",
            }

            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            response = r2.list_objects_v2(**kwargs)
            contents = response.get("Contents", [])

            for obj in contents:
                key = obj["Key"]      # example: mobile/9876543210/id/abc.json
                parts = key.split("/")

                # Expected format: ["mobile", "9876543210", ...]
                if len(parts) > 1 and parts[1].isdigit():
                    mobile_set.add(parts[1])

            if response.get("IsTruncated"):
                continuation_token = response.get("NextContinuationToken")
            else:
                break

        mobile_list = sorted(mobile_set)

        payload = {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "mobile_count": len(mobile_list),
            "mobile_numbers": mobile_list,
        }

        # ✅ Push to R2
        r2.put_object(
            Bucket=bucket,
            Key="mobile_list/mobile_numbers.json",
            Body=json.dumps(payload, indent=2),
            ContentType="application/json",
        )

        return {
            "status": "success",
            "mobile_count": len(mobile_list),
            "duration": f"{round(time.time() - start, 2)}s",
            "output_key": "mobile_list/mobile_numbers.json",
        }

    except Exception as e:
        raise RuntimeError(f"Failed to collect mobile numbers: {e}")


collect_and_store_mobile_numbers("pos-transaction")
