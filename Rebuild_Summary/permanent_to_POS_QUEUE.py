import json, subprocess
from datetime import datetime, timezone, timedelta


batch_id = "352d30f9-6864-49dc-b7bc-d85e33845554"
dump_key = "queue/main/permanent_failure/2026/03/05/352d30f9-6864-49dc-b7bc-d85e33845554.json"
QUEUE_NAME = "pos-transaction-python-queue"


def send_queue_message():
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    if not dump_key:
        raise ValueError("dump_key is required")

    payload = {
        "batch_id": batch_id,
        "replay_count": 0,
        "dump_key": dump_key,
        "source": "manual-python",
        "timestamp": now
    }

    payload_json = json.dumps(payload)

    cmd = [
        "wrangler",
        "queues",
        "send",
        QUEUE_NAME,
        payload_json
    ]

    subprocess.run(cmd, check=True)

    print("✅ Queue message sent")
    print(json.dumps(payload, indent=2))


# Example usage
send_queue_message()
