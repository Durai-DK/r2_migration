import requests, os
from typing import List, Dict, Any
from logs.log_settings import success_log, error_log

class QueueClient:
    def __init__(self):
        self.worker_url = os.getenv("CLOUDFLARE_WORKER_URL")
        self.auth_token = os.getenv("CLOUDFLARE_WORKER_TOKEN")

    def send(self, messages):
        if not self.worker_url:
            raise RuntimeError("CLOUDFLARE_WORKER_URL not set")

        headers = {
            "Content-Type": "application/json",
            "X-Auth-Token": self.auth_token
        }

        r = requests.post(
            f"{self.worker_url}/enqueue",
            json=messages,
            headers=headers,
            timeout=5
        )

        if r.status_code not in (200, 201, 202):
            raise RuntimeError(f"Queue failed: {r.text}")

        return True


# Singleton instance
queue_client = QueueClient()
