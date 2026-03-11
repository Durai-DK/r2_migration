from typing import List, Dict, Any
from logs.log_settings import success_log, error_log

class QueueClient:
    def __init__(self, queue_name: str = "pos_processing_queue"):
        self.queue_name = queue_name
        # In a real external Producer scenario (outside Cloudflare Workers),
        # this might store credentials or an HTTP endpoint to the Worker.
        # For now, we simulate the interface.

    def send(self, messages: List[Dict[str, Any]]):
        """
        Sends a batch of messages to the Cloudflare Queue.
        """
        try:
            # SIMULATION: In a real deployment, this would be an HTTP POST
            # to a Cloudflare Worker that has a binding to the Queue.
            # Example: requests.post(F"{WORKER_URL}/enqueue", json=messages)
            
            # logging for verification
            for msg in messages:
                pri_id = msg.get("pri_id", "unknown")
                success_log.info(f"🚀 [QueueClient] Enqueued message for PRI_ID: {pri_id} to '{self.queue_name}'")
            
            return True
            
        except Exception as e:
            error_log.error(f"❌ [QueueClient] Failed to send to queue: {e}")
            raise

# Singleton instance
queue_client = QueueClient()
