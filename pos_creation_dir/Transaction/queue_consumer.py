from typing import List, Dict, Any
from logs.log_settings import success_log, error_log
from routers.Pos_Transaction.pos_creation import push_to_bucket

# --- CLOUDFLARE WORKER CONSUMER (Simulated/Ready for Porting) ---
# This code structure mirrors how a Python-based queue consumer would work,
# or serves as the logic core for a server-side worker that polls/receives tasks.

def handle_queue(batch: Any):
    # 'batch' is expected to be an object with a 'messages' attribute,
    # consistent with Cloudflare Queues interface.
    
    success_log.info(f"📥 Consumer received batch of {len(batch.messages)} messages")

    for message in batch.messages:
        try:
            # message.body is the payload we sent
            payload = message.body
            
            invoice = payload.get("invoice")
            pri_id = payload.get("pri_id")
            
            if not invoice or not pri_id:
                error_log.warning(f"⚠️ Invalid payload structure: {payload}")
                continue

            success_log.info(f"⚙️ Processing PRI_ID: {pri_id} from Queue")

            # Call the existing heavy processing logic
            push_to_bucket(invoice, str(pri_id))

            # If no exception, the message is effectively "ACKed"
            # In Cloudflare, if you don't throw, it's a success.
            success_log.info(f"✅ Successfully processed PRI_ID: {pri_id}")

        except Exception as e:
            # If processing fails (e.g., CRM API timeout, R2 failure),
            # mark the message for retry.
            error_log.exception(f"❌ Processing failed for PRI_ID {pri_id}. Requesting Retry...")
            
            if hasattr(message, 'retry'):
                message.retry()
            else:
                # Fallback for simulated environment
                error_log.error("Message retry requested but not supported in this environment.")
