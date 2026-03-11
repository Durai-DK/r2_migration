
import sys
import os
import json
import logging

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from routers.Pos_Transaction.queue_consumer import handle_queue
from logs.log_settings import success_log, error_log

# Mock classes to simulate Cloudflare Queue objects
class MockMessage:
    def __init__(self, body):
        self.body = body

    def retry(self):
        print(f"🔄 [MOCK] Message retry requested for payload: {self.body}")

class MockBatch:
    def __init__(self, messages):
        self.messages = messages

def test_queue_flow():
    print("🚀 Starting Local Queue Flow Test...")
    
    # 1. Define Sample Invoice
    sample_invoice = {
        "bill_transaction_no__c": "INV-TEST-QUEUE-001",
        "customer_mobile": "9998887776",
        "customer_name": "Queue Tester",
        "item_remarks1": "IMEI-QUEUE-1, IMEI-QUEUE-2",
        "item_remarks2": "EMP01-TestUser",
        "Bill_Grant_Total__c": 5000,
        "store_code": "PM01"
    }
    
    pri_id = "1001"
    
    # 2. Create Payload (as Producer would)
    payload = {
        "invoice": sample_invoice,
        "pri_id": pri_id,
        "bucket_name": "pos-transaction-test"
    }
    
    # 3. Create Mock Batch
    message = MockMessage(payload)
    batch = MockBatch([message])
    
    # 4. Invoke Consumer
    try:
        handle_queue(batch)
        print("✅ Test Passed: handle_queue executed without error.")
    except Exception as e:
        print(f"❌ Test Failed: {e}")
        # error_log.exception("Test failure")

if __name__ == "__main__":
    test_queue_flow()
