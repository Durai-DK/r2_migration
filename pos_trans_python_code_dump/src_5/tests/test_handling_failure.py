import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import os

mock_js = MagicMock()
sys.modules['js'] = mock_js

# Path setup
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from handling_failure import failed_queue_store

class TestHandlingFailure(unittest.IsolatedAsyncioTestCase):
    @patch('handling_failure.get_date_paths')
    @patch('handling_failure.datetime')
    async def test_failed_queue_store_key_type(self, mock_datetime, mock_get_dates):
        # Setup
        env = MagicMock()
        env.IMEI_BUCKET_PROD.put = AsyncMock()
        payload = {"foo": "bar"}
        batch_id = "test-batch-123"
        error = Exception("test error")

        mock_get_dates.return_value = ("2025", "01", "01")
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"

        # Execute
        await failed_queue_store(env, payload, batch_id, error)

        # Assertions
        # Check what arguments `env.IMEI_BUCKET_PROD.put` was called with
        args, _ = env.IMEI_BUCKET_PROD.put.call_args
        key_arg = args[0]
        
        # This is the CRITICAL check: is it a string or a tuple?
        self.assertIsInstance(key_arg, str, f"Key should be a string, but got {type(key_arg)}")
        self.assertEqual(key_arg, "Failed/Queues/2025/01/01/test-batch-123.json")

if __name__ == '__main__':
    unittest.main()
