import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import os
import json

# -------------------------------------------------------------------------
# Mock the 'js' module
# -------------------------------------------------------------------------
mock_js = MagicMock()
sys.modules['js'] = mock_js
mock_js.JSON = MagicMock()
mock_js.JSON.parse = MagicMock(side_effect=lambda x: f"JS_OBJECT_FROM_{x}") # specialized mock to track calls

# -------------------------------------------------------------------------
# Path setup
# -------------------------------------------------------------------------
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Import AFTER mocking
from create_records import send_to_dlq

class TestCreateRecordsDLQ(unittest.IsolatedAsyncioTestCase):
    @patch('create_records.deep_to_py')
    @patch('create_records.time')
    async def test_send_to_dlq_logic(self, mock_time, mock_deep_to_py):
        # Setup
        env = MagicMock()
        env.POS_DLQ.send = AsyncMock()
        
        message = MagicMock()
        message.body = "mock_js_proxy_body"
        message.attempts = 4
        
        error = ValueError("Something went wrong")
        
        # Mocks behavior
        mock_deep_to_py.return_value = {"foo": "bar"} # Outcome of converting proxy to python
        mock_time.time.return_value = 1234567890.0

        # Execute
        await send_to_dlq(env, message, error)

        # Assertions
        
        # 1. Check deep_to_py called on message.body
        mock_deep_to_py.assert_called_once_with("mock_js_proxy_body")
        
        # 2. Check JSON.parse(json.dumps(...)) pattern
        # expected python dict before dumping
        expected_py_payload = {
            "original_message": {"foo": "bar"},
            "error": "Something went wrong",
            "attempts": 4,
            "ts": 1234567890.0
        }
        expected_json_str = json.dumps(expected_py_payload)
        
        mock_js.JSON.parse.assert_called_once_with(expected_json_str)
        
        # 3. Check env.POS_DLQ.send called with the RESULT of JSON.parse
        env.POS_DLQ.send.assert_awaited_once_with(f"JS_OBJECT_FROM_{expected_json_str}")

if __name__ == '__main__':
    unittest.main()
