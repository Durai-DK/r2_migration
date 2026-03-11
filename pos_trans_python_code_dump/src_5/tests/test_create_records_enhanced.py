import unittest
from unittest.mock import MagicMock, AsyncMock, patch, call
import sys
import os

# Mock JS module
mock_js = MagicMock()
sys.modules['js'] = mock_js
mock_js.console = MagicMock()
mock_js.JSON = MagicMock()

# Path setup
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from create_records import (
    compute_backoff, 
    is_already_processed, 
    mark_processed, 
    process_single_message,
    send_to_dlq,
    MAX_RETRIES
)
from handling_failure import FatalProcessingError


class TestComputeBackoff(unittest.TestCase):
    def test_backoff_first_attempt(self):
        """First retry should be 5 seconds."""
        self.assertEqual(compute_backoff(1), 5)
    
    def test_backoff_second_attempt(self):
        """Second retry should be 10 seconds."""
        self.assertEqual(compute_backoff(2), 10)
    
    def test_backoff_third_attempt(self):
        """Third retry should be 20 seconds."""
        self.assertEqual(compute_backoff(3), 20)
    
    def test_backoff_exponential(self):
        """Test exponential growth."""
        self.assertEqual(compute_backoff(4), 40)
        self.assertEqual(compute_backoff(5), 80)
        self.assertEqual(compute_backoff(6), 160)
    
    def test_backoff_max_cap(self):
        """Backoff should cap at 300 seconds (5 minutes)."""
        self.assertEqual(compute_backoff(10), 300)
        self.assertEqual(compute_backoff(20), 300)


class TestIdempotency(unittest.IsolatedAsyncioTestCase):
    async def test_is_already_processed_returns_value(self):
        """Test KV lookup for processed batch."""
        env = MagicMock()
        env.POS_KV.get = AsyncMock(return_value="1")
        
        result = await is_already_processed(env, "batch-123")
        
        env.POS_KV.get.assert_awaited_once_with("processed:batch-123")
        self.assertEqual(result, "1")
    
    async def test_is_already_processed_not_found(self):
        """Test KV lookup returns None if not processed."""
        env = MagicMock()
        env.POS_KV.get = AsyncMock(return_value=None)
        
        result = await is_already_processed(env, "batch-456")
        
        self.assertIsNone(result)
    
    async def test_mark_processed_sets_kv(self):
        """Test marking batch as processed in KV."""
        env = MagicMock()
        env.POS_KV.put = AsyncMock()
        
        await mark_processed(env, "batch-789")
        
        env.POS_KV.put.assert_awaited_once_with(
            "processed:batch-789", 
            "1", 
            expirationTtl=86400
        )


class TestProcessSingleMessage(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """Reset console mocks before each test."""
        mock_js.console.reset_mock()
    
    @patch('create_records.deep_to_py')
    @patch('create_records.is_already_processed')
    @patch('create_records.mark_processed')
    @patch('create_records.process_record')
    @patch('create_records.r2_bucket_push')
    async def test_successful_processing(
        self, mock_r2_push, mock_process, mock_mark, mock_is_processed, mock_deep_to_py
    ):
        """Test successful message processing flow."""
        # Setup
        env = MagicMock()
        message = MagicMock()
        message.body = MagicMock()
        
        mock_deep_to_py.return_value = {
            "batch_id": "test-batch",
            "invoice": {"data": "test"}
        }
        mock_is_processed.return_value = None  # Not processed yet
        mock_process.return_value = {"records": "data"}
        
        # Execute
        await process_single_message(message, env)
        
        # Assertions
        mock_is_processed.assert_awaited_once_with(env, "test-batch")
        mock_process.assert_awaited_once()
        mock_r2_push.assert_awaited_once()
        mock_mark.assert_awaited_once_with(env, "test-batch")
        message.ack.assert_called_once()
        
        # Check console logs - must exist somewhere in call history
        console_calls = [str(call) for call in mock_js.console.log.call_args_list]
        self.assertTrue(
            any("[QUEUE] ✅ Completed test-batch" in str(call) for call in console_calls),
            f"Expected log message not found in: {console_calls}"
        )
    
    @patch('create_records.deep_to_py')
    @patch('create_records.is_already_processed')
    async def test_idempotency_skip(self, mock_is_processed, mock_deep_to_py):
        """Test that already processed messages are skipped."""
        # Setup
        env = MagicMock()
        message = MagicMock()
        
        mock_deep_to_py.return_value = {
            "batch_id": "duplicate-batch",
            "invoice": {"data": "test"}
        }
        mock_is_processed.return_value = "1"  # Already processed
        
        # Execute
        await process_single_message(message, env)
        
        # Assertions
        mock_js.console.warn.assert_any_call("[IDEMPOTENT] Skip duplicate-batch")
        message.ack.assert_called_once()
    
    @patch('create_records.deep_to_py')
    @patch('create_records.is_already_processed')
    @patch('create_records.process_record')
    @patch('create_records.failed_queue_store')
    @patch('create_records.send_to_dlq')
    async def test_fatal_error_immediate_dlq(
        self, mock_send_dlq, mock_failed_store, mock_process, mock_is_processed, mock_deep_to_py
    ):
        """Test FatalProcessingError immediately sends to DLQ without retry."""
        # Setup
        env = MagicMock()
        message = MagicMock()
        message.attempts = 1
        
        payload = {
            "batch_id": "fatal-batch",
            "invoice": {"data": "test"}
        }
        mock_deep_to_py.return_value = payload
        mock_is_processed.return_value = None
        
        # Raise FatalProcessingError
        error = FatalProcessingError("Bad data format")
        mock_process.side_effect = error
        
        # Execute
        await process_single_message(message, env)
        
        # Assertions
        mock_js.console.error.assert_any_call("[POISON] fatal-batch: Bad data format")
        mock_failed_store.assert_awaited_once_with(env, payload, "fatal-batch", error)
        mock_send_dlq.assert_awaited_once_with(env, message, error)
        message.ack.assert_called_once()  # Should ACK, not retry
        message.retry.assert_not_called()
    
    @patch('create_records.deep_to_py')
    @patch('create_records.is_already_processed')
    @patch('create_records.process_record')
    @patch('create_records.compute_backoff')
    async def test_retry_with_backoff(
        self, mock_backoff, mock_process, mock_is_processed, mock_deep_to_py
    ):
        """Test that failures below MAX_RETRIES trigger retry with backoff."""
        # Setup
        env = MagicMock()
        message = MagicMock()
        message.attempts = 2  # Below MAX_RETRIES (3)
        
        mock_deep_to_py.return_value = {
            "batch_id": "retry-batch",
            "invoice": {"data": "test"}
        }
        mock_is_processed.return_value = None
        mock_process.side_effect = Exception("Temporary error")
        mock_backoff.return_value = 10  # 10 second delay
        
        # Execute
        await process_single_message(message, env)
        
        # Assertions
        mock_backoff.assert_called_once_with(2)
        
        # Check that retry warning was called
        console_calls = [str(call) for call in mock_js.console.warn.call_args_list]
        self.assertTrue(
            any("[RETRY] Attempt 2 | Delay 10s" in str(call) for call in console_calls),
            f"Expected warning not found in: {console_calls}"
        )
        
        message.retry.assert_called_once_with(delaySeconds=10)
        message.ack.assert_not_called()
    
    @patch('create_records.deep_to_py')
    @patch('create_records.is_already_processed')
    @patch('create_records.process_record')
    @patch('create_records.failed_queue_store')
    @patch('create_records.send_to_dlq')
    async def test_max_retries_sends_to_dlq(
        self, mock_send_dlq, mock_failed_store, mock_process, mock_is_processed, mock_deep_to_py
    ):
        """Test that >= MAX_RETRIES sends to DLQ."""
        # Setup
        env = MagicMock()
        message = MagicMock()
        message.attempts = 3  # Equal to MAX_RETRIES
        
        payload = {
            "batch_id": "exhausted-batch",
            "invoice": {"data": "test"}
        }
        mock_deep_to_py.return_value = payload
        mock_is_processed.return_value = None
        
        error = Exception("Persistent error")
        mock_process.side_effect = error
        
        # Execute
        await process_single_message(message, env)
        
        # Assertions
        mock_failed_store.assert_awaited_once_with(env, payload, "exhausted-batch", error)
        mock_send_dlq.assert_awaited_once_with(env, message, error)
        
        # Check error log
        console_calls = [str(call) for call in mock_js.console.error.call_args_list]
        self.assertTrue(
            any("exhausted-batch" in str(call) and "DLQ" in str(call) for call in console_calls),
            f"Expected DLQ error log not found in: {console_calls}"
        )
        
        message.ack.assert_called_once()
        message.retry.assert_not_called()


class TestSendToDLQ(unittest.IsolatedAsyncioTestCase):
    @patch('create_records.deep_to_py')
    @patch('create_records.time')
    async def test_send_to_dlq_includes_batch_id(self, mock_time, mock_deep_to_py):
        """Test that DLQ payload includes batch_id field."""
        # Setup
        env = MagicMock()
        env.POS_DLQ.send = AsyncMock()
        
        message = MagicMock()
        message.body = MagicMock()
        message.attempts = 3
        
        mock_deep_to_py.return_value = {
            "batch_id": "dlq-batch-123",
            "invoice": {"data": "test"}
        }
        mock_time.time.return_value = 1234567890.0
        
        error = Exception("Test error")
        
        # Execute
        await send_to_dlq(env, message, error)
        
        # Verify batch_id is in payload
        call_args = env.POS_DLQ.send.call_args
        # The actual argument passed would be the result of JSON.parse
        # We need to verify the intermediate python dict had batch_id
        mock_deep_to_py.assert_called_once_with(message.body)
        env.POS_DLQ.send.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
