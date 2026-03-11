import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import os
import io

# -------------------------------------------------------------------------
# Mock the 'js' module and other environment specifics BEFORE importing worker
# -------------------------------------------------------------------------
mock_js = MagicMock()
sys.modules['js'] = mock_js

# Define the attributes that are imported from 'js'
mock_js.console = MagicMock()
mock_js.Response = MagicMock()
mock_js.JSON = MagicMock()
mock_js.Object = MagicMock()
mock_js.Date = MagicMock()
mock_js.fetch = MagicMock()

# -------------------------------------------------------------------------
# Setup system path to look for src modules
# -------------------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
src_dir = os.path.join(parent_dir, 'src')
sys.path.insert(0, src_dir)

# -------------------------------------------------------------------------
# Import the module under test
# -------------------------------------------------------------------------
# Note: importing worker will trigger imports of other modules.
# Since we mocked 'js', they should load fine if they don't have other side effects.
try:
    from worker import on_fetch
except ImportError as e:
    print(f"Failed to import worker: {e}")
    sys.exit(1)

class TestWorkerCreateRecords(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Reset mocks before each test
        mock_js.console.reset_mock()
        mock_js.Response.reset_mock()
        mock_js.JSON.reset_mock()

    @patch('worker.verify_static_token')
    @patch('worker.handle_post_records')
    @patch('worker.make_response')
    async def test_create_records_success(self, mock_make_resp, mock_handle, mock_verify):
        """
        Test that valid static token leads to handle_post_records being called.
        """
        # Setup Request
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/records"
        request.method = "POST"
        
        # Setup Env
        env = MagicMock()

        # Setup token verification success
        mock_verify.return_value = {"status": "ok"}
        
        # Setup handling success
        mock_handle.return_value = "success_response"

        # Execute
        response = await on_fetch(request, env)

        # Assertions
        mock_verify.assert_called_once_with(request, env)
        mock_handle.assert_awaited_once_with(request, env)
        self.assertEqual(response, "success_response")
        
        # Ensure no error/warning logs for success path
        # (Assuming console.warn is only called on failure)
        mock_js.console.warn.assert_not_called()

    @patch('worker.verify_static_token')
    @patch('worker.handle_post_records')
    @patch('worker.make_response')
    async def test_create_records_unauthorized(self, mock_make_resp, mock_handle, mock_verify):
        """
        Test that 'unauthorized' status from verify_static_token returns 401.
        """
        # Setup Request
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/records"
        request.method = "POST"
        
        # Setup Env
        env = MagicMock()

        # Setup token verification failure
        mock_verify.return_value = {"status": "unauthorized"}
        
        # Setup make_response to return a specific mock object
        mock_response_obj = MagicMock()
        mock_make_resp.return_value = mock_response_obj

        # Execute
        response = await on_fetch(request, env)

        # Assertions
        mock_verify.assert_called_once_with(request, env)
        mock_handle.assert_not_called()
        
        # Verify Warning Log
        mock_js.console.warn.assert_any_call("[CREATE] ❌ Unauthorized access attempt to /api/pos-bucket/records")
        
        # Verify Response Construction
        mock_make_resp.assert_called_with(
            {"error": "Unauthorized", "message": "Missing or invalid static token"},
            status=401,
            headers={"WWW-Authenticate": "Bearer"},
            request=request
        )
        self.assertEqual(response, mock_response_obj)

    @patch('worker.verify_static_token')
    @patch('worker.handle_post_records')
    @patch('worker.make_response')
    async def test_create_records_forbidden(self, mock_make_resp, mock_handle, mock_verify):
        """
        Test that 'forbidden' status from verify_static_token returns 403.
        """
        # Setup Request
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/records"
        request.method = "POST"
        
        # Setup Env
        env = MagicMock()

        # Setup token verification forbidden
        mock_verify.return_value = {"status": "forbidden"}
        
        mock_response_obj = MagicMock()
        mock_make_resp.return_value = mock_response_obj

        # Execute
        response = await on_fetch(request, env)

        # Assertions
        mock_verify.assert_called_once_with(request, env)
        mock_handle.assert_not_called()
        
        # Verify Warning Log
        mock_js.console.warn.assert_any_call("[CREATE] ❌ Forbidden access attempt to /api/pos-bucket/records")
        
        # Verify Response Construction
        mock_make_resp.assert_called_with(
            {"error": "Forbidden", "message": "Static token validation failed"},
            status=403,
            request=request
        )
        self.assertEqual(response, mock_response_obj)
    
    @patch('worker.verify_static_token')
    @patch('worker.handle_post_records')
    @patch('worker.make_response')
    async def test_create_records_implicit_fallthrough_bug(self, mock_make_resp, mock_handle, mock_verify):
        """
        Demonstrates a potential bug: if status is neither unauthorized nor forbidden,
        it falls through to success even if status is not 'ok'.
        """
        # Setup Request
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/records"
        request.method = "POST"
        env = MagicMock()

        # Setup token verification sending an unknown status
        mock_verify.return_value = {"status": "unknown_error"}
        
        mock_handle.return_value = "success_response"

        # Execute
        response = await on_fetch(request, env)

        # Assertions - FIX VERIFIED: It SHOULD NOT call handle
        mock_verify.assert_called_once_with(request, env)
        mock_handle.assert_not_called()
        
        # Verify Fallback Error Response
        mock_make_resp.assert_called_with(
            {"error": "Forbidden", "message": "Authentication failed"},
            status=403,
            request=request
        )

if __name__ == '__main__':
    unittest.main()
