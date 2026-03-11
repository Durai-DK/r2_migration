import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import os

# -------------------------------------------------------------------------
# Mock the 'js' module and other environment specifics BEFORE importing worker
# -------------------------------------------------------------------------
mock_js = MagicMock()
sys.modules['js'] = mock_js

# Define the attributes that are imported from 'js'
mock_js.console = MagicMock()
mock_js.Response = MagicMock()
mock_js.JSON = MagicMock()
# mock_js.Object = MagicMock() # Not used in worker directly, but maybe in helpers

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
try:
    from worker import on_fetch
except ImportError as e:
    print(f"Failed to import worker: {e}")
    sys.exit(1)

class TestWorkerComplete(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Reset mocks before each test
        mock_js.console.reset_mock()
        mock_js.Response.reset_mock()
        mock_js.JSON.reset_mock()

    @patch('worker.make_response')
    async def test_options_request(self, mock_make_resp):
        """Test OPTIONS request returns 204."""
        request = MagicMock()
        request.url = "https://example.com/api/any"
        request.method = "OPTIONS"
        env = MagicMock()

        await on_fetch(request, env)

        mock_make_resp.assert_called_with(None, status=204, request=request)

    # -------------------------------------------------------------------------
    # CREATE RECORDS Tests (/api/pos-bucket/records [POST])
    # -------------------------------------------------------------------------
    @patch('worker.verify_static_token')
    @patch('worker.handle_post_records')
    @patch('worker.make_response')
    async def test_create_records_success(self, mock_make_resp, mock_handle, mock_verify):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/records"
        request.method = "POST"
        env = MagicMock()
        
        mock_verify.return_value = {"status": "ok"}
        mock_handle.return_value = "success"

        res = await on_fetch(request, env)

        mock_verify.assert_called_once()
        mock_handle.assert_awaited_once()
        self.assertEqual(res, "success")

    @patch('worker.verify_static_token')
    @patch('worker.make_response')
    async def test_create_records_unauthorized(self, mock_make_resp, mock_verify):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/records"
        request.method = "POST"
        env = MagicMock()
        
        mock_verify.return_value = {"status": "unauthorized"}

        await on_fetch(request, env)
        
        # Verify console warning
        mock_js.console.warn.assert_any_call("[CREATE] ❌ Unauthorized access attempt to /api/pos-bucket/records")
        # Verify 401 response
        mock_make_resp.assert_called_with(
            {"error": "Unauthorized", "message": "Missing or invalid static token"},
            status=401,
            headers={"WWW-Authenticate": "Bearer"},
            request=request
        )

    @patch('worker.verify_static_token')
    @patch('worker.make_response')
    async def test_create_records_forbidden(self, mock_make_resp, mock_verify):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/records"
        request.method = "POST"
        env = MagicMock()
        
        mock_verify.return_value = {"status": "forbidden"}

        await on_fetch(request, env)
        
        mock_js.console.warn.assert_any_call("[CREATE] ❌ Forbidden access attempt to /api/pos-bucket/records")
        mock_make_resp.assert_called_with(
            {"error": "Forbidden", "message": "Static token validation failed"},
            status=403,
            request=request
        )

    # -------------------------------------------------------------------------
    # FETCH RECORDS Tests (/api/pos-bucket/fetch-records [GET])
    # -------------------------------------------------------------------------
    @patch('worker.verify_request_token')
    @patch('worker.handle_get_records')
    @patch('worker.make_response')
    async def test_fetch_records_success(self, mock_make_resp, mock_handle_get, mock_verify):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/fetch-records"
        request.method = "GET"
        env = MagicMock()
        
        mock_verify.return_value = {"status": "ok"}
        mock_handle_get.return_value = "fetch_success"

        res = await on_fetch(request, env)

        mock_verify.assert_called_once()
        mock_handle_get.assert_awaited_once()
        self.assertEqual(res, "fetch_success")

    @patch('worker.verify_request_token')
    @patch('worker.make_response')
    async def test_fetch_records_unauthorized(self, mock_make_resp, mock_verify):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/fetch-records"
        request.method = "GET"
        env = MagicMock()
        
        mock_verify.return_value = {"status": "unauthorized"}

        await on_fetch(request, env)

        mock_js.console.warn.assert_any_call("[FETCH] ❌ Unauthorized access attempt to /api/pos-bucket/fetch-records")
        mock_make_resp.assert_called_with(
            {"error": "Unauthorized", "message": "Missing or invalid access token"},
            status=401,
            headers={"WWW-Authenticate": "Bearer"},
            request=request
        )

    @patch('worker.verify_request_token')
    @patch('worker.make_response')
    async def test_fetch_records_forbidden(self, mock_make_resp, mock_verify):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/fetch-records"
        request.method = "GET"
        env = MagicMock()
        
        mock_verify.return_value = {"status": "forbidden"}

        await on_fetch(request, env)

        mock_js.console.warn.assert_any_call("[FETCH] ❌ Forbidden access attempt to /api/pos-bucket/fetch-records")
        mock_make_resp.assert_called_with(
            {"error": "Forbidden", "message": "Token claims validation failed"},
            status=403,
            request=request
        )

    # -------------------------------------------------------------------------
    # GET TOKEN Tests (/api/pos-bucket/get-token [GET])
    # -------------------------------------------------------------------------
    @patch('worker.generate_token')
    @patch('worker.make_response')
    async def test_get_token_success(self, mock_make_resp, mock_gen_token):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/get-token?token_id=correct_token"
        request.method = "GET"
        
        env = MagicMock()
        env.TOKEN_ID = "correct_token"
        
        mock_gen_token.return_value = "token_response"

        res = await on_fetch(request, env)
        
        # Verify call to generate_token
        mock_gen_token.assert_awaited_once_with(request, env, "correct_token")
        self.assertEqual(res, "token_response")

    @patch('worker.make_response')
    async def test_get_token_missing_id(self, mock_make_resp):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/get-token" # No query params
        request.method = "GET"
        env = MagicMock()
        env.TOKEN_ID = "correct_token"

        await on_fetch(request, env)

        mock_js.console.warn.assert_any_call("[TOKEN] ❌ Unauthorized access attempt to /api/pos-bucket/get-token")
        mock_make_resp.assert_called_with(
            {"error": "missing_token_id", "message": "The required query parameter 'token_id' is missing."},
            status=400,
            request=request
        )

    @patch('worker.make_response')
    async def test_get_token_invalid_id(self, mock_make_resp):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/get-token?token_id=wrong_token"
        request.method = "GET"
        env = MagicMock()
        env.TOKEN_ID = "correct_token"

        await on_fetch(request, env)

        mock_js.console.warn.assert_any_call("[TOKEN] ❌ Unauthorized access attempt to /api/pos-bucket/get-token")
        mock_make_resp.assert_called_with(
            {"error": "invalid_token_id", "message": "The provided token_id is invalid."},
            status=401,
            request=request
        )

    # -------------------------------------------------------------------------
    # NOT FOUND Test
    # -------------------------------------------------------------------------
    @patch('worker.make_response')
    async def test_not_found(self, mock_make_resp):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/unknown-route"
        request.method = "GET"
        env = MagicMock()

        await on_fetch(request, env)

        mock_make_resp.assert_called_with("Result Not Found", status=404, request=request)

    # -------------------------------------------------------------------------
    # EXCEPTION Test
    # -------------------------------------------------------------------------
    @patch('worker.make_response')
    async def test_exception_handling(self, mock_make_resp):
        request = MagicMock()
        request.url = "https://example.com/api/pos-bucket/records"
        # Trigger an exception during parsing (or any early stage) by making url invalid or similar
        # Easier: patch urlparse inside worker to raise Exception
        
        env = MagicMock()
        
        with patch('worker.urlparse', side_effect=Exception("Explosion")):
            await on_fetch(request, env)
        
        mock_js.console.error.assert_called_with("[FETCH] ❌ Fatal error | Explosion")
        mock_make_resp.assert_called_with(
            {"error": "Internal Server Error", "details": "Explosion"},
            status=500,
            request=request
        )

if __name__ == '__main__':
    unittest.main()
