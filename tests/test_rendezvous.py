import unittest
from unittest.mock import patch, AsyncMock
import asyncio

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from client_py.rendezvous import HttpRendezvous, SqsRendezvous, AmpCacheRendezvous
from curl_cffi.requests import AsyncSession as CurlAsyncSession, Response, RequestsError

# Suppress logging during tests unless specifically needed
import logging
logging.basicConfig(level=logging.CRITICAL)


class TestHttpRendezvousCurlCffi(unittest.TestCase):

    async def _async_test_exchange_offer_answer_success(self, mock_curl_post):
        # Mock the curl_cffi response
        mock_response = AsyncMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json = unittest.mock.Mock(return_value={ # json is a method, not an attribute
            "answer": "dummy_answer_sdp",
            "proxy_id": "proxy123"
        })
        mock_response.raise_for_status = unittest.mock.Mock() # Ensure it doesn't raise
        mock_curl_post.return_value = mock_response

        rendezvous = HttpRendezvous(broker_url="https://fakebroker.com", impersonate_profile="chrome120")
        offer_sdp = "dummy_offer_sdp"
        client_nat_type = "unknown"

        answer, proxy_id = await rendezvous.exchange_offer_answer(offer_sdp, client_nat_type)

        self.assertEqual(answer, "dummy_answer_sdp")
        self.assertEqual(proxy_id, "proxy123")
        mock_curl_post.assert_called_once()
        # Check impersonate profile was passed
        self.assertEqual(mock_curl_post.call_args.kwargs.get("impersonate"), "chrome120")

    @patch('curl_cffi.requests.AsyncSession.post', new_callable=AsyncMock)
    def test_exchange_offer_answer_success(self, mock_curl_post):
        asyncio.run(self._async_test_exchange_offer_answer_success(mock_curl_post))

    async def _async_test_exchange_offer_answer_broker_error(self, mock_curl_post):
        mock_response = AsyncMock(spec=Response)
        mock_response.status_code = 200 # Broker can return 200 OK with an error field
        mock_response.json = unittest.mock.Mock(return_value={"error": "No proxies available"})
        mock_response.raise_for_status = unittest.mock.Mock()
        mock_curl_post.return_value = mock_response

        rendezvous = HttpRendezvous(broker_url="https://fakebroker.com")
        with self.assertRaisesRegex(Exception, "Broker error: No proxies available"):
            await rendezvous.exchange_offer_answer("dummy_offer", "unknown")

    @patch('curl_cffi.requests.AsyncSession.post', new_callable=AsyncMock)
    def test_exchange_offer_answer_broker_error(self, mock_curl_post):
        asyncio.run(self._async_test_exchange_offer_answer_broker_error(mock_curl_post))

    async def _async_test_exchange_offer_answer_http_error(self, mock_curl_post):
        mock_response = AsyncMock(spec=Response)
        mock_response.status_code = 500
        # Mock the .text property using a PropertyMock
        type(mock_response).text = unittest.mock.PropertyMock(return_value="Internal Server Error Text")

        # Configure raise_for_status on this mock_response to raise the RequestsError
        # Manually create the error instance and attach the response, as RequestsError constructor
        # might not take request/response kwargs directly.
        error_instance = RequestsError(f"HTTP {mock_response.status_code} - Internal Server Error Text") # Message for the error
        error_instance.response = mock_response # Attach the mock response to the error instance

        mock_response.raise_for_status = unittest.mock.Mock(side_effect=error_instance)
        mock_curl_post.return_value = mock_response # session.post returns this response object

        rendezvous = HttpRendezvous(broker_url="https://fakebroker.com")
        # The string should match what's constructed in HttpRendezvous's except block for status errors
        expected_error_msg = f"Broker HTTP error: {mock_response.status_code} - {mock_response.text}"
        with self.assertRaisesRegex(Exception, expected_error_msg):
            await rendezvous.exchange_offer_answer("dummy_offer", "unknown")

    @patch('curl_cffi.requests.AsyncSession.post', new_callable=AsyncMock)
    def test_exchange_offer_answer_http_error(self, mock_curl_post):
        asyncio.run(self._async_test_exchange_offer_answer_http_error(mock_curl_post))

    async def _async_test_domain_fronting_curl_cffi(self, mock_curl_post):
        mock_response = AsyncMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json = unittest.mock.Mock(return_value={"answer": "fronted_answer", "proxy_id": "front_proxy"})
        mock_response.raise_for_status = unittest.mock.Mock()
        mock_curl_post.return_value = mock_response

        rendezvous = HttpRendezvous(
            broker_url="https://realbroker.com/api",
            front_domains=["https://frontdomain.com"],
            impersonate_profile="chrome110"
        )
        await rendezvous.exchange_offer_answer("dummy_offer", "unknown")

        mock_curl_post.assert_called_once()
        call_args = mock_curl_post.call_args
        requested_url = call_args.args[0] # URL is the first positional argument to session.post
        headers = call_args.kwargs.get('headers', {})

        self.assertTrue(requested_url.startswith("https://frontdomain.com/api"))
        self.assertEqual(headers.get("Host"), "realbroker.com")
        self.assertEqual(call_args.kwargs.get("impersonate"), "chrome110")

    @patch('curl_cffi.requests.AsyncSession.post', new_callable=AsyncMock)
    def test_domain_fronting_curl_cffi(self, mock_curl_post):
        asyncio.run(self._async_test_domain_fronting_curl_cffi(mock_curl_post))

    async def _async_test_no_impersonation(self, mock_curl_post):
        mock_response = AsyncMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json = unittest.mock.Mock(return_value={"answer": "ans", "proxy_id": "p1"})
        mock_response.raise_for_status = unittest.mock.Mock()
        mock_curl_post.return_value = mock_response

        rendezvous = HttpRendezvous(broker_url="https://fakebroker.com", impersonate_profile=None)
        await rendezvous.exchange_offer_answer("dummy_offer", "unknown")

        mock_curl_post.assert_called_once()
        self.assertIsNone(mock_curl_post.call_args.kwargs.get("impersonate"))

    @patch('curl_cffi.requests.AsyncSession.post', new_callable=AsyncMock)
    def test_no_impersonation(self, mock_curl_post):
        asyncio.run(self._async_test_no_impersonation(mock_curl_post))


class TestAmpCacheRendezvousCurlCffi(unittest.TestCase):
    async def _async_test_ampcache_exchange_success(self, mock_curl_get):
        mock_response = AsyncMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json = unittest.mock.Mock(return_value={
            "answer": "amp_answer_sdp",
            "proxy_id": "amp_proxy123"
        })
        mock_response.raise_for_status = unittest.mock.Mock()
        mock_curl_get.return_value = mock_response

        rendezvous = AmpCacheRendezvous(
            ampcache_url="https://ampcache.example.com",
            target_broker_url="https://realbroker.com/snowflake",
            impersonate_profile="safari170"
        )
        offer_sdp = "dummy_amp_offer"
        client_nat_type = "restricted"

        answer, proxy_id = await rendezvous.exchange_offer_answer(offer_sdp, client_nat_type)

        self.assertEqual(answer, "amp_answer_sdp")
        self.assertEqual(proxy_id, "amp_proxy123")
        mock_curl_get.assert_called_once()

        called_url = mock_curl_get.call_args.args[0]
        self.assertTrue(called_url.startswith("https://ampcache.example.com/v0/s/"))
        self.assertIn("payload=", called_url)
        self.assertIn(base64.urlsafe_b64encode("realbroker.com".encode()).decode().rstrip("="), called_url)
        self.assertIn("/snowflake?payload=", called_url) # Path part
        self.assertEqual(mock_curl_get.call_args.kwargs.get("impersonate"), "safari170")

    @patch('curl_cffi.requests.AsyncSession.get', new_callable=AsyncMock) # AMPCache uses GET
    def test_ampcache_exchange_success(self, mock_curl_get):
        asyncio.run(self._async_test_ampcache_exchange_success(mock_curl_get))

    # TODO: Add tests for AMPCache domain fronting, broker errors, HTTP errors similar to HttpRendezvous tests.


class TestSqsRendezvous(unittest.TestCase):
    # SQS tests are more complex due to boto3 interaction.
    # These would require mocking aioboto3.Session and its client.

    def test_placeholder_sqs(self):
        # This is a placeholder. Full SQS testing requires significant mocking.
        # Example structure:
        # @patch('aioboto3.Session')
        # async def test_sqs_exchange(self, MockSession):
        #     mock_sqs_client = AsyncMock()
        #     mock_sqs_client.send_message = AsyncMock()
        #     mock_sqs_client.receive_message = AsyncMock(return_value={
        #         "Messages": [{
        #             "Body": json.dumps({"Type": "proxyAnswer", "AnswerSDP": "sqs_answer", "ProxyID": "sqs_proxy"}),
        #             "ReceiptHandle": "handle123"
        #         }]
        #     })
        #     mock_sqs_client.delete_message = AsyncMock()

        #     # Configure MockSession to return a context manager that yields mock_sqs_client
        #     mock_session_instance = MockSession.return_value
        #     mock_sqs_context_manager = AsyncMock()
        #     mock_sqs_context_manager.__aenter__.return_value = mock_sqs_client
        #     mock_session_instance.client.return_value = mock_sqs_context_manager

        #     sqs_rendezvous = SqsRendezvous(queue_url="https://sqs.us-east-1.amazonaws.com/123/testqueue", region_name="us-east-1")
        #     answer, proxy_id = await sqs_rendezvous.exchange_offer_answer("sqs_offer", "unknown")

        #     self.assertEqual(answer, "sqs_answer")
        #     self.assertEqual(proxy_id, "sqs_proxy")
        #     mock_sqs_client.send_message.assert_called_once()
        #     mock_sqs_client.receive_message.assert_called() # Called in a loop
        #     mock_sqs_client.delete_message.assert_called_once_with(QueueUrl=sqs_rendezvous.queue_url, ReceiptHandle="handle123")
        self.assertTrue(True, "SQS tests need detailed mocking of aioboto3.")

# To run these async tests:
# 1. Ensure you have `asynctest` or similar if using older unittest with async.
#    Or run with `python -m unittest tests.test_rendezvous` if using Python 3.8+ which supports async tests natively.
# 2. For older Python versions or more complex async mocking, `unittest.mock.AsyncMock` and `asynctest` library might be needed.
#    The current setup assumes Python 3.8+ for native AsyncMock and async test support.

if __name__ == '__main__':
    # This allows running tests with `python tests/test_rendezvous.py`
    # For an async suite:
    # loop = asyncio.get_event_loop()
    # suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    # runner = unittest.TextTestRunner()
    # result = loop.run_until_complete(loop.run_in_executor(None, runner.run, suite))

    # Simpler: just use unittest.main() which should handle async tests in Python 3.8+
    unittest.main()

# import httpx # No longer needed as httpx is replaced by curl_cffi
import json # For SQS mock body
import base64 # For TestAmpCacheRendezvousCurlCffi
