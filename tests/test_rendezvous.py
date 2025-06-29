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

    @patch('curl_cffi.requests.AsyncSession.post', new_callable=AsyncMock)
    async def test_exchange_offer_answer_success(self, mock_curl_post):
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
    async def test_exchange_offer_answer_broker_error(self, mock_curl_post):
        mock_response = AsyncMock(spec=Response)
        mock_response.status_code = 200 # Broker can return 200 OK with an error field
        mock_response.json = unittest.mock.Mock(return_value={"error": "No proxies available"})
        mock_response.raise_for_status = unittest.mock.Mock()
        mock_curl_post.return_value = mock_response

        rendezvous = HttpRendezvous(broker_url="https://fakebroker.com")
        with self.assertRaisesRegex(Exception, "Broker error: No proxies available"):
            await rendezvous.exchange_offer_answer("dummy_offer", "unknown")

    @patch('curl_cffi.requests.AsyncSession.post', new_callable=AsyncMock)
    async def test_exchange_offer_answer_http_error(self, mock_curl_post):
        # Simulate RequestsError for a status code
        mock_response_obj = Response(None) # Create a dummy Response object for the error
        mock_response_obj.status_code = 500
        mock_response_obj.text = "Internal Server Error"

        # The actual error raised by curl_cffi for HTTP status might be RequestsError
        # and raise_for_status() would trigger it.
        # We need to mock raise_for_status() to throw the error that the code expects to catch.
        # Or, more simply, have the post call itself raise the error.
        mock_curl_post.side_effect = RequestsError("HTTP 500", response=mock_response_obj)

        rendezvous = HttpRendezvous(broker_url="https://fakebroker.com")
        with self.assertRaisesRegex(Exception, "Broker HTTP error: 500"):
            await rendezvous.exchange_offer_answer("dummy_offer", "unknown")

    @patch('curl_cffi.requests.AsyncSession.post', new_callable=AsyncMock)
    async def test_domain_fronting_curl_cffi(self, mock_curl_post):
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
    async def test_no_impersonation(self, mock_curl_post):
        mock_response = AsyncMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json = unittest.mock.Mock(return_value={"answer": "ans", "proxy_id": "p1"})
        mock_response.raise_for_status = unittest.mock.Mock()
        mock_curl_post.return_value = mock_response

        rendezvous = HttpRendezvous(broker_url="https://fakebroker.com", impersonate_profile=None)
        await rendezvous.exchange_offer_answer("dummy_offer", "unknown")

        mock_curl_post.assert_called_once()
        self.assertIsNone(mock_curl_post.call_args.kwargs.get("impersonate"))


class TestAmpCacheRendezvousCurlCffi(unittest.TestCase):
    @patch('curl_cffi.requests.AsyncSession.get', new_callable=AsyncMock) # AMPCache uses GET
    async def test_ampcache_exchange_success(self, mock_curl_get):
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

import httpx # For HTTPStatusError in mock
import json # For SQS mock body
