import unittest
import asyncio
import os
import logging

# Ensure client_py is in path for testing
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from client_py.config import ClientConfig
from client_py.rendezvous import HttpRendezvous
from client_py.webrtc_handler import WebRTCHandler

# Configure logging for the test
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variable to skip live tests
SKIP_LIVE_TESTS = os.environ.get("SKIP_LIVE_TESTS") == "1" # Standard skip logic
DEFAULT_BROKER_URL = "https://snowflake-broker.torproject.net/"
DEFAULT_STUN_URL = "stun:stun.l.google.com:19302"
DEFAULT_IMPERSONATE_PROFILE = "chrome120"
DATA_CHANNEL_OPEN_TIMEOUT = 20
DATA_CHANNEL_ACTIVITY_TIMEOUT = 10

@unittest.skipIf(SKIP_LIVE_TESTS, "Skipping live integration tests that hit external services.") # Reinstated
class TestWebRTCDataChannelLive(unittest.TestCase):

    def _run_async(self, coro):
        return asyncio.run(coro)

    async def _test_live_datachannel_opens_and_sends_async(self):
        logger.info(f"Starting live WebRTC DataChannel test with broker: {DEFAULT_BROKER_URL}")

        config = ClientConfig(
            broker_url=DEFAULT_BROKER_URL,
            ice_servers=[DEFAULT_STUN_URL],
            impersonate_profile=DEFAULT_IMPERSONATE_PROFILE,
            keep_local_addresses=False
        )

        rendezvous_client = HttpRendezvous(
            broker_url=config.broker_url, # type: ignore
            impersonate_profile=config.impersonate_profile
        )

        dc_opened_event = asyncio.Event()
        dc_closed_event = asyncio.Event()
        received_messages = []

        async def on_dc_open():
            logger.info("Test: DataChannel opened.")
            dc_opened_event.set()

        async def on_dc_message(message: str):
            logger.info(f"Test: DataChannel received message: {message[:100]}")
            received_messages.append(message)

        async def on_dc_close():
            logger.info("Test: DataChannel closed.")
            dc_closed_event.set()

        wrtc_handler = WebRTCHandler(
            ice_servers=config.ice_servers,
            keep_local_addresses=config.keep_local_addresses,
            on_open=on_dc_open,
            on_message=on_dc_message,
            on_close=on_dc_close
        )

        try:
            logger.info("Generating SDP offer...")
            offer_sdp = await wrtc_handler.create_offer()
            self.assertTrue(offer_sdp, "Generated SDP offer should not be empty.")

            logger.info("Exchanging offer/answer with live broker...")
            answer_sdp, proxy_id = await rendezvous_client.exchange_offer_answer(
                offer_sdp=offer_sdp,
                client_nat_type="unknown"
            )
            self.assertTrue(answer_sdp, "Answer SDP should not be empty.")
            logger.info(f"Received proxy ID: {proxy_id}, Answer SDP (first 100): {answer_sdp[:100].replace(os.linesep, ' ')}...")

            logger.info("Setting remote SDP answer...")
            await wrtc_handler.set_answer(answer_sdp)

            logger.info(f"Waiting for DataChannel to open (timeout: {DATA_CHANNEL_OPEN_TIMEOUT}s)...")
            try:
                await asyncio.wait_for(dc_opened_event.wait(), timeout=DATA_CHANNEL_OPEN_TIMEOUT)
            except asyncio.TimeoutError:
                self.fail(f"DataChannel did not open within {DATA_CHANNEL_OPEN_TIMEOUT} seconds.")

            self.assertTrue(dc_opened_event.is_set(), "DataChannel open event was not set.")
            logger.info("DataChannel successfully opened with live proxy.")

            test_message = "hello_snowflake_proxy_from_python_client"
            logger.info(f"Sending test message: '{test_message}'")
            try:
                await wrtc_handler.send_message(test_message)
                logger.info("Test message sent successfully.")
            except Exception as e:
                self.fail(f"wrtc_handler.send_message failed: {e}")

            logger.info(f"Waiting for DataChannel activity or closure (timeout: {DATA_CHANNEL_ACTIVITY_TIMEOUT}s)...")
            try:
                await asyncio.wait_for(dc_closed_event.wait(), timeout=DATA_CHANNEL_ACTIVITY_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(f"DataChannel did not close within {DATA_CHANNEL_ACTIVITY_TIMEOUT}s after sending data. Proxy might keep it open longer or an issue occurred.")

            if received_messages:
                logger.info(f"Total messages received from proxy: {len(received_messages)}")
            else:
                logger.info("No messages received from proxy (as expected, or proxy closed quickly).")

            self.assertTrue(dc_closed_event.is_set(), "DataChannel close event was expected to be set eventually.")
            logger.info("Live WebRTC DataChannel test interaction completed.")

        finally:
            logger.info("Closing WebRTCHandler for the live test.")
            if wrtc_handler:
                await wrtc_handler.close()

    def test_live_datachannel_opens_and_sends(self):
        """
        Integration test: Full rendezvous, WebRTC DataChannel establishment with a live proxy,
        tests if DC opens, a message can be sent, and the channel eventually closes.
        Requires network access. Skipped if SKIP_LIVE_TESTS=1.
        """
        self._run_async(self._test_live_datachannel_opens_and_sends_async())

if __name__ == "__main__":
    if SKIP_LIVE_TESTS: # Check module-level variable
        print("Skipping live WebRTC DataChannel integration tests (SKIP_LIVE_TESTS=1).")
    else:
        print(f"Running live WebRTC DataChannel integration tests against: {DEFAULT_BROKER_URL}")
        print(f"Using STUN: {DEFAULT_STUN_URL}, Impersonation: {DEFAULT_IMPERSONATE_PROFILE}")
        print("Ensure you have network connectivity.")

    unittest.main()
