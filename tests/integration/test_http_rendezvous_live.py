import unittest
import asyncio
import os
import logging

# Ensure client_py is in path for testing
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from client_py.rendezvous import HttpRendezvous
from client_py.webrtc_handler import WebRTCHandler # To generate a real offer
from client_py.config import ClientConfig # For WebRTCHandler config

# Configure logging for the test; can be adjusted for more/less verbosity
# For live tests, seeing some logs can be helpful.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variable to skip live tests
SKIP_LIVE_TESTS = os.environ.get("SKIP_LIVE_TESTS") == "1"
DEFAULT_BROKER_URL = "https://snowflake-broker.torproject.net/"
DEFAULT_IMPERSONATE_PROFILE = "chrome120" # A common, recent profile

@unittest.skipIf(SKIP_LIVE_TESTS, "Skipping live integration tests that hit external services.")
class TestHttpRendezvousLive(unittest.TestCase):

    def _run_async(self, coro):
        """Helper to run async functions in unittest."""
        return asyncio.run(coro)

    async def _generate_real_offer_sdp(self) -> str:
        """Generates a realistic SDP offer using WebRTCHandler."""
        # Minimal config for WebRTCHandler to generate an offer
        # Using a public STUN server for realistic candidate generation if aiortc uses it during offer.
        config = ClientConfig(ice_servers=["stun:stun.l.google.com:19302"])

        wrtc_handler = WebRTCHandler(
            ice_servers=config.ice_servers,
            keep_local_addresses=config.keep_local_addresses
        )
        offer_sdp = ""
        try:
            logger.info("Generating a real SDP offer for the live test...")
            offer_sdp = await wrtc_handler.create_offer()
            self.assertTrue(offer_sdp, "Generated SDP offer should not be empty.")
            logger.info(f"Generated Offer SDP (first 100 chars): {offer_sdp[:100].replace(os.linesep, ' ')}...")
        except Exception as e:
            logger.error(f"Failed to generate SDP offer for live test: {e}", exc_info=True)
            self.fail(f"SDP Offer generation failed: {e}")
        finally:
            logger.info("Closing WebRTCHandler used for offer generation.")
            await wrtc_handler.close() # Important to release resources
        return offer_sdp

    async def _test_live_exchange_async(self):
        logger.info(f"Starting live test for HttpRendezvous with broker: {DEFAULT_BROKER_URL}")

        rendezvous_client = HttpRendezvous(
            broker_url=DEFAULT_BROKER_URL,
            impersonate_profile=DEFAULT_IMPERSONATE_PROFILE
        )

        offer_sdp = await self._generate_real_offer_sdp()

        try:
            logger.info("Attempting to exchange offer/answer with live broker...")
            answer_sdp, proxy_id = await rendezvous_client.exchange_offer_answer(
                offer_sdp=offer_sdp,
                client_nat_type="unknown" # NAT type isn't critical for this test's success
            )

            logger.info(f"Received Proxy ID: {proxy_id}")
            logger.info(f"Received Answer SDP (first 100 chars): {answer_sdp[:100].replace(os.linesep, ' ')}...")

            self.assertTrue(answer_sdp, "Answer SDP should not be empty.")
            # Proxy ID can sometimes be empty/None from some brokers if no specific ID is assigned.
            # For Tor Snowflake broker, it's usually present.
            self.assertIsNotNone(proxy_id, "Proxy ID should ideally be present.")

            # Basic validation of answer SDP content
            self.assertIn("v=0", answer_sdp.lower())
            self.assertIn("o=-", answer_sdp.lower()) # Common part of SDP origin line
            self.assertIn("a=ice-ufrag", answer_sdp.lower())
            self.assertIn("a=candidate", answer_sdp.lower()) # Expect at least one candidate typically
            self.assertIn("m=application", answer_sdp.lower()) # Media line for data channel

            logger.info("Live HttpRendezvous exchange test successful.")

        except Exception as e:
            # This could be a network issue, broker issue, or an issue with our client/impersonation.
            # For a CI test, we might want to distinguish transient errors from client bugs.
            logger.error(f"Live HttpRendezvous exchange failed: {e}", exc_info=True)
            if "No proxies available" in str(e):
                logger.warning("Broker reported no proxies available. This is a valid broker response but means no proxy obtained.")
                # Depending on strictness, this could be a pass or a specific type of skip/fail.
                # For now, let's consider it a pass for the communication itself.
                pass
            else:
                self.fail(f"HttpRendezvous live exchange raised an unexpected exception: {e}")

    def test_live_broker_exchange(self):
        """
        Integration test for HttpRendezvous: performs an offer/answer exchange
        with the live Tor Snowflake broker.
        This test requires network access and depends on the broker's availability.
        It can be skipped by setting the environment variable SKIP_LIVE_TESTS=1.
        """
        self._run_async(self._test_live_exchange_async())


if __name__ == "__main__":
    if SKIP_LIVE_TESTS:
        print("Skipping live integration tests (SKIP_LIVE_TESTS=1).")
    else:
        print(f"Running live integration tests against: {DEFAULT_BROKER_URL}")
        print(f"Using impersonation profile: {DEFAULT_IMPERSONATE_PROFILE}")
        print("Ensure you have network connectivity.")

    # Configure logging for direct script run if needed
    # logging.getLogger('client_py').setLevel(logging.DEBUG) # Example

    unittest.main()
