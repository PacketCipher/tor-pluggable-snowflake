import asyncio
import logging
from typing import List, Optional, Callable, Awaitable

from aiortc import RTCIceCandidate, RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.signaling import object_from_string, object_to_string

logger = logging.getLogger(__name__)

class WebRTCHandler:
    def __init__(self,
                 ice_servers: List[str],
                 keep_local_addresses: bool = False, # Added config option
                 on_open: Optional[Callable[[], Awaitable[None]]] = None,
                 on_message: Optional[Callable[[str], Awaitable[None]]] = None,
                 on_close: Optional[Callable[[], Awaitable[None]]] = None):
        self.pc = RTCPeerConnection()
        self.ice_servers = ice_servers
        self.keep_local_addresses = keep_local_addresses # Store config

        if self.ice_servers:
            # aiortc expects RTCIceServer objects or dicts. String list needs conversion.
            ice_server_dicts = []
            for server_url in self.ice_servers:
                # Basic parsing, assuming format like "stun:server:port" or "turn:server:port"
                # More complex URLs with username/credential would need more parsing.
                ice_server_dicts.append({"urls": server_url})

            self.pc.setConfiguration({"iceServers": ice_server_dicts})

        self.data_channel = None
        self._on_open_callback = on_open
        self._on_message_callback = on_message
        self._on_close_callback = on_close

        self._setup_event_handlers()

    def _setup_event_handlers(self):
        @self.pc.on("icecandidate")
        async def on_icecandidate(candidate: Optional[RTCIceCandidate]):
            if candidate:
                logger.info(f"ICE Candidate: {candidate.to_json()}")
            # In a real scenario, this candidate would be sent to the remote peer via signaling

        @self.pc.on("iceconnectionstatechange")
        async def on_iceconnectionstatechange():
            logger.info(f"ICE connection state is {self.pc.iceConnectionState}")
            if self.pc.iceConnectionState == "failed":
                await self.pc.close()
            elif self.pc.iceConnectionState == "closed":
                if self._on_close_callback and self.data_channel and self.data_channel.readyState == "closed":
                    await self._on_close_callback()


        @self.pc.on("track")
        async def on_track(track):
            logger.info(f"Track {track.kind} received")
            # Snowflake client typically doesn't receive tracks, it initiates data channels

        @self.pc.on("datachannel")
        async def on_datachannel(channel):
            logger.info(f"Data channel {channel.label} received (unexpected for client initiator)")
            # This is typically for the accepting side, client initiates the DC

    async def create_offer(self) -> str:
        """
        Creates an SDP offer and sets the local description.
        Returns the SDP offer as a string.
        """
        self.data_channel = self.pc.createDataChannel("snowflake")
        logger.info(f"Data channel '{self.data_channel.label}' created")

        @self.data_channel.on("open")
        async def on_open():
            logger.info(f"Data channel '{self.data_channel.label}' opened")
            if self._on_open_callback:
                await self._on_open_callback()

        @self.data_channel.on("message")
        async def on_message(message: str):
            # logger.debug(f"Message received on '{self.data_channel.label}': {message[:50]}...")
            if self._on_message_callback:
                await self._on_message_callback(message)

        @self.data_channel.on("close")
        async def on_close():
            logger.info(f"Data channel '{self.data_channel.label}' closed")
            # This might be triggered before ICE connection closes
            # Ensure on_close_callback is handled robustly, possibly via ICE state

        offer = await self.pc.createOffer()
        await self.pc.setLocalDescription(offer)

        # Get the SDP string from the local description
        offer_sdp_str = object_to_string(self.pc.localDescription)

        # Filter local addresses if needed
        if not self.keep_local_addresses:
            from client_py.utils import filter_local_addresses_from_sdp # Import here to avoid circular deps if utils imports webrtc_handler
            try:
                filtered_offer_sdp_str = filter_local_addresses_from_sdp(offer_sdp_str, self.keep_local_addresses)
                if filtered_offer_sdp_str != offer_sdp_str:
                    logger.info("SDP Offer filtered for local addresses.")
                    # Update the local description with the filtered SDP
                    # This requires creating a new RTCSessionDescription object
                    # However, aiortc doesn't allow easily modifying the description once set or
                    # re-setting it with a modified string directly without re-negotiation.
                    # The filtering should ideally happen *before* setLocalDescription or by
                    # influencing candidate gathering.
                    # For now, we return the filtered string, assuming the caller handles it.
                    # This is a known limitation of this approach with aiortc.
                    # The Go client modifies the string *before* sending it to the broker.
                    offer_sdp_str = filtered_offer_sdp_str
                else:
                    logger.debug("No changes to SDP after attempting to filter local addresses.")
            except Exception as e:
                logger.error(f"Error filtering local addresses from SDP: {e}. Using original SDP.", exc_info=True)

        logger.info("SDP Offer created (and potentially filtered). Local description set.")
        return offer_sdp_str

    async def set_answer(self, answer_sdp_str: str) -> None:
        """
        Sets the remote SDP answer.
        """
        answer = object_from_string(answer_sdp_str)
        if not isinstance(answer, RTCSessionDescription):
            raise ValueError("Provided answer SDP is not a valid RTCSessionDescription")
        await self.pc.setRemoteDescription(answer)
        logger.info("Remote SDP answer set.")

    async def send_message(self, message: str) -> None:
        if self.data_channel and self.data_channel.readyState == "open":
            self.data_channel.send(message)
            # logger.debug(f"Message sent: {message[:50]}...")
        else:
            logger.warning("Data channel not open or not created. Cannot send message.")

    async def close(self) -> None:
        logger.info("Closing WebRTC peer connection.")
        if self.data_channel and self.data_channel.readyState != "closed":
             self.data_channel.close()
        await self.pc.close()
        # The on_iceconnectionstatechange to "closed" should trigger the final callback

async def example_main():
    logging.basicConfig(level=logging.INFO)

    async def on_dc_open():
        print("Data Channel Opened!")
        # Example: Send a message after opening
        # This part of the example won't fully run without a remote peer answering
        # await handler.send_message("Hello from Python Snowflake Client!")

    async def on_dc_message(msg: str):
        print(f"Received message: {msg}")

    async def on_dc_close():
        print("Data Channel or Connection Closed!")

    # Dummy ICE servers for local testing if no STUN server is available
    # For real use, provide actual STUN/TURN servers
    ice_servers = ["stun:stun.l.google.com:19302"]
    # Or use config.ice_servers from your config module

    handler = WebRTCHandler(ice_servers=ice_servers,
                            on_open=on_dc_open,
                            on_message=on_dc_message,
                            on_close=on_dc_close)

    try:
        offer_sdp = await handler.create_offer()
        print("\n----- Offer SDP -----")
        print(offer_sdp)
        print("---------------------\n")

        # --- In a real application, the offer_sdp is sent to the remote peer ---
        # --- and an answer_sdp is received. For this example, we'll simulate it. ---

        # This is where you would paste an SDP answer from a Snowflake proxy/broker
        # For now, we can't proceed further in this example without a remote peer.
        # If you have an answer, you can uncomment and use it:
        # answer_sdp_input = input("Paste SDP Answer here: ")
        # if answer_sdp_input:
        #     await handler.set_answer(answer_sdp_input)
        #     # Keep the connection alive for a bit to see if it connects
        #     await asyncio.sleep(30)
        # else:
        #     print("No SDP answer provided, example will stop here.")

        print("WebRTCHandler created. Offer generated.")
        print("To test further, you need a remote peer to provide an SDP answer.")
        print("The ICE connection will likely fail or close without a remote peer.")

        # Keep it running for a bit to observe ICE state changes or if an answer is pasted
        await asyncio.sleep(10)

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        await handler.close()
        logger.info("Example finished.")

if __name__ == "__main__":
    try:
        asyncio.run(example_main())
    except KeyboardInterrupt:
        pass
