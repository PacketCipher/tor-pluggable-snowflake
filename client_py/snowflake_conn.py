import asyncio
import logging
import random
from typing import List, Optional, Tuple, Callable, Awaitable

from client_py.config import ClientConfig
from client_py.webrtc_handler import WebRTCHandler
from client_py.rendezvous import RendezvousMethod, HttpRendezvous, SqsRendezvous # AmpCacheRendezvous needs constructor fix
# from client_py.kcp_handler import KCPHandler # Stub
# from client_py.smux_handler import SmuxSession, SmuxStream # Stub

logger = logging.getLogger(__name__)

# Constants from Go client
RECONNECT_TIMEOUT = 10  # seconds
SNOWFLAKE_TIMEOUT = 20 # seconds (for individual WebRTC connection)
DATA_CHANNEL_TIMEOUT = 10 # seconds

class PeerConnection:
    """Represents a single WebRTC connection to a Snowflake proxy."""
    def __init__(self, web_rtc_handler: WebRTCHandler, proxy_id: str):
        self.web_rtc_handler = web_rtc_handler
        self.proxy_id = proxy_id
        self.last_activity = asyncio.get_event_loop().time()
        self.is_connected = False # Becomes true when data channel is open

        # These would be set by the WebRTCHandler's callbacks
        self.on_open_event = asyncio.Event()
        self.on_close_event = asyncio.Event() # Signaled when WebRTC or DC closes

    async def establish_connection(self, rendezvous_method: RendezvousMethod, client_nat_type: str) -> None:
        """
        Creates an offer, uses rendezvous to get an answer, and sets the answer.
        """
        try:
            offer_sdp = await self.web_rtc_handler.create_offer()

            answer_sdp, proxy_id = await rendezvous_method.exchange_offer_answer(offer_sdp, client_nat_type)
            self.proxy_id = proxy_id # Update proxy_id if provided by rendezvous

            await self.web_rtc_handler.set_answer(answer_sdp)
            # At this point, ICE negotiation starts. We need to wait for data channel to open.
            logger.info(f"PeerConnection: Waiting for data channel to open with proxy {self.proxy_id}...")

            # The WebRTCHandler's on_open callback for the data channel should set self.on_open_event
            # We also need a timeout for this.
            await asyncio.wait_for(self.on_open_event.wait(), timeout=DATA_CHANNEL_TIMEOUT)

            self.is_connected = True
            self.last_activity = asyncio.get_event_loop().time()
            logger.info(f"PeerConnection: Data channel OPEN with proxy {self.proxy_id}")

        except asyncio.TimeoutError:
            logger.warning(f"PeerConnection: Timeout waiting for data channel to open with {self.proxy_id}.")
            await self.close() # Ensure cleanup
            raise
        except Exception as e:
            logger.error(f"PeerConnection: Error establishing connection with {self.proxy_id}: {e}")
            await self.close() # Ensure cleanup
            raise

    async def send(self, data: str) -> None: # Data channel sends strings or bytes
        if self.is_connected and self.web_rtc_handler.data_channel:
            await self.web_rtc_handler.send_message(data)
            self.last_activity = asyncio.get_event_loop().time()
        else:
            raise ConnectionError(f"PeerConnection with {self.proxy_id} not connected or data channel unavailable.")

    async def receive(self) -> str: # This is a simplified receive for direct DC usage
        # In reality, KCP/SMUX would handle the stream of bytes.
        # This is a placeholder if we were to use the DC directly.
        if self.is_connected and self.web_rtc_handler.data_channel:
            # WebRTCHandler needs a way to queue incoming messages if not using KCP/SMUX
            # For now, this is conceptual. The on_message callback in WebRTCHandler would feed a queue.
            raise NotImplementedError("Direct receive on PeerConnection not fully implemented; use KCP/SMUX.")
        else:
            raise ConnectionError(f"PeerConnection with {self.proxy_id} not connected.")

    async def close(self) -> None:
        logger.info(f"PeerConnection: Closing connection with proxy {self.proxy_id}")
        self.is_connected = False
        await self.web_rtc_handler.close()
        self.on_close_event.set() # Signal that it's closed

    def is_alive(self) -> bool:
        # Check based on last activity time against SNOWFLAKE_TIMEOUT
        # Or if the underlying WebRTC connection is still reported as active.
        # For now, primarily based on explicit close or initial failure.
        if not self.is_connected and self.on_close_event.is_set(): # If it never connected or explicitly closed
            return False
        if self.is_connected and (asyncio.get_event_loop().time() - self.last_activity > SNOWFLAKE_TIMEOUT):
            logger.warning(f"PeerConnection with {self.proxy_id} timed out due to inactivity.")
            # Should trigger a close
            # asyncio.create_task(self.close()) # Avoid awaiting here
            return False
        return True # Assumed alive if connected and not timed out, or attempting connection


class Peers:
    """Manages a pool of active WebRTC peer connections (Snowflakes)."""
    def __init__(self, config: ClientConfig, rendezvous_method: RendezvousMethod):
        self.config = config
        self.rendezvous_method = rendezvous_method
        self.client_nat_type = "unknown" # TODO: Implement NAT type detection like Go client

        self.pool: List[PeerConnection] = []
        self._available_peers_queue: asyncio.Queue[PeerConnection] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._stopped = False
        self._collect_task: Optional[asyncio.Task] = None

        # Callbacks for WebRTCHandler to signal PeerConnection events
        self._setup_webrtc_callbacks()

    def _setup_webrtc_callbacks(self):
        # These need to be instance methods if they modify instance state of PeerConnection
        # Or PeerConnection needs to be passed to them.
        # For simplicity, PeerConnection itself will set its events.
        # WebRTCHandler will call these generic callbacks, which then find the relevant PeerConnection.
        # This is a bit indirect. Better: WebRTCHandler's callbacks are set per instance.
        pass


    def _create_new_peer_connection_obj(self) -> PeerConnection:
        # Define the on_open, on_message, on_close for this specific WebRTCHandler instance
        # This requires WebRTCHandler to accept these specific callbacks for its DC.

        # Temporary placeholder for callbacks until WebRTCHandler is adjusted or PeerConnection handles it internally
        async def temp_on_open(): pass
        async def temp_on_message(msg): pass
        async def temp_on_close(): pass

        # This is where ICE servers from config.ice_servers would be used.
        # And config.keep_local_addresses.
        web_rtc_handler = WebRTCHandler(
            ice_servers=self.config.ice_servers,
            # Callbacks below need to be tied to the PeerConnection instance
            # on_open=temp_on_open,
            # on_message=temp_on_message,
            # on_close=temp_on_close
        )

        peer_conn = PeerConnection(web_rtc_handler, proxy_id="pending")

        # Wire WebRTCHandler's DC events to the PeerConnection's events
        # This assumes WebRTCHandler allows setting these post-init or takes them at init
        # and calls them. Let's assume WebRTCHandler calls are generic and PeerConnection
        # sets them up itself by passing its own methods to WebRTCHandler.

        # Modifying WebRTCHandler to take callables that accept the PeerConnection instance,
        # or more simply, the PeerConnection object itself configures its WebRTCHandler instance's callbacks
        # to set its own internal asyncio.Events.

        # Let PeerConnection.establish_connection handle setting up the specific callbacks
        # by assigning its own methods to web_rtc_handler._on_open_callback etc.
        # This is what WebRTCHandler currently supports.
        web_rtc_handler._on_open_callback = peer_conn.on_open_event.set

        # A real on_close for WebRTCHandler would be more like:
        async def _managed_on_close():
            logger.debug(f"Peers: WebRTCHandler closed for proxy {peer_conn.proxy_id}, signaling PeerConnection.")
            peer_conn.is_connected = False # Ensure state update
            peer_conn.on_close_event.set()
        web_rtc_handler._on_close_callback = _managed_on_close

        # _on_message_callback is for KCP/SMUX layer, not directly Peers.
        # web_rtc_handler._on_message_callback = ...

        return peer_conn


    async def collect_one(self) -> Optional[PeerConnection]:
        """Attempts to gather one Snowflake proxy connection."""
        if self._stopped:
            return None

        logger.info("Peers: Attempting to collect a new Snowflake...")
        peer_conn_obj = self._create_new_peer_connection_obj()

        try:
            await peer_conn_obj.establish_connection(self.rendezvous_method, self.client_nat_type)

            # Successfully connected
            async with self._lock:
                if len(self.pool) < self.config.max_peers:
                    self.pool.append(peer_conn_obj)
                    await self._available_peers_queue.put(peer_conn_obj)
                    logger.info(f"Peers: New Snowflake proxy {peer_conn_obj.proxy_id} collected and added to pool. Pool size: {len(self.pool)}")
                    return peer_conn_obj
                else:
                    logger.info(f"Peers: Pool is full ({len(self.pool)}/{self.config.max_peers}). Discarding new peer {peer_conn_obj.proxy_id}.")
                    await peer_conn_obj.close() # Close if pool is full
                    return None
        except Exception as e:
            logger.warning(f"Peers: Failed to collect a Snowflake: {e}")
            # Ensure peer_conn_obj is cleaned up if establish_connection failed partway
            if not peer_conn_obj.on_close_event.is_set(): # If not already closed by establish_connection
                 await peer_conn_obj.web_rtc_handler.close() # Ensure underlying WebRTC is closed
            return None

    async def connect_loop(self):
        """Maintains the pool of available Snowflake connections."""
        logger.info("Peers: Starting connect_loop.")
        while not self._stopped:
            try:
                num_needed = self.config.max_peers - len(self.pool) # Or _available_peers_queue.qsize()? Pool is better.
                if num_needed > 0:
                    logger.info(f"Peers: Connect loop needs {num_needed} more peers (current pool: {len(self.pool)}).")
                    await self.collect_one() # Collect one and loop will check again
                else:
                    logger.debug("Peers: Connect loop - pool is full.")

                # Cleanup dead peers from pool
                async with self._lock:
                    original_pool_size = len(self.pool)
                    live_peers = []
                    for p in self.pool:
                        if p.is_alive():
                            live_peers.append(p)
                        else:
                            logger.info(f"Peers: Removing dead peer {p.proxy_id} from pool.")
                            if not p.on_close_event.is_set(): # If not already closed
                                await p.close() # Ensure it's closed
                    self.pool = live_peers
                    if len(self.pool) != original_pool_size:
                         logger.info(f"Peers: Pool cleaned. New size: {len(self.pool)}")


                await asyncio.sleep(RECONNECT_TIMEOUT if num_needed > 0 else 1) # Shorter sleep if just checking, longer if actively trying
            except asyncio.CancelledError:
                logger.info("Peers: Connect_loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Peers: Error in connect_loop: {e}", exc_info=True)
                await asyncio.sleep(RECONNECT_TIMEOUT) # Wait before retrying loop on error

    async def pop(self) -> PeerConnection:
        """Retrieves an available WebRTC connection. Blocks if none are available."""
        if self._stopped and self._available_peers_queue.empty():
            raise ConnectionAbortedError("Peers manager stopped and no available peers.")

        logger.info("Peers: Waiting for an available Snowflake peer...")
        peer = await self._available_peers_queue.get()
        self._available_peers_queue.task_done()

        async with self._lock: # Remove from active pool once popped for use
            if peer in self.pool: # Should be
                self.pool.remove(peer)
                logger.info(f"Peers: Popped peer {peer.proxy_id}. Pool size: {len(self.pool)}")
            else: # Should not happen if logic is correct
                 logger.warning(f"Peers: Popped peer {peer.proxy_id} was not in the active pool. This is unexpected.")


        if not peer.is_alive(): # Double check, though connect_loop should clean up
            logger.warning(f"Peers: Popped peer {peer.proxy_id} is not alive. Attempting to get another.")
            await peer.close() # Ensure it's closed
            return await self.pop() # Recursively try to get another one

        logger.info(f"Peers: Successfully popped peer {peer.proxy_id}.")
        return peer

    def start(self):
        if not self._collect_task:
            self._stopped = False
            self._collect_task = asyncio.create_task(self.connect_loop())
            logger.info("Peers: Manager started.")

    async def stop(self):
        if self._stopped:
            return
        self._stopped = True
        logger.info("Peers: Stopping manager...")
        if self._collect_task:
            self._collect_task.cancel()
            try:
                await self._collect_task
            except asyncio.CancelledError:
                logger.info("Peers: Collect_loop task successfully cancelled.")
            self._collect_task = None

        logger.info("Peers: Closing all connections in pool...")
        async with self._lock:
            for peer in self.pool:
                await peer.close()
            self.pool.clear()

        # Drain the queue of any pending peers
        while not self._available_peers_queue.empty():
            try:
                peer = self._available_peers_queue.get_nowait()
                await peer.close()
            except asyncio.QueueEmpty:
                break
        logger.info("Peers: Manager stopped.")


class SnowflakeConn:
    """
    Represents a persistent connection to a Snowflake server, multiplexed over
    one or more ephemeral WebRTC connections (Snowflakes).
    This is the Python equivalent of client/lib/snowflake.go's SnowflakeConn.
    """
    def __init__(self, config: ClientConfig, rendezvous: RendezvousMethod):
        self.config = config
        self.rendezvous = rendezvous
        self.peers_manager: Optional[Peers] = None

        # These would be the KCP and SMUX layers
        # self.kcp_handler: Optional[KCPHandler] = None
        # self.smux_session: Optional[SmuxSession] = None
        self.underlying_connection: Optional[PeerConnection] = None # The current WebRTC peer
        self._is_closed = False
        logger.info("SnowflakeConn initialized. KCP and SMUX layers are currently STUBS.")

    async def dial(self) -> None: # In Go, returns net.Conn (smux.Stream)
        """
        Establishes the Snowflake connection.
        In a full implementation, this would return an smux.Stream equivalent.
        For now, it sets up the Peers manager and prepares for KCP/SMUX.
        """
        if self._is_closed:
            raise ConnectionError("Cannot dial on a closed SnowflakeConn.")

        self.peers_manager = Peers(self.config, self.rendezvous)
        self.peers_manager.start()
        logger.info("SnowflakeConn: Peers manager started.")

        # --- This is where KCP and SMUX logic would go ---
        # 1. Get an initial WebRTC connection (PeerConnection) from self.peers_manager.pop()
        #    This PeerConnection's WebRTCHandler data channel is the raw transport.
        #
        # 2. Setup KCPHandler:
        #    - send_callback for KCP: sends packets over the current WebRTC DC.
        #    - receive_callback for KCP: data from KCP to SMUX.
        #    - KCPHandler.handle_incoming_packet(): feed it with messages from WebRTC DC.
        #
        # 3. Setup SmuxSession:
        #    - underlying_send_func for SMUX: sends SMUX frames via KCPHandler.send_data().
        #    - SmuxSession.handle_incoming_packet(): feed it with data from KCP's receive_callback.
        #
        # 4. Open an SMUX stream:
        #    - smux_stream = await self.smux_session.open_stream()
        #    - This smux_stream is the net.Conn equivalent.
        #
        # The Go client uses a `RedialPacketConn` concept to allow KCP to switch
        # underlying WebRTC connections when one dies. This is complex.
        # A simpler start might be a KCP session over a single WebRTC connection.

        logger.warning("SnowflakeConn.dial(): KCP and SMUX layers are STUBS.")
        logger.info("SnowflakeConn.dial(): For now, will pop one peer to demonstrate flow but not establish KCP/SMUX.")

        try:
            # Get the first peer connection to bootstrap
            self.underlying_connection = await self.peers_manager.pop()
            logger.info(f"SnowflakeConn: Popped initial peer {self.underlying_connection.proxy_id} for KCP/SMUX (conceptual).")

            # At this point, self.underlying_connection.web_rtc_handler.data_channel is available.
            # This would be the pipe for KCP.
            # For example:
            # async def kcp_send_via_webrtc(data: bytes):
            #     if self.underlying_connection and self.underlying_connection.is_connected:
            #         await self.underlying_connection.web_rtc_handler.send_message(data.decode()) # Assuming KCP packets are stringified for DC
            #     else: # Handle connection drop, redial logic needed here
            #         logger.error("KCP send: No underlying WebRTC connection!")
            #
            # async def smux_receive_from_kcp(data: bytes):
            #     if self.smux_session:
            #         await self.smux_session.handle_incoming_packet(data) # SMUX expects frames
            #
            # self.kcp_handler = KCPHandler(send_callback=kcp_send_via_webrtc, receive_callback=smux_receive_from_kcp)
            # # Need to wire web_rtc_handler's on_message to kcp_handler.handle_incoming_packet
            # if self.underlying_connection:
            #     self.underlying_connection.web_rtc_handler._on_message_callback = self.kcp_handler.handle_incoming_packet

            # self.smux_session = SmuxSession(underlying_send_func=self.kcp_handler.send_data, is_client=True)

            # smux_app_stream = await self.smux_session.open_stream()
            # logger.info(f"SnowflakeConn: SMUX Stream {smux_app_stream.stream_id} opened (conceptual).")
            # return smux_app_stream # This would be the goal

            # For now, since KCP/SMUX are stubs, we don't return a stream.
            # The 'dial' has "succeeded" in getting a peer.
            if not self.underlying_connection: # Should not happen if pop worked
                 raise ConnectionError("Failed to get an initial peer connection.")

            return None # Placeholder: should return an SMUX stream object

        except Exception as e:
            logger.error(f"SnowflakeConn: Error during dial: {e}", exc_info=True)
            await self.close() # Cleanup if dial fails
            raise

    async def close(self) -> None:
        if self._is_closed:
            return
        self._is_closed = True
        logger.info("SnowflakeConn: Closing connection...")

        # if self.smux_session:
        #     await self.smux_session.close()
        #     self.smux_session = None
        # logger.info("SnowflakeConn: SMUX session closed (conceptual).")

        # if self.kcp_handler:
        #     await self.kcp_handler.close()
        #     self.kcp_handler = None
        # logger.info("SnowflakeConn: KCP handler closed (conceptual).")

        if self.underlying_connection: # Close any currently held peer
            await self.underlying_connection.close()
            self.underlying_connection = None

        if self.peers_manager:
            await self.peers_manager.stop()
            self.peers_manager = None
        logger.info("SnowflakeConn: Peers manager stopped.")
        logger.info("SnowflakeConn: Connection closed.")


# --- Example Usage ---
async def example_snowflake_conn():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Dummy config
    config = ClientConfig(
        broker_url="https://snowflake-broker.torproject.net/", # Using public broker for example
        ice_servers=["stun:stun.l.google.com:19302"],
        max_peers=1
    )
    # Choose rendezvous method
    # rendezvous = HttpRendezvous(broker_url=config.broker_url, front_domains=config.front_domains)
    # Or SQS, AMPCache (once constructor is fixed)

    # Forcing HttpRendezvous for this example
    if not config.broker_url:
        logger.error("Broker URL must be set in ClientConfig for this example.")
        return

    rendezvous = HttpRendezvous(broker_url=config.broker_url)


    snowflake_connection = SnowflakeConn(config, rendezvous)

    try:
        logger.info("Attempting SnowflakeConn.dial()...")
        # dial() currently returns None as SMUX stream is conceptual
        # In a real scenario, it would return a stream-like object.
        await snowflake_connection.dial()

        logger.info("SnowflakeConn.dial() completed (conceptually).")
        logger.info("Normally, you would now have an SMUX stream to read/write from.")
        logger.info("Since KCP/SMUX are stubs, this example will just hold for a bit then close.")

        # Simulate keeping the connection alive for some time
        await asyncio.sleep(15)

    except Exception as e:
        logger.error(f"Error in SnowflakeConn example: {e}", exc_info=True)
    finally:
        logger.info("Closing SnowflakeConn in example...")
        await snowflake_connection.close()
        logger.info("SnowflakeConn example finished.")

if __name__ == "__main__":
    # This example will try to connect to a real broker and STUN server.
    # It will establish WebRTC connections but won't use KCP/SMUX.
    try:
        asyncio.run(example_snowflake_conn())
    except KeyboardInterrupt:
        logger.info("SnowflakeConn example interrupted by user.")
    except Exception as e:
        logger.error(f"Unhandled error in main: {e}", exc_info=True)
