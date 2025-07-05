import asyncio
import logging
import random
from typing import List, Optional, Tuple, Callable, Awaitable, Any

from client_py.config import ClientConfig
from client_py.webrtc_handler import WebRTCHandler
from client_py.rendezvous import RendezvousMethod, HttpRendezvous # SqsRendezvous, AmpCacheRendezvous
from client_py.kcp_handler import KCPHandler
from client_py.smux_handler import SmuxSession, SmuxStream

logger = logging.getLogger(__name__)

# Constants from Go client
RECONNECT_TIMEOUT = 10  # seconds
SNOWFLAKE_TIMEOUT = 20 # seconds (for individual WebRTC connection)
DATA_CHANNEL_TIMEOUT = 10 # seconds
UINT32_MAX = 0xFFFFFFFF

class PeerConnection:
    """Represents a single WebRTC connection to a Snowflake proxy."""
    def __init__(self, web_rtc_handler: WebRTCHandler, proxy_id: str, loop: asyncio.AbstractEventLoop):
        self.web_rtc_handler = web_rtc_handler
        self.proxy_id = proxy_id
        self.loop = loop
        self.last_activity = self.loop.time()
        self.is_connected = False # Becomes true when data channel is open

        self.on_open_event = asyncio.Event()
        self.on_close_event = asyncio.Event()

    async def establish_connection(self, rendezvous_method: RendezvousMethod, client_nat_type: str) -> None:
        try:
            offer_sdp = await self.web_rtc_handler.create_offer()
            answer_sdp, proxy_id = await rendezvous_method.exchange_offer_answer(offer_sdp, client_nat_type)
            self.proxy_id = proxy_id or self.proxy_id

            await self.web_rtc_handler.set_answer(answer_sdp)
            logger.info(f"PeerConnection: Waiting for data channel to open with proxy {self.proxy_id}...")
            await asyncio.wait_for(self.on_open_event.wait(), timeout=DATA_CHANNEL_TIMEOUT)
            self.is_connected = True
            self.last_activity = self.loop.time()
            logger.info(f"PeerConnection: Data channel OPEN with proxy {self.proxy_id}")
        except asyncio.TimeoutError:
            logger.warning(f"PeerConnection: Timeout waiting for data channel to open with {self.proxy_id}.")
            await self.close()
            raise
        except Exception as e:
            logger.error(f"PeerConnection: Error establishing connection with {self.proxy_id}: {e}")
            await self.close()
            raise

    async def send_on_datachannel(self, data: bytes) -> None:
        """Sends bytes on the WebRTC data channel."""
        if self.is_connected and self.web_rtc_handler.data_channel:
            # KCP/SMUX will send bytes. WebRTCHandler needs to handle bytes.
            # Assuming WebRTCHandler.send_message can take bytes or string.
            # If it strictly takes string, KCP output needs encoding (e.g., base64),
            # and WebRTC on_message needs decoding. For efficiency, binary is preferred.
            # Let's assume WebRTCHandler is adapted for bytes if underlying DC supports it.
            # aiortc data channels can send bytes.
            await self.web_rtc_handler.send_message(data)
            self.last_activity = self.loop.time()
        else:
            # This path should ideally not be hit if KCP is only given a live channel.
            # If it is, KCP's send will fail, and it should retry or signal error.
            logger.warning(f"PeerConnection with {self.proxy_id}: Attempt to send when not connected or DC unavailable.")
            raise ConnectionError(f"PeerConnection {self.proxy_id} not ready for send.")


    async def close(self) -> None:
        logger.info(f"PeerConnection: Closing connection with proxy {self.proxy_id}")
        self.is_connected = False # Mark as not connected first
        await self.web_rtc_handler.close() # This should trigger _on_close_callback
        self.on_close_event.set() # Explicitly signal closure

    def is_alive(self) -> bool:
        if self.on_close_event.is_set(): # Explicitly closed
            return False
        if not self.is_connected : # Not yet open or closed without event set (should not happen)
            # If it's trying to connect, on_open_event won't be set.
            # If on_close_event is not set, and is_connected is false, it might be still trying.
            # This needs careful state. is_alive is for *active, usable* peers.
            return False # If not connected, it's not "alive" for use.

        # Check based on last activity time against SNOWFLAKE_TIMEOUT
        # This timeout is for the WebRTC connection itself. KCP/SMUX have their own.
        if self.loop.time() - self.last_activity > SNOWFLAKE_TIMEOUT:
            logger.warning(f"PeerConnection with {self.proxy_id} deemed inactive by timeout.")
            # Should trigger an async close, not block here.
            # The Peers manager should handle this cleanup.
            return False
        return True


class Peers:
    """Manages a pool of active WebRTC peer connections (Snowflakes)."""
    def __init__(self, config: ClientConfig, rendezvous_method: RendezvousMethod, loop: asyncio.AbstractEventLoop):
        self.config = config
        self.rendezvous_method = rendezvous_method
        self.client_nat_type = "unknown"
        self.loop = loop

        self.pool: List[PeerConnection] = [] # Peers being actively used or ready in queue
        self._available_peers_queue: asyncio.Queue[PeerConnection] = asyncio.Queue()
        self._lock = asyncio.Lock() # Protects self.pool
        self._stopped = False
        self._collect_task: Optional[asyncio.Task] = None

        # Callback from SnowflakeConn for KCP to handle incoming messages
        self.kcp_input_callback: Optional[Callable[[bytes], Awaitable[None]]] = None


    def _create_new_peer_connection_obj(self) -> PeerConnection:
        # WebRTCHandler will be configured here. Its on_message callback needs to feed KCP.
        # The on_open and on_close callbacks will manage PeerConnection state.

        peer_conn_obj_ref = [None] # To allow inner functions to reference the PeerConnection object

        async def managed_on_message(message: Any): # Message can be str or bytes from aiortc
            # This callback is now for KCP.
            # Ensure message is bytes for KCP.
            data_bytes: bytes
            if isinstance(message, str):
                # This case should ideally be avoided. KCP expects bytes.
                # If underlying DC sends text, it implies an issue in DC config or WebRTCHandler.
                logger.warning("PeerConnection: Received string message from DC, expected bytes for KCP. Encoding to UTF-8.")
                data_bytes = message.encode('utf-8', errors='replace')
            elif isinstance(message, bytes):
                data_bytes = message
            else:
                logger.error(f"PeerConnection: Unknown message type from DC: {type(message)}. Discarding.")
                return

            if self.kcp_input_callback:
                await self.kcp_input_callback(data_bytes)
            else:
                logger.warning("PeerConnection: Message received but no KCP input callback set.")

            # Update activity on the PeerConnection object
            if peer_conn_obj_ref[0]:
                peer_conn_obj_ref[0].last_activity = self.loop.time()


        async def managed_on_open():
            if peer_conn_obj_ref[0]:
                peer_conn_obj_ref[0].on_open_event.set()

        async def managed_on_close():
            logger.debug(f"Peers: WebRTCHandler closed for proxy (ID may not be set yet), signaling PeerConnection.")
            if peer_conn_obj_ref[0]:
                # This indicates the underlying WebRTC connection is gone.
                # The PeerConnection object itself might still exist until SnowflakeConn handles it.
                peer_conn_obj_ref[0].is_connected = False
                peer_conn_obj_ref[0].on_close_event.set()
                # Remove from pool if it was there
                async with self._lock:
                    if peer_conn_obj_ref[0] in self.pool:
                        self.pool.remove(peer_conn_obj_ref[0])
                        logger.info(f"Peers: Removed {peer_conn_obj_ref[0].proxy_id} from pool due to WebRTC close.")


        web_rtc_handler = WebRTCHandler(
            ice_servers=self.config.ice_servers,
            on_open=managed_on_open,
            on_message=managed_on_message, # This is now the KCP input pipe
            on_close=managed_on_close,
            loop=self.loop
        )
        # Ensure WebRTCHandler is set to handle binary data if possible
        # (This is usually default for aiortc if not specified otherwise)

        peer_conn = PeerConnection(web_rtc_handler, proxy_id="pending", loop=self.loop)
        peer_conn_obj_ref[0] = peer_conn # Allow callbacks to access the peer_conn instance
        return peer_conn


    async def collect_one(self) -> Optional[PeerConnection]:
        if self._stopped: return None
        logger.info("Peers: Attempting to collect a new Snowflake...")
        peer_conn_obj = self._create_new_peer_connection_obj()

        try:
            await peer_conn_obj.establish_connection(self.rendezvous_method, self.client_nat_type)
            async with self._lock: # Protect self.pool
                if len(self.pool) < self.config.max_peers: # Check if still needed / space
                    self.pool.append(peer_conn_obj) # Add to list of active/connecting peers
                    await self._available_peers_queue.put(peer_conn_obj)
                    logger.info(f"Peers: New Snowflake proxy {peer_conn_obj.proxy_id} collected. Pool size: {len(self.pool)}, Queue size: {self._available_peers_queue.qsize()}")
                    return peer_conn_obj
                else: # Pool filled up while we were collecting this one
                    logger.info(f"Peers: Pool full. Discarding new peer {peer_conn_obj.proxy_id}.")
                    await peer_conn_obj.close()
                    return None
        except Exception as e:
            logger.warning(f"Peers: Failed to collect a Snowflake: {e}")
            if not peer_conn_obj.on_close_event.is_set():
                 await peer_conn_obj.web_rtc_handler.close()
            return None

    async def connect_loop(self):
        logger.info("Peers: Starting connect_loop.")
        while not self._stopped:
            try:
                # How many peers are currently usable or in queue?
                # qsize is approximate. A more robust check might involve pool + queue.
                # For now, consider number of active connections in pool.
                # This logic needs to be robust for RedialPacketConn.
                # We want `max_peers` available in the queue if possible.

                num_in_pool_or_queue = len(self.pool) # Simplified: just consider pool size

                if self._available_peers_queue.qsize() < self.config.max_peers : # Try to keep queue full
                    logger.info(f"Peers: Connect loop: queue_size={self._available_peers_queue.qsize()}, max_peers={self.config.max_peers}. Need more.")
                    await self.collect_one()
                else:
                    logger.debug("Peers: Connect loop - available peers queue is full.")

                # Cleanup dead peers (those whose on_close_event is set)
                # This is now partially handled by managed_on_close callback.
                # A periodic sweep might still be good.
                async with self._lock:
                    self.pool = [p for p in self.pool if not p.on_close_event.is_set()]

                await asyncio.sleep(1) # Check periodically
            except asyncio.CancelledError:
                logger.info("Peers: Connect_loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Peers: Error in connect_loop: {e}", exc_info=True)
                await asyncio.sleep(RECONNECT_TIMEOUT)

    async def pop(self) -> PeerConnection:
        if self._stopped and self._available_peers_queue.empty():
            raise ConnectionAbortedError("Peers manager stopped and no available peers.")

        logger.info("Peers: Waiting for an available Snowflake peer...")
        peer = await self._available_peers_queue.get()
        self._available_peers_queue.task_done()

        # Peer is no longer "available in queue", but it's still in self.pool until it's truly closed.
        # The KCP layer will use it. If it dies, KCP will ask for another one.

        if peer.on_close_event.is_set(): # Popped a dead peer
            logger.warning(f"Peers: Popped peer {peer.proxy_id} is already closed. Attempting to get another.")
            # No need to explicitly remove from pool, managed_on_close or sweep should handle it.
            return await self.pop()

        logger.info(f"Peers: Successfully popped peer {peer.proxy_id} for use.")
        return peer

    def set_kcp_input_callback(self, callback: Callable[[bytes], Awaitable[None]]):
        self.kcp_input_callback = callback

    def start(self):
        if not self._collect_task:
            self._stopped = False
            self._collect_task = self.loop.create_task(self.connect_loop())
            logger.info("Peers: Manager started.")

    async def stop(self):
        # ... (similar to before, ensure all peers in pool are closed)
        if self._stopped: return
        self._stopped = True
        logger.info("Peers: Stopping manager...")
        if self._collect_task:
            self._collect_task.cancel()
            try: await self._collect_task
            except asyncio.CancelledError: logger.info("Peers: Collect_loop task successfully cancelled.")
            self._collect_task = None

        logger.info("Peers: Closing all connections in pool...")
        async with self._lock:
            for peer in list(self.pool): # Iterate copy
                await peer.close()
            self.pool.clear()
        while not self._available_peers_queue.empty():
            try:
                peer = self._available_peers_queue.get_nowait()
                await peer.close()
            except asyncio.QueueEmpty: break
        logger.info("Peers: Manager stopped.")


class SnowflakeConn:
    def __init__(self, config: ClientConfig, rendezvous: RendezvousMethod, loop: asyncio.AbstractEventLoop):
        self.config = config
        self.rendezvous = rendezvous
        self.loop = loop
        self.peers_manager: Optional[Peers] = None
        self.kcp_handler: Optional[KCPHandler] = None
        self.smux_session: Optional[SmuxSession] = None

        self._current_peer_conn: Optional[PeerConnection] = None
        self._is_closed = False
        self._lock = asyncio.Lock() # To protect _current_peer_conn changes
        self._redial_task: Optional[asyncio.Task] = None

    async def _kcp_output_via_webrtc(self, data: bytes):
        async with self._lock:
            peer = self._current_peer_conn

        if peer and peer.is_connected:
            try:
                await peer.send_on_datachannel(data)
                return
            except ConnectionError as e:
                logger.warning(f"SnowflakeConn: KCP output failed on current peer {peer.proxy_id}: {e}. Will try to redial.")
                # Mark this peer as problematic / signal for redial
                # The redial loop should pick this up.
                if not peer.on_close_event.is_set():
                    await peer.close() # Ensure it's marked as closed
            except Exception as e:
                logger.error(f"SnowflakeConn: Unexpected error in KCP output via {peer.proxy_id}: {e}")
                if not peer.on_close_event.is_set():
                    await peer.close()
        else:
            logger.warning("SnowflakeConn: KCP output attempt but no active peer or peer not connected.")

        # If send failed or no peer, KCP will buffer and retry. Redial loop should provide a new peer.
        # We might need to explicitly trigger redial or rely on its periodic check.
        if self._redial_task and not self._redial_task.done():
             # Nudge the redial task if it's sleeping, or it will pick up on its own.
             # This can be done by signalling an event that the redial task waits on,
             # in addition to its timer. For now, rely on redial_loop's timing.
             pass
        raise ConnectionError("KCP output failed: No usable WebRTC peer.")


    async def _manage_underlying_connection(self):
        """Ensures KCP always has a WebRTC connection, redialing if necessary."""
        while not self._is_closed:
            peer_ok = False
            async with self._lock:
                if self._current_peer_conn and \
                   self._current_peer_conn.is_connected and \
                   not self._current_peer_conn.on_close_event.is_set():
                    peer_ok = True

            if peer_ok:
                await asyncio.sleep(1) # Check periodically
                continue

            logger.info("SnowflakeConn: No active WebRTC peer or peer lost. Attempting to get a new one.")
            if not self.peers_manager: # Should not happen if dial was called
                logger.error("SnowflakeConn: Peers manager not available for redial.")
                await asyncio.sleep(RECONNECT_TIMEOUT)
                continue

            try:
                new_peer = await self.peers_manager.pop() # This blocks until a peer is available
                if self._is_closed: # Check if session closed while waiting for peer
                    if new_peer: await new_peer.close()
                    break

                logger.info(f"SnowflakeConn: Acquired new peer {new_peer.proxy_id} for KCP.")
                async with self._lock:
                    # Close old peer if any and not already closed by its callback
                    if self._current_peer_conn and not self._current_peer_conn.on_close_event.is_set():
                        await self._current_peer_conn.close()
                    self._current_peer_conn = new_peer

                # KCP's output callback (_kcp_output_via_webrtc) will now use this new peer.
                # KCP's input callback (peers_manager.kcp_input_callback) is set once and
                # WebRTCHandler instances call it directly.
                logger.info(f"SnowflakeConn: Switched to new peer {new_peer.proxy_id}.")

            except ConnectionAbortedError: # Peers manager stopped
                logger.info("SnowflakeConn: Peers manager stopped, cannot redial.")
                break
            except Exception as e:
                logger.error(f"SnowflakeConn: Error during redial: {e}. Retrying after timeout.")
                await asyncio.sleep(RECONNECT_TIMEOUT)
        logger.info("SnowflakeConn: Underlying connection management loop stopped.")


    async def dial(self) -> None: # Changed: does not return a stream directly
        if self._is_closed: raise ConnectionError("Cannot dial on a closed SnowflakeConn.")
        if self.smux_session and not self.smux_session.is_closed():
            logger.info("SnowflakeConn: Dial called but session already active.")
            return

        self.peers_manager = Peers(self.config, self.rendezvous, self.loop)
        self.peers_manager.start()

        # Generate a conversation ID for KCP. Max value for uint32.
        conv_id = random.randint(0, UINT32_MAX)
        self.kcp_handler = KCPHandler(conv_id, self._kcp_output_via_webrtc, loop=self.loop)

        # Set the KCP input callback in Peers manager
        # This callback is called by WebRTCHandler's on_message
        if self.peers_manager: # Should exist
            self.peers_manager.set_kcp_input_callback(self.kcp_handler.handle_incoming_packet)

        self.smux_session = SmuxSession(self.kcp_handler, is_client=True, loop=self.loop,
                                        max_stream_buffer=self.config.smux_max_stream_buffer,
                                        keep_alive_interval=self.config.smux_keep_alive_interval,
                                        max_frame_size=self.config.smux_max_frame_size) # Pass from config

        # Start the redial task to ensure KCP always has a peer
        self._redial_task = self.loop.create_task(self._manage_underlying_connection())

        # KCP and SMUX start their internal loops
        self.kcp_handler.start() # Applies settings, starts KCP update loop
        self.smux_session.start() # Starts SMUX receiver and keep-alive loops

        logger.info(f"SnowflakeConn: Dialed. KCP ConvID={conv_id}. SMUX session started.")
        # The SOCKS handler will call open_smux_stream() to get individual streams.

    async def open_smux_stream(self) -> SmuxStream:
        if self._is_closed or not self.smux_session or self.smux_session.is_closed():
            raise ConnectionError("SnowflakeConn is not active or SMUX session closed.")
        if not self.kcp_handler or not self.kcp_handler.is_connected():
            # Wait for KCP to become connected (i.e., underlying peer available)
            # This might require a KCP-level connected event or check.
            # For now, assume if smux_session is up, KCP is trying.
            logger.info("SnowflakeConn: KCP not yet connected, open_smux_stream may block or fail if no peer quickly.")

        try:
            stream = await self.smux_session.open_stream()
            logger.info(f"SnowflakeConn: Opened SMUX stream {stream.stream_id}")
            return stream
        except Exception as e:
            logger.error(f"SnowflakeConn: Failed to open SMUX stream: {e}")
            # Consider closing SnowflakeConn if SMUX errors are persistent
            await self.close() # Make it fatal for now
            raise ConnectionError("Failed to open SMUX stream") from e


    async def close(self) -> None:
        if self._is_closed: return
        self._is_closed = True
        logger.info("SnowflakeConn: Closing connection...")

        if self._redial_task:
            self._redial_task.cancel()
            try: await self._redial_task
            except asyncio.CancelledError: pass
            self._redial_task = None

        if self.smux_session:
            await self.smux_session.close()
            self.smux_session = None
        logger.debug("SnowflakeConn: SMUX session closed.")

        if self.kcp_handler:
            await self.kcp_handler.close()
            self.kcp_handler = None
        logger.debug("SnowflakeConn: KCP handler closed.")

        # Current peer conn is closed by redial task or here if redial task already exited
        async with self._lock:
            if self._current_peer_conn and not self._current_peer_conn.on_close_event.is_set():
                await self._current_peer_conn.close()
            self._current_peer_conn = None
        logger.debug("SnowflakeConn: Current peer connection closed.")

        if self.peers_manager:
            await self.peers_manager.stop()
            self.peers_manager = None
        logger.info("SnowflakeConn: Peers manager stopped.")
        logger.info("SnowflakeConn: Connection fully closed.")


# --- Example Usage (Update if needed, but main test is via SOCKS) ---
# (Example usage can be simplified or removed as SOCKS proxy is the primary user)
# ...
