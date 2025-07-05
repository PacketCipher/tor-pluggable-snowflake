"""
Minimal SMUX (Stream Multiplexing) Implementation for Snowflake Python Client.
Aims for wire compatibility with xtaci/smux (Version 2) as used by the Go Snowflake client.
"""

import asyncio
import logging
import struct
import time
from collections import deque
from enum import IntEnum
from typing import Callable, Awaitable, Dict, Optional, Deque, Any

# Assuming kcp_handler.KCPHandler provides send_data and receive_data methods
from .kcp_handler import KCPHandler

logger = logging.getLogger(__name__)

# SMUX constants from xtaci/smux
SMUX_VERSION = 2
SMUX_HEADER_SIZE = 8 # ver(1), cmd(1), length(2), sid(4)

class SmuxCmd(IntEnum):
    SYN = 0  # New stream
    FIN = 1  # Close stream (half-close)
    PSH = 2  # Data push
    NOP = 3  # No operation (keep-alive)
    UPD = 4  # Window update (for flow control credits)

# Default config values from xtaci/smux SessionConfig
SMUX_DEFAULT_KEEP_ALIVE_INTERVAL = 10 # seconds
# Go client sets session.Config.KeepAliveTimeout = 10 * time.Minute
# which sets the interval to timeout / 4 = 2.5 minutes (150s).
# PYTHON_SMUX_KEEP_ALIVE_INTERVAL is now set from ClientConfig.

PYTHON_SMUX_STREAM_TIMEOUT = 120 # seconds for inactivity on a stream (local setting)

SMUX_DEFAULT_MAX_STREAM_BUFFER = 65536 # Default per-stream buffer from xtaci/smux
                                       # Go client sets this to 1MB (1048576) via ClientConfig.smux_max_stream_buffer

SMUX_DEFAULT_MAX_FRAME_SIZE = 32768    # Default full frame size from xtaci/smux
SMUX_MAX_PAYLOAD_SIZE = SMUX_DEFAULT_MAX_FRAME_SIZE - SMUX_HEADER_SIZE # Derived max payload


class SmuxStream:
    def __init__(self, stream_id: int, session: 'SmuxSession', initial_recv_window: int):
        self.stream_id = stream_id
        self.session = session
        self._loop = session._loop

        self._recv_buffer: Deque[bytes] = deque()
        self._recv_event = asyncio.Event() # Set when new data arrives or EOF

        self.current_recv_window = initial_recv_window # Our capacity to receive
        self.max_recv_window = initial_recv_window     # Max capacity (from config)
        self.remote_send_window: int = 0 # How much remote can send (their rcv_wnd for this stream)
                                        # This is updated by UPD frames from remote.
                                        # For sending, we mainly care that remote has space.

        self._local_fin_sent = False    # We sent FIN
        self._remote_fin_rcvd = False   # We received FIN from remote
        self._closed_event = asyncio.Event() # Set when stream is fully closed

        self._write_lock = asyncio.Lock()
        self.last_active_time = self._loop.time()

        logger.debug(f"SmuxStream {self.stream_id}: Created. InitialRecvWindow: {initial_recv_window}")

    def _update_activity(self):
        self.last_active_time = self._loop.time()

    async def read(self, n: int = -1) -> bytes:
        self._update_activity()
        if self._remote_fin_rcvd and not self._recv_buffer:
            return b"" # EOF

        while not self._recv_buffer and not self._remote_fin_rcvd:
            try:
                await asyncio.wait_for(self._recv_event.wait(), timeout=PYTHON_SMUX_STREAM_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(f"SmuxStream {self.stream_id}: Read timeout.")
                # Consider this an error, close stream?
                await self.session._close_stream_internal(self.stream_id, error="read timeout")
                raise ConnectionAbortedError(f"Stream {self.stream_id} read timeout")

            self._recv_event.clear()
            if self._remote_fin_rcvd and not self._recv_buffer: # Check again after event
                return b""

        if not self._recv_buffer: # Should only happen if remote_fin_rcvd was set while waiting
            return b""

        # Collect data
        if n == -1: # Read all available
            data_parts = list(self._recv_buffer)
            self._recv_buffer.clear()
            data = b"".join(data_parts)
        else:
            data_parts = []
            bytes_to_read = n
            while bytes_to_read > 0 and self._recv_buffer:
                front_chunk = self._recv_buffer.popleft()
                if len(front_chunk) <= bytes_to_read:
                    data_parts.append(front_chunk)
                    bytes_to_read -= len(front_chunk)
                else: # front_chunk is larger than needed
                    data_parts.append(front_chunk[:bytes_to_read])
                    self._recv_buffer.appendleft(front_chunk[bytes_to_read:]) # Put back remainder
                    bytes_to_read = 0
            data = b"".join(data_parts)

        # Send window update to peer for bytes we've consumed from buffer
        consumed_bytes = len(data)
        if consumed_bytes > 0:
            self.current_recv_window += consumed_bytes # We made space
            # Send UPD frame to inform peer they can send more
            await self.session._send_window_update(self.stream_id, consumed_bytes)
            logger.debug(f"SmuxStream {self.stream_id}: Read {consumed_bytes} bytes. Sent UPD. New local recv_wnd: {self.current_recv_window}")

        return data

    async def write(self, data: bytes) -> None:
        self._update_activity()
        if self._local_fin_sent:
            raise ConnectionAbortedError(f"Stream {self.stream_id} is closing (FIN sent). Cannot write.")
        if self._remote_fin_rcvd: # Technically could still write if peer only half-closed, but common to stop.
            logger.warning(f"SmuxStream {self.stream_id}: Writing to stream after remote FIN received.")
            # raise ConnectionAbortedError(f"Stream {self.stream_id} peer closed (FIN received).")

        if not data:
            return

        async with self._write_lock:
            offset = 0
            while offset < len(data):
                # TODO: Respect remote_send_window (flow control from peer)
                # This is not fully implemented in this minimal version.
                # `xtaci/smux` sends data and remote buffers it or applies flow control.
                # The UPD frame is about receiver telling sender it has space.
                # We need to wait if self.remote_send_window (peer's rcv_wnd for this stream) is too small.
                # This requires UPD frames from peer to update self.remote_send_window.
                # For now, we send aggressively.

                # Respect MaxFrameSize (which dictates max payload size)
                max_payload_for_frame = self.session.max_frame_size - SMUX_HEADER_SIZE
                chunk_size = min(len(data) - offset, max_payload_for_frame)
                chunk = data[offset : offset + chunk_size]

                await self.session._send_frame(SmuxCmd.PSH, self.stream_id, chunk)
                logger.debug(f"SmuxStream {self.stream_id}: Sent PSH frame with {len(chunk)} bytes.")
                offset += chunk_size

    async def close(self) -> None: # Initiates half-close from our side
        self._update_activity()
        if self._local_fin_sent:
            logger.debug(f"SmuxStream {self.stream_id}: Already called close (FIN sent).")
            # Wait for full closure if not already fully closed
            await self._closed_event.wait()
            return

        logger.info(f"SmuxStream {self.stream_id}: Closing (sending FIN).")
        self._local_fin_sent = True
        try:
            await self.session._send_frame(SmuxCmd.FIN, self.stream_id, b'')
        except Exception as e:
            logger.error(f"SmuxStream {self.stream_id}: Error sending FIN: {e}")
            # Mark as fully closed on error to avoid hangs
            self._remote_fin_rcvd = True # Assume worst case
            self._closed_event.set()
            self.session._remove_stream(self.stream_id) # Ensure cleanup
            return

        if self._remote_fin_rcvd: # If peer already sent FIN
            logger.info(f"SmuxStream {self.stream_id}: FIN sent, FIN already received. Stream fully closed.")
            self._closed_event.set()
            self.session._remove_stream(self.stream_id)
        else:
            # Wait for peer's FIN or timeout
            logger.debug(f"SmuxStream {self.stream_id}: FIN sent, waiting for peer's FIN or timeout.")
            # The session's main loop will handle incoming FIN and call _handle_remote_fin.
            # We can wait on _closed_event here.
            try:
                await asyncio.wait_for(self._closed_event.wait(), timeout=PYTHON_SMUX_STREAM_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(f"SmuxStream {self.stream_id}: Timeout waiting for remote FIN after sending local FIN.")
                # Force close
                self._remote_fin_rcvd = True # Assume it's gone
                self._closed_event.set()
                self.session._remove_stream(self.stream_id)


    def _receive_data(self, data: bytes):
        self._update_activity()
        if self._remote_fin_rcvd:
            logger.warning(f"SmuxStream {self.stream_id}: Data received after remote FIN. Discarding.")
            return

        if len(data) > self.current_recv_window:
            logger.error(f"SmuxStream {self.stream_id}: Received data ({len(data)} bytes) exceeds current receive window ({self.current_recv_window} bytes). Protocol error or window mismanagement.")
            # This indicates a problem. Either peer is not respecting window, or our accounting is off.
            # For robustness, we might still buffer it if overall limit not hit, or close stream.
            # For now, log and accept.
            pass # Continue to buffer

        self._recv_buffer.append(data)
        self.current_recv_window -= len(data)
        self._recv_event.set()
        logger.debug(f"SmuxStream {self.stream_id}: Buffered {len(data)} bytes. New local recv_wnd: {self.current_recv_window}")


    def _handle_remote_fin(self):
        self._update_activity()
        if self._remote_fin_rcvd:
            logger.debug(f"SmuxStream {self.stream_id}: Duplicate remote FIN received.")
            return

        logger.info(f"SmuxStream {self.stream_id}: Remote FIN received.")
        self._remote_fin_rcvd = True
        self._recv_event.set() # Wake up any readers so they can see EOF

        if self._local_fin_sent: # If we also sent FIN
            logger.info(f"SmuxStream {self.stream_id}: FIN received, FIN already sent. Stream fully closed.")
            self._closed_event.set()
            self.session._remove_stream(self.stream_id)
        # If local FIN not sent, stream is now half-closed (remote side). We can still write.

    def _handle_remote_window_update(self, new_credits: int):
        self._update_activity()
        # This means the remote peer has processed `new_credits` bytes we sent,
        # and is granting us `new_credits` more to send.
        # `xtaci/smux` UPD payload is (num_credits uint32), which is how many more bytes sender can send.
        self.remote_send_window += new_credits # This seems to be the interpretation for xtaci/smux
        logger.debug(f"SmuxStream {self.stream_id}: Remote window updated by {new_credits}. New remote_send_wnd: {self.remote_send_window}")
        # TODO: If there are writers waiting on window, notify them.

    def is_fully_closed(self) -> bool:
        return self._local_fin_sent and self._remote_fin_rcvd

    def _force_close(self, reason="forced"):
        logger.warning(f"SmuxStream {self.stream_id}: Force closing due to: {reason}")
        self._local_fin_sent = True
        self._remote_fin_rcvd = True
        self._recv_event.set() # Unblock readers
        self._closed_event.set() # Unblock anyone waiting on close
        # No need to call session._remove_stream here, as session loop will do it or it's already done.

class SmuxSession:
    def __init__(self, kcp_handler: KCPHandler, is_client: bool,
                 loop: Optional[asyncio.AbstractEventLoop] = None,
                 max_stream_buffer: int = SMUX_DEFAULT_MAX_STREAM_BUFFER, # Per-stream recv window
                 keep_alive_interval: int = 150, # Defaulted to 2.5 minutes (150s) to match Go client
                 max_frame_size: int = SMUX_DEFAULT_MAX_FRAME_SIZE
                 ):

        self._kcp_handler = kcp_handler
        self._is_client = is_client
        self._loop = loop if loop else asyncio.get_event_loop()

        self._streams: Dict[int, SmuxStream] = {}
        self._next_stream_id: int = 1 if is_client else 2

        self.version = SMUX_VERSION
        self.max_stream_buffer_size = max_stream_buffer
        self.keep_alive_interval = keep_alive_interval
        self.max_frame_size = max_frame_size # Max full frame size (header + payload)
        self.last_peer_activity_time = self._loop.time()

        self._session_closed = False
        self._session_closed_event = asyncio.Event()
        self._smux_receiver_task: Optional[asyncio.Task] = None
        self._keep_alive_task: Optional[asyncio.Task] = None

        logger.info(f"SmuxSession created. Client: {is_client}. KCP ConvID: {kcp_handler.kcp_conn.get_conv_id()}. MaxStreamBuffer: {max_stream_buffer}")

    def _update_peer_activity(self):
        self.last_peer_activity_time = self._loop.time()

    async def _send_frame(self, cmd: SmuxCmd, stream_id: int, payload: bytes):
        if self._session_closed:
            raise ConnectionAbortedError("SMUX session is closed.")

        if len(payload) > SMUX_MAX_PAYLOAD_SIZE:
            # This should be handled by SmuxStream.write splitting logic
            raise ValueError(f"SMUX payload too large: {len(payload)} bytes")

        header = bytearray(SMUX_HEADER_SIZE)
        struct.pack_into('!B', header, 0, self.version)      # Version
        struct.pack_into('!B', header, 1, cmd.value)         # Command
        struct.pack_into('!H', header, 2, len(payload))      # Length of payload
        struct.pack_into('!I', header, 4, stream_id)         # Stream ID

        frame = bytes(header) + payload
        try:
            await self._kcp_handler.send_data(frame)
        except Exception as e:
            logger.error(f"SmuxSession: Error sending frame via KCP: {e}")
            await self.close(error="KCP send error") # Close session on KCP error
            raise ConnectionAbortedError("KCP send error") from e


    async def _smux_receiver_loop(self):
        logger.info("SmuxSession: Receiver loop started.")
        while not self._session_closed:
            try:
                if not self._kcp_handler.is_connected():
                    logger.warning("SmuxSession: KCP connection lost. Shutting down SMUX session.")
                    await self.close(error="KCP disconnected")
                    break

                frame_data = await self._kcp_handler.receive_data()
                if frame_data is None: # KCP might return None if it's closing or no data
                    if not self._kcp_handler.is_connected() and not self._session_closed :
                        logger.warning("SmuxSession: KCP connection closed while trying to receive. Shutting down.")
                        await self.close(error="KCP closed")
                    # If KCP is connected but returns None, could be a temporary state or signal to yield.
                    await asyncio.sleep(0.01) # Small sleep if KCP returns None but is connected
                    continue

                self._update_peer_activity()
                await self._handle_incoming_frame(frame_data)

            except ConnectionAbortedError as e:
                logger.warning(f"SmuxSession: Connection aborted in receiver loop: {e}")
                await self.close(error=str(e))
                break
            except asyncio.CancelledError:
                logger.info("SmuxSession: Receiver loop cancelled.")
                break
            except Exception as e:
                logger.error(f"SmuxSession: Unexpected error in receiver loop: {e}", exc_info=True)
                await self.close(error=f"Receiver loop error: {e}")
                break
        logger.info("SmuxSession: Receiver loop stopped.")
        self._session_closed_event.set() # Signal that session processing is done


    async def _handle_incoming_frame(self, frame_data: bytes):
        if len(frame_data) < SMUX_HEADER_SIZE:
            logger.warning(f"SmuxSession: Received runt frame, len {len(frame_data)}. Discarding.")
            return

        ver = frame_data[0]
        cmd_val = frame_data[1]
        length = struct.unpack_from('!H', frame_data, 2)[0]
        stream_id = struct.unpack_from('!I', frame_data, 4)[0]

        payload_offset = SMUX_HEADER_SIZE
        if len(frame_data) < payload_offset + length:
            logger.warning(f"SmuxSession: Frame payload shorter than specified length. Expected {length}, got {len(frame_data) - payload_offset}. Discarding.")
            return

        payload = frame_data[payload_offset : payload_offset + length]

        if ver != self.version:
            logger.error(f"SmuxSession: Received frame with incompatible version {ver}. Expected {self.version}. Closing session.")
            await self.close(error="Incompatible SMUX version")
            return

        try:
            cmd = SmuxCmd(cmd_val)
        except ValueError:
            logger.warning(f"SmuxSession: Received unknown command {cmd_val} for stream {stream_id}. Discarding.")
            return

        logger.debug(f"SmuxSession < Frame: SID={stream_id}, CMD={cmd.name}, Len={length}, Payload='{payload[:20]}...'")

        stream = self._streams.get(stream_id)

        if cmd == SmuxCmd.SYN:
            if stream:
                logger.warning(f"SmuxSession: Received SYN for existing stream {stream_id}. Ignoring.")
                # TODO: xtaci/smux sends RST here. We don't have RST, so just ignore for now.
            elif self._is_client:
                 logger.warning(f"SmuxSession: Client received SYN for stream {stream_id}. Server should not initiate streams with odd IDs to client. Ignoring.")
                 # Or if stream_id % 2 == 0 (even): Client received SYN for a server-initiated stream ID.
                 # Standard is client uses odd, server uses even. If client gets SYN for even ID, it's valid.
                 # For now, this minimal client only opens streams, doesn't accept.
                 # So any SYN is unexpected if it's not for a stream we're waiting to be acked (not a feature here).
            else: # We are server, and received SYN from client
                # This path is not used by Snowflake client, but for completeness:
                logger.info(f"SmuxSession (Server): Received SYN for new stream {stream_id}. MaxBuf: {self.max_stream_buffer_size}")
                new_stream = SmuxStream(stream_id, self, self.max_stream_buffer_size)
                self._streams[stream_id] = new_stream
                # Application layer would need to accept this stream. (e.g. via self.accept_stream())
                # For now, just log.
                if hasattr(self, '_pending_accepts_queue'): # If accept_stream() is implemented
                    self._pending_accepts_queue.put_nowait(new_stream)

        elif cmd == SmuxCmd.FIN:
            if stream:
                stream._handle_remote_fin()
            else:
                logger.warning(f"SmuxSession: FIN for unknown/closed stream {stream_id}.")

        elif cmd == SmuxCmd.PSH:
            if stream:
                if stream._remote_fin_rcvd:
                     logger.warning(f"SmuxSession: PSH for stream {stream_id} after remote FIN. Discarding.")
                else:
                    stream._receive_data(payload)
            else:
                logger.warning(f"SmuxSession: PSH for unknown/closed stream {stream_id}. Discarding.")
                # TODO: Send RST for stream in full implementation.

        elif cmd == SmuxCmd.UPD:
            # This is for flow control. Payload: (uint32 num_credits, uint32 num_bytes_consumed_by_ack)
            # xtaci/smux v2 UPD payload is just (credits uint32), where credits is how many more bytes peer can send.
            if stream:
                if len(payload) == 4: # expecting uint32 for credits
                    credits = struct.unpack('!I', payload)[0]
                    stream._handle_remote_window_update(credits)
                else:
                    logger.warning(f"SmuxSession: Received UPD for stream {stream_id} with invalid payload length {len(payload)}. Expected 4.")
            else:
                logger.warning(f"SmuxSession: UPD for unknown/closed stream {stream_id}.")

        elif cmd == SmuxCmd.NOP:
            logger.debug("SmuxSession: NOP received (keep-alive).")
            # No action needed other than updating last_peer_activity_time (done at frame receipt)

        else: # Should not happen due to SmuxCmd enum check
            logger.warning(f"SmuxSession: Unhandled command {cmd} for stream {stream_id}.")


    async def _keep_alive_loop(self):
        logger.info(f"SmuxSession: Keep-alive loop started. Interval: {self.keep_alive_interval}s.")
        while not self._session_closed:
            try:
                await asyncio.sleep(self.keep_alive_interval)
                if self._session_closed: break

                if self._loop.time() - self.last_peer_activity_time > self.keep_alive_interval * 2 : # Allow some grace
                     logger.warning(f"SmuxSession: Peer inactive for too long. Closing session. Last active: {self.last_peer_activity_time}")
                     await self.close(error="Peer inactivity timeout")
                     break

                if not self._session_closed: # Check again after potential close
                    logger.debug("SmuxSession: Sending NOP (keep-alive).")
                    await self._send_frame(SmuxCmd.NOP, 0, b'') # StreamID 0 for NOP

            except asyncio.CancelledError:
                logger.info("SmuxSession: Keep-alive loop cancelled.")
                break
            except Exception as e:
                logger.error(f"SmuxSession: Error in keep-alive loop: {e}", exc_info=True)
                # Don't necessarily close session for keep-alive error, unless it's a send error.
                if isinstance(e, ConnectionAbortedError): # From _send_frame
                    break # Session already closing
        logger.info("SmuxSession: Keep-alive loop stopped.")


    def start(self):
        if self._smux_receiver_task is not None:
            logger.warning("SmuxSession: Start called but already started.")
            return

        self._session_closed = False
        self._kcp_handler.start() # Ensure KCP is started

        self._smux_receiver_task = self._loop.create_task(self._smux_receiver_loop())
        if self.keep_alive_interval > 0:
            self._keep_alive_task = self._loop.create_task(self._keep_alive_loop())
        logger.info("SmuxSession: Started.")


    async def open_stream(self) -> SmuxStream:
        if self._session_closed:
            raise ConnectionAbortedError("SMUX session is closed.")
        if not self._is_client:
            # Server typically uses accept_stream, but can also open.
            # Ensure stream ID parity is correct if server opens.
            # For now, this is primarily a client implementation.
            pass

        stream_id = self._next_stream_id
        self._next_stream_id += 2 # Client uses odd IDs, server uses even. Increment by 2.

        # Initial remote window for sending is unknown until first UPD.
        # xtaci/smux sets initial remote window to default (262144) or from config.
        # For now, let's be optimistic or rely on buffering.
        # The stream's `current_recv_window` is *our* capacity.
        stream = SmuxStream(stream_id, self, self.max_stream_buffer_size)
        self._streams[stream_id] = stream

        try:
            await self._send_frame(SmuxCmd.SYN, stream_id, b'')
            logger.info(f"SmuxSession: Opened new stream {stream_id} (SYN sent).")
        except Exception as e:
            logger.error(f"SmuxSession: Failed to send SYN for stream {stream_id}: {e}")
            self._remove_stream(stream_id) # Clean up
            raise ConnectionAbortedError(f"Failed to send SYN for stream {stream_id}") from e

        return stream

    def _remove_stream(self, stream_id: int):
        if stream_id in self._streams:
            # stream = self._streams.pop(stream_id) # Ensure it's removed
            # stream._force_close("removed from session") # Ensure stream knows it's dead
            # logger.info(f"SmuxSession: Stream {stream_id} removed from active streams.")
            # Do not force close here, let stream manage its state. Just pop.
            if self._streams.pop(stream_id, None):
                 logger.info(f"SmuxSession: Stream {stream_id} removed from active streams.")

    async def _close_stream_internal(self, stream_id: int, error: Optional[str] = None):
        stream = self._streams.get(stream_id)
        if stream:
            if error:
                logger.warning(f"SmuxSession: Closing stream {stream_id} due to error: {error}")
                stream._force_close(reason=error)
            elif not stream.is_fully_closed():
                # Graceful close if not already closing due to error
                # This might be called if session is closing globally
                try:
                    await stream.close() # Attempt graceful FIN exchange
                except Exception as e:
                    logger.error(f"SmuxSession: Error during internal close of stream {stream_id}: {e}")
                    stream._force_close(reason=f"internal close error: {e}")

            self._remove_stream(stream_id) # Ensure it's removed after close attempt

    async def _send_window_update(self, stream_id: int, num_bytes_consumed: int):
        # Inform peer that we have processed num_bytes_consumed and they can send more.
        # Payload for UPD is (uint32 credits_granted)
        if num_bytes_consumed <= 0: return

        payload = struct.pack('!I', num_bytes_consumed)
        try:
            await self._send_frame(SmuxCmd.UPD, stream_id, payload)
            logger.debug(f"SmuxSession: Sent UPD for SID={stream_id}, credits={num_bytes_consumed}")
        except Exception as e:
            logger.error(f"SmuxSession: Failed to send UPD for stream {stream_id}: {e}")
            # This could lead to deadlock if peer stops sending.
            # Consider closing session if UPDs fail consistently.

    async def close(self, error: Optional[str] = None):
        if self._session_closed:
            await self._session_closed_event.wait() # Wait for full shutdown if already closing
            return

        self._session_closed = True # Mark immediately to stop new operations
        if error:
            logger.error(f"SmuxSession: Closing due to error: {error}")
        else:
            logger.info("SmuxSession: Closing session gracefully.")

        # Cancel keep-alive task first
        if self._keep_alive_task:
            self._keep_alive_task.cancel()
            try:
                await self._keep_alive_task
            except asyncio.CancelledError: pass
            self._keep_alive_task = None

        # Close all active streams
        # Iterate over a copy of keys as _close_stream_internal might modify self._streams
        active_stream_ids = list(self._streams.keys())
        for stream_id in active_stream_ids:
            await self._close_stream_internal(stream_id, error=error if error else "session closing")
        self._streams.clear()

        # Cancel the main receiver task
        if self._smux_receiver_task:
            self._smux_receiver_task.cancel()
            try:
                await self._smux_receiver_task # Allow it to process cancellation
            except asyncio.CancelledError: pass # Expected
            self._smux_receiver_task = None

        # Underlying KCP connection should be closed by the owner of SmuxSession
        # (e.g. SnowflakeClient or equivalent)
        # Here, we just ensure our tasks are stopped.
        # If KCP is not connected, set event quickly.
        if not self._kcp_handler.is_connected():
            self._session_closed_event.set()
        else:
            # If KCP still connected, wait for receiver loop to confirm shutdown
            try:
                await asyncio.wait_for(self._session_closed_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("SmuxSession: Timeout waiting for receiver loop to stop during close.")

        logger.info("SmuxSession: Session fully closed.")

    def is_closed(self) -> bool:
        return self._session_closed and self._session_closed_event.is_set()


# Example usage (for testing standalone SMUX)
async def main_smux_test(): # pragma: no cover
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    loop = asyncio.get_event_loop()

    # Mock KCP Handler
    class MockKCPHandler:
        def __init__(self, name: str, peer_queue: asyncio.Queue, my_queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
            self.name = name
            self.peer_queue = peer_queue # Queue to send to peer
            self.my_queue = my_queue     # Queue to receive from peer
            self.loop = loop
            self._connected = True
            self.kcp_conn = self # Mocking KCPConn attributes needed by SmuxSession
            self.conv_id_counter = 1000

        def get_conv_id(self) -> int: # Mock for SmuxSession constructor
            self.conv_id_counter +=1
            return self.conv_id_counter

        async def send_data(self, data: bytes):
            if not self._connected: raise ConnectionAbortedError("MockKCP closed")
            logger.debug(f"MockKCP ({self.name}): Sending {len(data)} bytes.")
            await self.peer_queue.put(data)

        async def receive_data(self) -> Optional[bytes]:
            if not self._connected and self.my_queue.empty():
                 raise ConnectionAbortedError("MockKCP closed and no pending data")
            try:
                # Give a chance for sender to put something if queue is empty
                data = await asyncio.wait_for(self.my_queue.get(), timeout=0.1 if self._connected else 0.01)
                if data:
                    logger.debug(f"MockKCP ({self.name}): Received {len(data)} bytes.")
                    return data
            except asyncio.TimeoutError:
                pass # No data currently

            if not self._connected and self.my_queue.empty(): # Check again after timeout
                 # This ensures that if KCP is marked disconnected AND queue is empty, we signal closure
                 return None # Or raise ConnectionAbortedError, depending on KCPHandler's contract
            return None

        def start(self): logger.debug(f"MockKCP ({self.name}): Started.")
        async def close(self):
            logger.debug(f"MockKCP ({self.name}): Closed.")
            self._connected = False
        def is_connected(self) -> bool: return self._connected

    q1_to_q2 = asyncio.Queue()
    q2_to_q1 = asyncio.Queue()

    kcp_client = MockKCPHandler("client", q1_to_q2, q2_to_q1, loop)
    kcp_server = MockKCPHandler("server", q2_to_q1, q1_to_q2, loop) # Server reads from q1_to_q2

    # Note: In this test, SmuxSession acts as both client and server logic for frame handling.
    # The `is_client` flag mainly affects initial stream ID generation.
    # A true test would have a client SmuxSession and a server SmuxSession.
    # Let's simulate client session:
    client_smux = SmuxSession(kcp_client, is_client=True, loop=loop, max_stream_buffer=1024*10) # 10KB buffer for test

    # Simulate a "server" side that just echoes on streams.
    # This is tricky without a full peer SmuxSession.
    # For this test, let's make client_smux talk to kcp_server which is just a pipe.
    # The kcp_server's receive_data will be client_smux's handle_incoming_frame.
    # This means we need a way for kcp_server to get data to client_smux's _handle_incoming_frame.
    # Let's simplify: client_smux sends to kcp_client. kcp_client puts on q1_to_q2.
    # A separate task reads from q1_to_q2 and feeds it to client_smux._handle_incoming_frame
    # (simulating it received from a peer). This isn't a true peer test.

    # Proper test requires two SmuxSession instances.
    # Let's adjust. client_smux (client) and server_smux (server).
    server_smux = SmuxSession(kcp_server, is_client=False, loop=loop, max_stream_buffer=1024*10)

    client_smux.start()
    server_smux.start() # Server starts its receiver loop

    async def server_stream_handler(stream: SmuxStream):
        logger.info(f"Server: Accepted stream {stream.stream_id}")
        try:
            while True:
                data = await stream.read(1024)
                if not data: # EOF
                    logger.info(f"Server: Stream {stream.stream_id} EOF.")
                    break
                logger.info(f"Server: Stream {stream.stream_id} received '{data.decode()}', echoing.")
                await stream.write(data) # Echo
        except ConnectionAbortedError as e:
             logger.warning(f"Server: Stream {stream.stream_id} aborted: {e}")
        except Exception as e:
            logger.error(f"Server: Stream {stream.stream_id} error: {e}", exc_info=True)
        finally:
            logger.info(f"Server: Stream {stream.stream_id} closing handler.")
            await stream.close()


    # Server needs an accept_stream method or equivalent logic.
    # SmuxSession doesn't have accept_stream, it processes SYN in _handle_incoming_frame
    # and expects application to get the stream. We'll patch it for test.

    # Monkey-patch server_smux to handle new streams for the test
    # In a real app, there would be a callback or accept queue.
    async def server_on_new_stream(self_srv_smux, stream_id, payload): # payload is empty for SYN
        if stream_id in self_srv_smux._streams: return # Already handled
        logger.info(f"Server (patched): SYN for new stream {stream_id}")
        new_stream = SmuxStream(stream_id, self_srv_smux, self_srv_smux.max_stream_buffer_size)
        self_srv_smux._streams[stream_id] = new_stream
        loop.create_task(server_stream_handler(new_stream))

    original_handle_syn = None
    def patch_server_syn_handling(srv_smux_instance):
        nonlocal original_handle_syn
        original_handle_frame = srv_smux_instance._handle_incoming_frame

        async def patched_handle_frame(frame_data_arg):
            # Basic parsing to find if it's SYN, then call our handler
            if len(frame_data_arg) >= SMUX_HEADER_SIZE:
                cmd_val = frame_data_arg[1]
                stream_id = struct.unpack_from('!I', frame_data_arg, 4)[0]
                if cmd_val == SmuxCmd.SYN.value:
                    # Call our custom SYN handler
                    await server_on_new_stream(srv_smux_instance, stream_id, b'')
                    # Mark as handled or let original also run if it does more setup
                    # For now, assume this is enough for test.
                    return # Don't call original for SYN if we handle it fully here
            # Call original for other commands or if not SYN
            await original_handle_frame(frame_data_arg)
        srv_smux_instance._handle_incoming_frame = patched_handle_frame

    patch_server_syn_handling(server_smux)


    # Client application logic
    try:
        logger.info("Client: Opening stream...")
        stream1 = await client_smux.open_stream()
        logger.info(f"Client: Opened stream {stream1.stream_id}")

        msg1 = "Hello from client stream 1"
        logger.info(f"Client: Stream {stream1.stream_id} sending '{msg1}'")
        await stream1.write(msg1.encode())

        response1 = await stream1.read(1024)
        logger.info(f"Client: Stream {stream1.stream_id} received '{response1.decode()}'")
        assert response1.decode() == msg1

        msg2 = "Another message on stream 1"
        await stream1.write(msg2.encode())
        response2 = await stream1.read(1024)
        logger.info(f"Client: Stream {stream1.stream_id} received '{response2.decode()}'")
        assert response2.decode() == msg2

        logger.info(f"Client: Closing stream {stream1.stream_id}")
        await stream1.close()
        logger.info(f"Client: Stream {stream1.stream_id} closed.")

        # Test opening another stream
        stream2 = await client_smux.open_stream()
        msg3 = "Testing stream 2"
        await stream2.write(msg3.encode())
        response3 = await stream2.read(1024)
        logger.info(f"Client: Stream {stream2.stream_id} received '{response3.decode()}'")
        assert response3.decode() == msg3
        await stream2.close()
        logger.info(f"Client: Stream {stream2.stream_id} closed.")


    except Exception as e:
        logger.error(f"Client App Error: {e}", exc_info=True)
    finally:
        logger.info("Client: Closing SMUX session.")
        await client_smux.close()
        logger.info("Server: Closing SMUX session.")
        await server_smux.close() # Ensure server also cleans up

        # Close mock KCPs as well
        await kcp_client.close()
        await kcp_server.close()

    logger.info("SMUX test finished.")


if __name__ == '__main__': # pragma: no cover
    # asyncio.run(main_smux_test())
    logger.info("SMUX Handler module. Contains a minimal SMUX (v2) implementation.")
    logger.info("Run main_smux_test() in an async context to test basic SMUX loopback with mock KCP.")
    print("To run the test: uncomment `asyncio.run(main_smux_test())` and execute the file.")
