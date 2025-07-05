# SMUX (Stream Multiplexing) Handler for Snowflake Python Client
#
# The Go Snowflake client uses `xtaci/smux` for multiplexing multiple logical streams
# over a single KCP connection. This allows, for example, multiple Tor circuits to
# share one Snowflake peer connection.
#
# Similar to KCP, a direct, well-maintained Python port of `xtaci/smux` (compatible
# with its wire format and behavior) is not readily available.
#
# Options:
# 1. Direct Port: Translate the Go SMUX logic to Python. This is a complex task
#    requiring careful handling of framing, stream states, window updates, etc.
#
# 2. Alternative Python SMUX: Find a Python stream multiplexing library that is
#    compatible with `xtaci/smux`. Unlikely to exist with full compatibility.
#    Some libraries like `pysmux` exist but are for different purposes or protocols.
#
# 3. CFFI/Cython Wrapper: Wrap the `xtaci/smux` Go library. Adds build complexity
#    and requires careful management of Go/Python interop.
#
# 4. Omit SMUX (and KCP): If KCP is omitted and we use WebRTC data channels directly,
#    and if multiplexing is not a strict requirement for an initial Python version,
#    SMUX could also be omitted. This would mean one Tor circuit per WebRTC peer
#    connection. This is a significant deviation from the Go client's capabilities.
#
# 5. Alternative Multiplexing Protocol: Implement or use a different, simpler
#    multiplexing protocol over the WebRTC data channel. This would break
#    compatibility with Snowflake proxies expecting `xtaci/smux`.
#
# The `xtaci/smux` library provides an interface similar to `net.Conn` for its
# streams, and it runs over a reliable packet-oriented connection (like KCP).
#
# For now, this module is a placeholder.

import asyncio
import logging
from typing import Callable, Awaitable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Minimal SMUX constants (refer to xtaci/smux for actual values)
SMUX_VERSION = 2
SMUX_CMD_SYN = 0 # New stream
SMUX_CMD_FIN = 1 # Close stream (half-close)
SMUX_CMD_PSH = 2 # Data push
SMUX_CMD_NOP = 3 # No operation (keep-alive)
SMUX_CMD_UPD = 4 # Window update

SMUX_DEFAULT_STREAM_BUFFER = 65536 # Default buffer size for a stream

class SmuxStream: # pragma: no cover
    """
    Represents a single multiplexed stream over an SMUX session.
    It should behave like a net.Conn or an asyncio.StreamReader/Writer pair.
    """
    def __init__(self, stream_id: int, session: 'SmuxSession', on_close: Callable[[int], None]):
        self.stream_id = stream_id
        self.session = session
        self._on_close_callback = on_close
        self._buffer = bytearray()
        self._read_event = asyncio.Event()
        self._closed = False
        self._eof_received = False
        logger.warning(f"SmuxStream {stream_id} is a STUB and not functional.")

    async def read(self, n: int = -1) -> bytes:
        """Reads up to n bytes. If n is -1, reads until EOF."""
        if self._closed and not self._buffer: # Or self._eof_received and not self._buffer
             raise ConnectionAbortedError("Stream closed") # Or return b"" for EOF

        while not self._buffer and not self._eof_received and not self._closed:
            await self._read_event.wait()
            self._read_event.clear()

        if self._eof_received and not self._buffer:
            return b"" # EOF

        if self._closed and not self._buffer : # Should not happen if EOF is handled
             raise ConnectionAbortedError("Stream closed while reading")

        if n == -1:
            data = bytes(self._buffer)
            self._buffer.clear()
        else:
            data = bytes(self._buffer[:n])
            self._buffer = self._buffer[n:]

        # TODO: Send window update if buffer was depleted
        # await self.session.send_window_update(self.stream_id, len(data))
        return data

    async def write(self, data: bytes) -> None:
        if self._closed or self._eof_received: # Cannot write after FIN received or sent
            raise ConnectionAbortedError("Stream is closed or closing.")
        if not data:
            return
        await self.session.send_data(self.stream_id, data)

    async def close(self) -> None: # Half-close (send FIN)
        if not self._closed:
            self._closed = True # Mark as logically closed by user
            # Should also mark that we've sent FIN
            await self.session.send_fin(self.stream_id)
            logger.info(f"SmuxStream {self.stream_id} STUB: close() called (FIN sent).")
            # Actual removal from session's stream list happens when FIN is acked or both sides FIN.
            if self._on_close_callback:
                self._on_close_callback(self.stream_id)


    def _feed_data(self, data: bytes):
        self._buffer.extend(data)
        self._read_event.set()

    def _handle_fin(self):
        logger.info(f"SmuxStream {self.stream_id} STUB: FIN received.")
        self._eof_received = True # Mark EOF
        self._read_event.set() # Wake up any readers waiting for data

    def _handle_rst(self): # RST not explicitly in xtaci/smux, but good for abrupt close
        logger.info(f"SmuxStream {self.stream_id} STUB: RST received/sent.")
        self._closed = True
        self._eof_received = True
        self._read_event.set()
        if self._on_close_callback:
            self._on_close_callback(self.stream_id)


class SmuxSession: # pragma: no cover
    """
    Manages all streams for a single SMUX session over an underlying connection (e.g., KCP).
    """
    def __init__(self, underlying_send_func: Callable[[bytes], Awaitable[None]], is_client: bool = True):
        self.underlying_send_func = underlying_send_func
        self.is_client = is_client
        self.streams: Dict[int, SmuxStream] = {}
        self.next_stream_id: int = 1 if is_client else 2 # Client uses odd, server uses even
        self._session_closed = False
        logger.critical("SmuxSession is a STUB and not functional. Full SMUX logic needs to be ported.")
        # TODO: Keep-alive timers, session config (version, buffer sizes)

    async def open_stream(self) -> SmuxStream:
        if self._session_closed:
            raise ConnectionAbortedError("SMUX session is closed.")

        stream_id = self.next_stream_id
        self.next_stream_id += 2 # Increment by 2 for next ID of same parity

        stream = SmuxStream(stream_id, self, on_close=self._on_stream_closed_by_logic)
        self.streams[stream_id] = stream

        # Send SYN packet
        # Frame: ver(1), cmd(1), length(2), sid(4)
        header = bytearray(8)
        header[0] = SMUX_VERSION
        header[1] = SMUX_CMD_SYN
        # length = 0 for SYN payload
        header[4:8] = stream_id.to_bytes(4, 'big')
        await self.underlying_send_func(bytes(header))
        logger.info(f"SmuxSession STUB: Opened stream {stream_id} (SYN sent).")
        return stream

    def _on_stream_closed_by_logic(self, stream_id: int):
        # This might be called when SmuxStream.close() is called, or when FIN is acked.
        # Actual cleanup might be more involved (e.g., waiting for FIN from other side too)
        if stream_id in self.streams:
            # del self.streams[stream_id] # Don't delete immediately, might need to handle incoming FINs/data
            logger.info(f"SmuxStream {stream_id} marked for closure in session.")
            pass


    async def handle_incoming_packet(self, packet: bytes) -> None:
        if self._session_closed:
            return

        if len(packet) < 8: # Minimum header size
            logger.warning(f"SMUX STUB: Received runt packet, len {len(packet)}")
            return

        ver = packet[0]
        cmd = packet[1]
        length = int.from_bytes(packet[2:4], 'big')
        stream_id = int.from_bytes(packet[4:8], 'big')
        payload = packet[8:8+length]

        if ver != SMUX_VERSION:
            logger.warning(f"SMUX STUB: Received packet with unknown version {ver}")
            # Probably close session
            return

        stream = self.streams.get(stream_id)

        if cmd == SMUX_CMD_SYN: # Incoming new stream request (if we are server for this stream_id)
            if stream: # Stream ID collision or already exists
                logger.warning(f"SMUX STUB: SYN for existing stream {stream_id}")
                # TODO: Send RST for stream
            else:
                logger.info(f"SMUX STUB: Received SYN for new stream {stream_id}")
                new_stream = SmuxStream(stream_id, self, on_close=self._on_stream_closed_by_logic)
                self.streams[stream_id] = new_stream
                # TODO: Accept the stream by application logic (e.g. new SOCKS connection)
                # For now, auto-accept for stub. Application needs a way to get this new_stream.
                # This is where an `accept_stream()` method would be useful on the session.
                if self.on_new_stream_callback: # Hypothetical callback
                    asyncio.create_task(self.on_new_stream_callback(new_stream))

        elif cmd == SMUX_CMD_FIN:
            if stream:
                stream._handle_fin()
                # If stream was also closed locally, it can be fully removed.
                if stream._closed: # i.e. local FIN was also sent
                    if stream_id in self.streams: del self.streams[stream_id]
                    logger.info(f"SMUX STUB: Stream {stream_id} fully closed (both FINs).")
            else:
                logger.warning(f"SMUX STUB: FIN for unknown stream {stream_id}")

        elif cmd == SMUX_CMD_PSH:
            if stream:
                stream._feed_data(payload)
            else:
                logger.warning(f"SMUX STUB: PSH for unknown stream {stream_id}")
                # TODO: Send RST for stream

        elif cmd == SMUX_CMD_UPD:
            if stream:
                # TODO: Handle window update from remote peer
                window_increment = int.from_bytes(payload[0:4], 'big') # Assuming payload for UPD is (credits, sid) - check spec
                # The actual xtaci/smux UPD payload is just (credits, sid is in header)
                logger.info(f"SMUX STUB: Received window update for stream {stream_id}, increment {window_increment}")
                # stream.update_remote_window(window_increment)
            else:
                logger.warning(f"SMUX STUB: UPD for unknown stream {stream_id}")

        elif cmd == SMUX_CMD_NOP:
            logger.debug("SMUX STUB: NOP received (keep-alive).")
            # Respond with NOP? Or just note.

        else:
            logger.warning(f"SMUX STUB: Unknown command {cmd} for stream {stream_id}")


    async def send_data(self, stream_id: int, data: bytes):
        # Frame and send data, respecting window size (not implemented in stub)
        header = bytearray(8)
        header[0] = SMUX_VERSION
        header[1] = SMUX_CMD_PSH
        header[2:4] = len(data).to_bytes(2, 'big')
        header[4:8] = stream_id.to_bytes(4, 'big')
        await self.underlying_send_func(bytes(header) + data)

    async def send_fin(self, stream_id: int):
        header = bytearray(8)
        header[0] = SMUX_VERSION
        header[1] = SMUX_CMD_FIN
        # length = 0
        header[4:8] = stream_id.to_bytes(4, 'big')
        await self.underlying_send_func(bytes(header))

    async def send_window_update(self, stream_id: int, increment: int):
        # Payload for window update: (uint32 num) - number of bytes received
        # This is a bit simplified, usually it's about increasing receive window.
        # xtaci/smux sends (credits uint32, sid uint32) in payload, but sid is already in header.
        # Let's assume payload is just credits for now.
        payload = increment.to_bytes(4, 'big') # This is how many more bytes remote can send

        header = bytearray(8)
        header[0] = SMUX_VERSION
        header[1] = SMUX_CMD_UPD
        header[2:4] = len(payload).to_bytes(2, 'big')
        header[4:8] = stream_id.to_bytes(4, 'big')
        await self.underlying_send_func(bytes(header) + payload)


    async def close(self):
        if not self._session_closed:
            self._session_closed = True
            # Close all active streams
            for stream_id in list(self.streams.keys()): # list() for safe iteration if modified
                stream = self.streams.get(stream_id)
                if stream and not stream._closed: # If not already closing locally
                    await stream.close() # Send FIN
            self.streams.clear()
            logger.info("SMUX STUB: Session closed.")
            # TODO: Send session close notification if protocol supports it (xtaci/smux doesn't seem to have one explicitly)

    # For server-side applications (like Snowflake proxy)
    async def accept_stream(self) -> Optional[SmuxStream]: # pragma: no cover
        """Waits for and returns the next remotely-opened stream."""
        # This needs a queue or event that `handle_incoming_packet` populates on SYN.
        # Example:
        # if not self._incoming_stream_queue: self._incoming_stream_queue = asyncio.Queue()
        # stream = await self._incoming_stream_queue.get()
        # return stream
        raise NotImplementedError("SMUX accept_stream STUB not fully implemented.")

    # Placeholder for callback when a new stream is initiated by the remote side
    async def on_new_stream_callback(self, stream: SmuxStream): # pragma: no cover
        logger.info(f"SMUX STUB: New incoming stream {stream.stream_id} received by session.")
        # Application would typically take this stream and handle it.
        # Example: pass it to a SOCKS handler if this is a server.
        pass


if __name__ == '__main__': # pragma: no cover
    logging.basicConfig(level=logging.DEBUG)
    logger.info("SMUX Handler STUB module. No functional SMUX implementation is present.")
    logger.info("This module requires a Python port of SMUX (like xtaci/smux) or a compatible alternative.")

    async def dummy_send_to_kcp(data: bytes):
        print(f"Dummy Send (to KCP): {data}")

    async def main():
        smux_sess = SmuxSession(dummy_send_to_kcp, is_client=True)

        # Simulate opening a stream
        try:
            stream1 = await smux_sess.open_stream()
            print(f"Opened stream stub: {stream1.stream_id}")

            # Simulate sending data on this stream
            # await stream1.write(b"hello from stream1 stub")

            # Simulate receiving a SYN from remote (e.g. if we were a server)
            # header_syn_remote = bytearray(8)
            # header_syn_remote[0] = SMUX_VERSION; header_syn_remote[1] = SMUX_CMD_SYN
            # header_syn_remote[4:8] = (2).to_bytes(4, 'big') # Remote stream ID 2
            # await smux_sess.handle_incoming_packet(bytes(header_syn_remote))
            # stream2 = smux_sess.streams.get(2)
            # if stream2: print(f"Remote opened stream stub: {stream2.stream_id}")

            # Simulate receiving data for stream1
            # header_psh_s1 = bytearray(8)
            # header_psh_s1[0] = SMUX_VERSION; header_psh_s1[1] = SMUX_CMD_PSH
            # data_payload = b"response data"
            # header_psh_s1[2:4] = len(data_payload).to_bytes(2, 'big')
            # header_psh_s1[4:8] = stream1.stream_id.to_bytes(4, 'big')
            # await smux_sess.handle_incoming_packet(bytes(header_psh_s1) + data_payload)
            # received_data = await stream1.read()
            # print(f"Stream {stream1.stream_id} read data: {received_data}")

            print("SMUX Session & Stream stubs initialized. Methods for data transfer are placeholders.")
        except Exception as e:
            print(f"Error with SMUX stub: {e}")
        finally:
            await smux_sess.close()

    # asyncio.run(main()) # Commented out as it will mostly show errors or incomplete logic.
    print("Run the main function in an async context to see stub behavior.")
