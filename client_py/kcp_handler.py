"""
Minimal KCP Implementation for Snowflake Python Client.
Aims for wire compatibility with xtaci/kcp-go's usage in the Go Snowflake client.
Focuses on:
- Conversation ID
- Packet types (data, ack, wnd probe, wnd ack)
- Sequence numbers (sn), Unacknowledged sequence number (una)
- Send/Receive windows (simplified)
- Retransmission (simplified timeout based)
- Configurable parameters (nodelay, interval, resend, nc, window sizes) matching Go client.

This is NOT a full KCP port. Many features are omitted or simplified.
(e.g., SACK, complex RTO calculation, full congestion control beyond nc=1)
"""

import asyncio
import logging
import struct
import time
from collections import deque, OrderedDict
from typing import Callable, Awaitable, Optional, Tuple, Deque, Dict, Any

logger = logging.getLogger(__name__)

# KCP Constants (from ikcp.h and xtaci/kcp-go)
IKCP_RTO_NDL = 30  # no delay min rto
IKCP_RTO_MIN = 100  # normal min rto
IKCP_RTO_DEF = 200
IKCP_RTO_MAX = 60000
IKCP_CMD_PUSH = 81  # cmd: push data
IKCP_CMD_ACK = 82  # cmd: ack
IKCP_CMD_WASK = 83  # cmd: window probe (ask)
IKCP_CMD_WINS = 84  # cmd: window size (tell)
IKCP_ASK_SEND = 1  # need to send IKCP_CMD_WASK
IKCP_ASK_TELL = 2  # need to send IKCP_CMD_WINS
IKCP_WND_SND = 32  # default send window. Go client uses 65535
IKCP_WND_RCV = 128  # default receive window. Go client uses 65535. xtaci default is 128, check this.
                    # xtaci/kcp-go default rcv_wnd is const IW_INIT = 128
                    # The SetWindowSize(65535, 65535) in Go client sets session.snd_wnd and session.rcv_wnd
IKCP_MTU_DEF = 1400
IKCP_INTERVAL = 100 # KCP internal update interval in ms. Go client sets this to 0 in SetNoDelay, meaning update check is frequent.
IKCP_DEADLINK = 20 # Max retransmits before link is considered dead. xtaci default is 20.
IKCP_THRESH_INIT = 2
IKCP_THRESH_MIN = 2
IKCP_PROBE_INIT = 7000  # 7 secs to probe window
IKCP_PROBE_LIMIT = 120000  # up to 120 secs to probe window

# KCP Segment Header (24 bytes):
# conv (4B), cmd (1B), frg (1B), wnd (2B)
# ts (4B), sn (4B)
# una (4B), len (4B)
# data (variable)
KCP_OVERHEAD = 24

class KCPSegment:
    def __init__(self, conv: int = 0, cmd: int = 0, frg: int = 0, wnd: int = 0,
                 ts: int = 0, sn: int = 0, una: int = 0, data: bytes = b''):
        self.conv = conv
        self.cmd = cmd
        self.frg = frg  # Fragment count (0 for last fragment)
        self.wnd = wnd  # Available receive window
        self.ts = ts    # Timestamp
        self.sn = sn    # Sequence number
        self.una = una  # Unacknowledged sequence number (next sn expected by receiver)
        self.length = len(data)
        self.data = data
        self.xmit = 0 # Transmit counter
        self.rto = 0  # Retransmission timeout for this segment
        self.resend_ts = 0 # Timestamp for next resend

    def encode(self) -> bytes:
        header = struct.pack('>I', self.conv)      # conv_id (little endian in C, but Go uses big for network)
                                                  # xtaci/kcp-go uses little endian for header encoding.
                                                  # struct.pack("<I", val) for little-endian
                                                  # Let's verify xtaci/kcp-go encoding.
                                                  # kcp.go, segment.encode(): binary.LittleEndian.PutUint32...
        header = struct.pack('<I', self.conv)      # Conversation ID
        header += struct.pack('<B', self.cmd)       # Command
        header += struct.pack('<B', self.frg)       # Fragment
        header += struct.pack('<H', self.wnd)       # Window size
        header += struct.pack('<I', self.ts)        # Timestamp
        header += struct.pack('<I', self.sn)        # Sequence number
        header += struct.pack('<I', self.una)       # Unacknowledged sequence number
        header += struct.pack('<I', self.length)    # Data length
        return header + self.data

    @classmethod
    def decode(cls, raw_data: bytes) -> Optional['KCPSegment']:
        if len(raw_data) < KCP_OVERHEAD:
            return None

        conv = struct.unpack_from('<I', raw_data, 0)[0]
        cmd = struct.unpack_from('<B', raw_data, 4)[0]
        frg = struct.unpack_from('<B', raw_data, 5)[0]
        wnd = struct.unpack_from('<H', raw_data, 6)[0]
        ts = struct.unpack_from('<I', raw_data, 8)[0]
        sn = struct.unpack_from('<I', raw_data, 12)[0]
        una = struct.unpack_from('<I', raw_data, 16)[0]
        length = struct.unpack_from('<I', raw_data, 20)[0]

        if len(raw_data) < KCP_OVERHEAD + length:
            logger.warning(f"KCP Segment decode: insufficient data for payload. Expected {KCP_OVERHEAD + length}, got {len(raw_data)}")
            return None # Not enough data for payload

        data = raw_data[KCP_OVERHEAD : KCP_OVERHEAD + length]
        return cls(conv, cmd, frg, wnd, ts, sn, una, data)


class KCPConn:
    def __init__(self, conv_id: int,
                 output_callback: Callable[[bytes], Awaitable[None]],
                 loop: Optional[asyncio.AbstractEventLoop] = None,
                 is_client: bool = True): # is_client currently not used for logic diff
        self.conv_id = conv_id
        self._output_callback = output_callback # Function to send raw bytes to underlying transport
        self._loop = loop if loop else asyncio.get_event_loop()

        self.snd_una: int = 0  # Oldest unacked sequence number sent
        self.snd_nxt: int = 0  # Next sequence number to send
        self.rcv_nxt: int = 0  # Next sequence number expected to receive

        self.snd_wnd: int = IKCP_WND_SND # Send window size (remote rcv_wnd)
        self.rcv_wnd: int = IKCP_WND_RCV # Receive window size (local)
        self.rmt_wnd: int = IKCP_WND_RCV # Remote receive window size

        self.mtu: int = IKCP_MTU_DEF
        self.mss: int = self.mtu - KCP_OVERHEAD # Max segment size

        self._send_queue: Deque[bytes] = deque() # Queue for application data to be segmented and sent
        self._send_buffer: OrderedDict[int, KCPSegment] = OrderedDict() # Segments sent but not yet ACKed (sn -> segment)
        self._recv_buffer: OrderedDict[int, KCPSegment] = OrderedDict() # Segments received out-of-order (sn -> segment)
        self._recv_queue: Deque[bytes] = deque() # Reassembled application data ready for reading

        self.rx_rto: int = IKCP_RTO_DEF
        self.rx_srtt: int = 0 # Smoothed RTT
        self.rx_rttvar: int = 0 # RTT variance

        # Configurable parameters (matching Go client's settings)
        self.nodelay: bool = False # KCP nodelay feature (false = regular KCP mode)
        self.rx_minrto: int = IKCP_RTO_DEF # Min RTO. Go: 200ms
        self.interval: int = 20 # Internal update interval (ms). Go: underlying loop speed (e.g. 10-20ms)
                                 # This 'interval' is for our Python update loop.
                                 # KCP's own 'interval' is used by ikcp_check to throttle ikcp_update.
                                 # If KCP's interval is 0 (from SetNoDelay), ikcp_update runs every check.
        self.fastresend: int = 0 # Fast resend trigger counter. Go: 0 (off)
        self.nocwnd: bool = True # Disable congestion window. Go: true

        self.current_ms: int = self._get_current_ms()
        self.ts_flush: int = self.current_ms + self.interval # Next time to call update

        self._acklist: Deque[Tuple[int, int]] = deque() # (sn, ts) of ACKs to send

        self.probe: int = 0 # Probe flags
        self.ts_probe: int = 0 # Timestamp for next window probe
        self.probe_wait: int = 0

        self.dead_link_threshold = IKCP_DEADLINK
        self.connected = True
        self._update_task: Optional[asyncio.Task] = None
        self._started = False

        logger.info(f"KCPConn created. ConvID: {self.conv_id}. MTU: {self.mtu}, MSS: {self.mss}")
        logger.info(f"KCP Config: nodelay_feat={self.nodelay}, rx_minrto={self.rx_minrto}, update_interval={self.interval}ms, "
                    f"fastresend_trigger={self.fastresend}, nocwnd={self.nocwnd}")


    def set_nodelay(self, nodelay_on: bool, interval_ms: int, resend_trigger: int, nc_on: bool):
        """ Matches xtaci/kcp-go SetNoDelay """
        self.nodelay = nodelay_on # This is the KCP 'nodelay' mode, not just min_rto setting
        if self.nodelay:
            self.rx_minrto = IKCP_RTO_NDL
        else:
            self.rx_minrto = IKCP_RTO_DEF

        # The 'interval_ms' in KCP's SetNoDelay sets an internal variable that ikcp_check uses
        # to decide if ikcp_update should run. If 0, ikcp_update runs every time.
        # Our Python KCPConn's self.interval is for the asyncio sleep in _run_update_loop.
        # We can adjust our loop's frequency based on this if needed, but Go client passes 0,
        # implying our _run_update_loop should be reasonably fast (e.g. 10-20ms).
        if interval_ms > 0 : # if KCP's interval is set, our loop can be relaxed.
            self.interval = max(10, interval_ms) # Ensure our loop runs at least this fast.

        self.fastresend = resend_trigger
        self.nocwnd = nc_on
        logger.info(f"KCP params updated: nodelay_feat={self.nodelay}, rx_minrto={self.rx_minrto}, "
                    f"update_interval_setting={interval_ms} (actual loop ~{self.interval}ms), "
                    f"fastresend_trigger={self.fastresend}, nocwnd={self.nocwnd}")

    def set_wndsize(self, sndwnd: int, rcvwnd: int):
        """ Matches xtaci/kcp-go SetWindowSize """
        if sndwnd > 0:
            self.snd_wnd = sndwnd # This is effectively rmt_wnd for sending logic
        if rcvwnd > 0:
            self.rcv_wnd = rcvwnd # Our local receive window capacity
        logger.info(f"KCP window sizes updated: snd_wnd(peer_rcv_wnd)={self.snd_wnd}, rcv_wnd(my_rcv_wnd)={self.rcv_wnd}")

    def _get_current_ms(self) -> int:
        return int(self._loop.time() * 1000)

    def _update_rto(self, rtt: int):
        if self.rx_srtt == 0:
            self.rx_srtt = rtt
            self.rx_rttvar = rtt // 2
        else:
            delta = abs(rtt - self.rx_srtt)
            self.rx_rttvar = (3 * self.rx_rttvar + delta) // 4
            self.rx_srtt = (7 * self.rx_srtt + rtt) // 8
            if self.rx_srtt < 1:
                self.rx_srtt = 1

        rto = self.rx_srtt + max(self.interval, 4 * self.rx_rttvar) # Max with self.interval is not standard KCP
                                                                # KCP: rto = srtt + max(1, 4 * rttvar)
                                                                # Let's use max(1, ...)
        rto = self.rx_srtt + max(1, 4 * self.rx_rttvar)

        self.rx_rto = max(self.rx_minrto, min(rto, IKCP_RTO_MAX))


    def _parse_segment(self, seg: KCPSegment):
        """ Process a received segment based on its sn and una """
        if seg.conv != self.conv_id:
            logger.warning(f"KCP: Received segment with wrong conv_id {seg.conv}, expected {self.conv_id}")
            return False # Not for this connection

        # Update remote window size
        self.rmt_wnd = seg.wnd

        # Process UNA: remove acknowledged segments from send_buffer
        # All segments with sn < seg.una are considered ACKed
        acked_sns = []
        for sn_sent in list(self._send_buffer.keys()): # Iterate over copy for modification
            if sn_sent < seg.una:
                sent_seg = self._send_buffer.pop(sn_sent)
                acked_sns.append(sn_sent)
                # Update RTO
                rtt = self.current_ms - sent_seg.ts
                if rtt >=0 : # Should always be positive
                     self._update_rto(rtt)
            else:
                break # send_buffer is ordered by sn
        if acked_sns:
            logger.debug(f"KCP: Processed UNA {seg.una}. Acked SNs: {acked_sns}. RTO: {self.rx_rto}ms")


        # Handle segment commands
        if seg.cmd == IKCP_CMD_PUSH:
            if seg.sn < self.rcv_nxt + self.rcv_wnd: # If within receive window
                self._add_ack(seg.sn, seg.ts) # Acknowledge it
                if seg.sn >= self.rcv_nxt: # If it's a new segment (or future one)
                    if seg.sn not in self._recv_buffer:
                        self._recv_buffer[seg.sn] = seg
                        # Sort recv_buffer by SN to easily get contiguous segments
                        self._recv_buffer = OrderedDict(sorted(self._recv_buffer.items()))

                    # Try to move data from recv_buffer to recv_queue
                    while self.rcv_nxt in self._recv_buffer:
                        data_seg = self._recv_buffer.pop(self.rcv_nxt)
                        self._recv_queue.append(data_seg.data) # Assuming no fragmentation for now (frg=0)
                        self.rcv_nxt += 1
                        if len(self._recv_queue) > self.rcv_wnd * 1.5 : # Safety break, should not happen
                            logger.warning("KCP: recv_queue growing too large, possible logic error.")
                            break
            else:
                logger.debug(f"KCP: Segment sn={seg.sn} outside receive window (rcv_nxt={self.rcv_nxt}, rcv_wnd={self.rcv_wnd})")


        elif seg.cmd == IKCP_CMD_ACK:
            # This ACK is for a specific segment sn. If we sent it, update RTO.
            if seg.sn in self._send_buffer:
                acked_seg = self._send_buffer.pop(seg.sn)
                rtt = self.current_ms - seg.ts # seg.ts in ACK packet is echo of original PSH ts
                if rtt >=0:
                    self._update_rto(rtt)
                logger.debug(f"KCP: Received ACK for sn={seg.sn}. RTO: {self.rx_rto}ms")

        elif seg.cmd == IKCP_CMD_WASK:
            # Remote is asking for our window size. Mark to send WINS.
            self.probe |= IKCP_ASK_TELL
            logger.debug("KCP: Received WASK, will send WINS.")

        elif seg.cmd == IKCP_CMD_WINS:
            # Remote is telling us its window size. This is already handled by rmt_wnd = seg.wnd
            # No specific action here beyond what's done for every segment.
            logger.debug(f"KCP: Received WINS. Remote window is {seg.wnd}.")

        return True


    async def _send_immediate_segment(self, seg: KCPSegment):
        """ Sends a segment immediately (e.g. ACK, WASK, WINS) """
        if not self.connected: return
        try:
            await self._output_callback(seg.encode())
        except Exception as e:
            logger.error(f"KCP: Error in output_callback for immediate segment: {e}")
            self.connected = False


    def _add_ack(self, sn: int, ts: int):
        self._acklist.append((sn, ts))

    async def _flush_acks(self):
        if not self._acklist:
            return

        # Combine multiple ACKs into one segment if possible (not implemented here for simplicity)
        # For now, send one ACK segment per ACK needed (could be optimized)
        # Or, better, send one ACK segment with the latest UNA and list of (sn,ts) pairs in payload
        # For simplicity, we send an ACK segment for each (sn, ts) pair, but this is inefficient.
        # A single ACK segment with current rcv_nxt as UNA is better.

        # Send a single ACK segment with current UNA
        if self._acklist: # Check again, as it might have been cleared by another path
            ack_seg = KCPSegment(conv=self.conv_id, cmd=IKCP_CMD_ACK, wnd=self._get_available_rcv_wnd())
            ack_seg.una = self.rcv_nxt # Highest contiguous SN received + 1
            # For simplicity, we don't include individual (sn,ts) for SACKs in this minimal version.
            # An ACK with just UNA acknowledges all up to UNA-1.
            logger.debug(f"KCP: Flushing ACK. UNA: {ack_seg.una}, RcvWnd: {ack_seg.wnd}")
            await self._send_immediate_segment(ack_seg)
            self._acklist.clear() # All pending ACKs are covered by this single ACK with UNA.

    def _get_available_rcv_wnd(self) -> int:
        # Available slots in receive window
        # rcv_wnd is total capacity. len(recv_buffer) is out-of-order segments.
        # len(recv_queue) is assembled data not yet read by app.
        # KCP rcv_wnd is about number of segments, not bytes.
        # Effective available window is capacity minus buffered segments.
        available = self.rcv_wnd - len(self._recv_buffer)
        return max(0, available)


    async def input_data(self, data: bytes):
        """ Called by underlying transport when a packet is received """
        if not self.connected: return
        self.current_ms = self._get_current_ms()

        # In xtaci/kcp-go, input can receive multiple KCP segments concatenated if underlying protocol allows.
        # WebRTC data channels are message-based, so one read = one message = one KCP segment.
        seg = KCPSegment.decode(data)
        if seg:
            logger.debug(f"KCP < Input: conv={seg.conv}, cmd={seg.cmd}, sn={seg.sn}, una={seg.una}, frg={seg.frg}, wnd={seg.wnd}, len={seg.length}, data='{seg.data[:20]}...'")
            self._parse_segment(seg)
        else:
            logger.warning(f"KCP: Failed to decode incoming packet, len {len(data)}")
            # Potentially handle error, e.g. close connection if too many decode errors.

        # After processing input, there might be ACKs to send or data to output.
        # The update loop will handle flushing. If an immediate ACK is needed for fast recovery,
        # it could be triggered here, but KCP often bundles ACKs.
        # For simplicity, rely on the update loop to flush ACKs.
        # However, if an ACK was generated, we might want to send it soon.
        if self._acklist: # If _parse_segment generated ACKs
            await self._flush_acks()


    async def send(self, data: bytes):
        """ Queues application data to be sent """
        if not self.connected:
            raise ConnectionAbortedError("KCP connection is not active")
        if not data: return

        # Simple segmentation: if data > mss, split it.
        # KCP `frg` field is for this, but this minimal version won't implement frg logic fully.
        # We'll assume SMUX sends packets that are <= MSS for KCP.
        # If SMUX sends larger, it should be an error or KCP should segment it.
        # For now, let's assume data is one segment's payload.

        # If data is larger than MSS, split it into multiple segments.
        # Each segment gets its own sequence number.
        # This is different from KCP fragmentation (frg field), which is about
        # splitting a single user message across multiple KCP segments if the message
        # itself is too large for a single KCP segment payload.
        # Here, `data` is what SMUX wants to send. SMUX itself should handle its own PDU sizes.
        # So, we assume `data` from SMUX is intended as a single KCP segment payload.
        if len(data) > self.mss:
            logger.warning(f"KCP Send: Data size {len(data)} > MSS {self.mss}. Truncating or error needed. For now, allowing.")
            # For simplicity, we'll allow sending it, but KCP proper would segment it using `frg`.
            # Or, this layer should expect SMUX to respect MSS.
            # Let's assume SMUX respects MSS. If not, we should raise error or properly fragment.
            # For now, just queue it. The update loop will try to send it.

        self._send_queue.append(data)
        # The update loop will pick this up and create segments.

    async def receive(self) -> Optional[bytes]:
        """ Returns reassembled application data, or None if nothing is ready """
        if not self.connected and not self._recv_queue:
             raise ConnectionAbortedError("KCP connection is not active and no pending data")
        if self._recv_queue:
            return self._recv_queue.popleft()
        return None

    async def update(self):
        """ Periodic update call for KCP logic (retransmissions, window probes, etc.) """
        self.current_ms = self._get_current_ms()
        if not self.connected: return

        # --- Flush ACKs ---
        # ACKs are generated when processing input. Flush them if any.
        await self._flush_acks()


        # --- Process Send Queue (application data -> KCP segments) ---
        # Only send if send window allows. (Remote receive window `rmt_wnd` and our `snd_wnd` setting)
        # Effective send window is min(self.snd_wnd, self.rmt_wnd).
        # Number of packets in flight: len(self._send_buffer)
        effective_snd_wnd = min(self.snd_wnd, self.rmt_wnd) if not self.nocwnd else self.snd_wnd

        while self._send_queue and (len(self._send_buffer) < effective_snd_wnd):
            app_data = self._send_queue.popleft()

            # Create PUSH segment
            # For now, assuming no fragmentation (frg=0)
            seg = KCPSegment(conv=self.conv_id, cmd=IKCP_CMD_PUSH, frg=0,
                             wnd=self._get_available_rcv_wnd(), ts=self.current_ms,
                             sn=self.snd_nxt, una=self.rcv_nxt, data=app_data)

            seg.rto = self.rx_rto
            seg.resend_ts = self.current_ms + seg.rto
            seg.xmit = 1

            self._send_buffer[seg.sn] = seg
            self.snd_nxt += 1

            logger.debug(f"KCP > Output (PUSH): sn={seg.sn}, una={seg.una}, ts={seg.ts}, rto={seg.rto}, len={seg.length}, data='{seg.data[:20]}...'")
            await self._send_immediate_segment(seg)

            if len(self._send_buffer) >= effective_snd_wnd:
                break # Send window full


        # --- Handle Retransmissions ---
        # Check segments in send_buffer for timeouts
        resent_sns = []
        for sn, seg in list(self._send_buffer.items()): # Iterate copy for potential modification (though not here)
            if self.current_ms >= seg.resend_ts:
                seg.xmit += 1
                if seg.xmit > self.dead_link_threshold:
                    logger.error(f"KCP: Segment sn={sn} retransmitted {seg.xmit} times. Dead link.")
                    self.connected = False
                    # TODO: Signal error to application
                    return

                # Double RTO for this segment (simplified backoff)
                seg.rto = max(seg.rto * 1.5, self.rx_minrto) # Simplified, KCP does better
                seg.resend_ts = self.current_ms + seg.rto

                # Resend segment
                resent_sns.append(sn)
                logger.debug(f"KCP > Output (RETRY PUSH): sn={seg.sn}, xmit={seg.xmit}, new_rto={seg.rto}, data='{seg.data[:20]}...'")
                await self._send_immediate_segment(seg) # Resend the original segment (with updated ts for RTT est?)
                                                        # KCP resends with original timestamp.

                # Fast resend logic (simplified, based on self.fastresend trigger count)
                # Not fully implemented here. KCP uses lost packet detection.

        if resent_sns:
            logger.info(f"KCP: Retransmitted SNs: {resent_sns}")


        # --- Window Probing (if needed) ---
        # If remote window is zero, send WASK (window ask)
        if self.rmt_wnd == 0:
            if self.probe_wait == 0: # Initial probe
                self.probe_wait = IKCP_PROBE_INIT
                self.ts_probe = self.current_ms + self.probe_wait
            else:
                if self.current_ms >= self.ts_probe:
                    if self.probe_wait < IKCP_PROBE_INIT: # Should not happen if init correctly
                        self.probe_wait = IKCP_PROBE_INIT
                    self.probe_wait = int(self.probe_wait * 1.5) # Exponential backoff for probe
                    if self.probe_wait > IKCP_PROBE_LIMIT:
                        self.probe_wait = IKCP_PROBE_LIMIT
                    self.ts_probe = self.current_ms + self.probe_wait
                    self.probe |= IKCP_ASK_SEND # Mark to send WASK
                    logger.debug(f"KCP: Remote window zero, scheduling WASK. Next probe in {self.probe_wait}ms.")
        else: # Remote window is not zero, clear probe state
            self.ts_probe = 0
            self.probe_wait = 0

        # Send WASK if marked
        if self.probe & IKCP_ASK_SEND:
            wask_seg = KCPSegment(conv=self.conv_id, cmd=IKCP_CMD_WASK, wnd=self._get_available_rcv_wnd(), una=self.rcv_nxt)
            logger.debug(f"KCP > Output (WASK): una={wask_seg.una}, wnd={wask_seg.wnd}")
            await self._send_immediate_segment(wask_seg)
            self.probe &= ~IKCP_ASK_SEND # Clear flag

        # Send WINS if marked (remote asked for our window)
        if self.probe & IKCP_ASK_TELL:
            wins_seg = KCPSegment(conv=self.conv_id, cmd=IKCP_CMD_WINS, wnd=self._get_available_rcv_wnd(), una=self.rcv_nxt)
            logger.debug(f"KCP > Output (WINS): una={wins_seg.una}, wnd={wins_seg.wnd}")
            await self._send_immediate_segment(wins_seg)
            self.probe &= ~IKCP_ASK_TELL


    async def _run_update_loop(self):
        self._started = True
        logger.info(f"KCP update loop started. Interval: {self.interval}ms")
        while self.connected:
            try:
                await self.update()
                await asyncio.sleep(self.interval / 1000.0)
            except asyncio.CancelledError:
                logger.info("KCP update loop cancelled.")
                break
            except Exception as e:
                logger.error(f"KCP: Error in update loop: {e}", exc_info=True)
                self.connected = False # Stop loop on unexpected error
                break
        logger.info("KCP update loop stopped.")
        self._started = False # Mark as stopped

    def start(self):
        if not self._started and self.connected:
            # Apply settings from Go client
            # conn.SetNoDelay(0, 0, 0, 1)
            self.set_nodelay(nodelay_on=False, interval_ms=0, resend_trigger=0, nc_on=True)
            # conn.SetWindowSize(WindowSize, WindowSize) -> WindowSize = 65535
            self.set_wndsize(sndwnd=65535, rcvwnd=65535)

            self._update_task = self._loop.create_task(self._run_update_loop())
        else:
            logger.warning("KCP: Start called but already started or not connected.")


    async def close(self):
        logger.info("KCP: Closing connection...")
        self.connected = False
        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass # Expected
            self._update_task = None

        # Clear buffers (optional, as instance will be garbage collected)
        self._send_queue.clear()
        self._send_buffer.clear()
        self._recv_buffer.clear()
        self._recv_queue.clear()
        self._acklist.clear()
        logger.info("KCP: Connection closed.")

    # Methods to make it act somewhat like a PacketConn for SMUX
    # SMUX expects a reliable, ordered packet stream. KCP provides this.
    # SMUX will call send() and receive() on this KCPConn object.

    def get_conv_id(self) -> int:
        return self.conv_id

    def is_connected(self) -> bool:
        return self.connected


class KCPHandler: # Wrapper for KCPConn, might not be strictly needed if KCPConn is used directly
    def __init__(self, conv_id: int,
                 underlying_send_func: Callable[[bytes], Awaitable[None]],
                 loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        conv_id: Conversation ID for KCP.
        underlying_send_func: Async function to call to send raw bytes (e.g., over WebRTC DC).
        """
        logger.info(f"KCPHandler initializing with ConvID: {conv_id}")
        self.kcp_conn = KCPConn(conv_id=conv_id, output_callback=underlying_send_func, loop=loop)
        # KCPConn will call set_nodelay and set_wndsize in its start() method.

    async def handle_incoming_packet(self, packet: bytes) -> None:
        """
        Process an incoming packet from the underlying transport (e.g., WebRTC data channel).
        This packet is fed into the KCP state machine.
        """
        await self.kcp_conn.input_data(packet)

    async def send_data(self, data: bytes) -> None:
        """
        Send application data via KCP. KCP will handle segmentation, reliability, etc.
        This data comes from the layer above KCP (e.g., SMUX).
        """
        await self.kcp_conn.send(data)

    async def receive_data(self) -> Optional[bytes]:
        """
        Receive reassembled application data from KCP.
        Called by the layer above KCP (e.g., SMUX).
        Returns one whole application message/packet that was sent by the peer.
        """
        return await self.kcp_conn.receive()

    def start(self):
        """Starts the KCP connection's internal update loop."""
        self.kcp_conn.start()

    async def close(self):
        """Closes the KCP connection and stops its update loop."""
        await self.kcp_conn.close()

    def is_connected(self) -> bool:
        return self.kcp_conn.is_connected()

# Example usage (for testing standalone KCPHandler)
async def main_kcp_test(): # pragma: no cover
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    loop = asyncio.get_event_loop()

    # Simulate two KCP endpoints talking to each other via a shared queue
    # In reality, underlying_send_func would send over network (WebRTC)

    shared_medium_c1_to_c2 = asyncio.Queue()
    shared_medium_c2_to_c1 = asyncio.Queue()

    async def send_for_c1(data: bytes):
        logger.debug(f"C1 -> C2 (medium): {len(data)} bytes")
        # Simulate network delay/loss if desired
        # await asyncio.sleep(0.01) # Small delay
        # if random.random() < 0.1: # 10% packet loss
        #     logger.warning("Simulating packet loss for C1->C2")
        #     return
        await shared_medium_c1_to_c2.put(data)

    async def send_for_c2(data: bytes):
        logger.debug(f"C2 -> C1 (medium): {len(data)} bytes")
        await shared_medium_c2_to_c1.put(data)

    conv_id = 12345
    kcp1 = KCPHandler(conv_id, send_for_c1, loop)
    kcp2 = KCPHandler(conv_id, send_for_c2, loop)

    kcp1.start()
    kcp2.start()

    async def c1_reader():
        while kcp1.is_connected() or kcp2.is_connected(): # check both for test
            try:
                packet = await asyncio.wait_for(shared_medium_c2_to_c1.get(), timeout=1.0)
                if packet:
                    logger.debug(f"C1 < Medium (from C2): {len(packet)} bytes")
                    await kcp1.handle_incoming_packet(packet)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error(f"C1 reader error: {e}")
                break
        logger.info("C1 reader loop finished.")


    async def c2_reader():
        while kcp2.is_connected() or kcp1.is_connected():
            try:
                packet = await asyncio.wait_for(shared_medium_c1_to_c2.get(), timeout=1.0)
                if packet:
                    logger.debug(f"C2 < Medium (from C1): {len(packet)} bytes")
                    await kcp2.handle_incoming_packet(packet)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error(f"C2 reader error: {e}")
                break
        logger.info("C2 reader loop finished.")

    async def c1_app_sender():
        for i in range(5):
            msg = f"Message from C1, number {i}".encode()
            logger.info(f"C1 App: Sending '{msg.decode()}'")
            await kcp1.send_data(msg)
            await asyncio.sleep(0.5) # Send periodically
        await asyncio.sleep(2) # Wait for acks
        await kcp1.close()


    async def c2_app_receiver():
        received_count = 0
        while kcp2.is_connected() or received_count < 5 : # Keep trying if expecting data
            data = await kcp2.receive_data()
            if data:
                logger.info(f"C2 App: Received '{data.decode()}'")
                received_count +=1
            else:
                await asyncio.sleep(0.1) # Wait for data
            if received_count >= 5 and not kcp1.is_connected(): # if sender is done
                break
        await asyncio.sleep(1) # wait for potential final acks from kcp2
        await kcp2.close()


    reader1_task = loop.create_task(c1_reader())
    reader2_task = loop.create_task(c2_reader())

    # Start application logic
    sender_task = loop.create_task(c1_app_sender())
    receiver_task = loop.create_task(c2_app_receiver())

    await asyncio.gather(sender_task, receiver_task)
    # Readers will stop once connections are closed.
    # Ensure tasks are truly finished before exiting gather for them too if needed.
    await asyncio.sleep(1) # Allow readers to process final packets/close down

    # Explicitly cancel reader tasks if they haven't self-terminated
    if not reader1_task.done(): reader1_task.cancel()
    if not reader2_task.done(): reader2_task.cancel()

    try:
        await reader1_task
    except asyncio.CancelledError: pass
    try:
        await reader2_task
    except asyncio.CancelledError: pass

    logger.info("KCP test finished.")


if __name__ == '__main__': # pragma: no cover
    # asyncio.run(main_kcp_test())
    logger.info("KCP Handler module. Contains a minimal KCP implementation.")
    logger.info("Run main_kcp_test() in an async context to test basic KCP loopback.")
    print("To run the test: uncomment `asyncio.run(main_kcp_test())` and execute the file.")
