import re
import ipaddress
import logging

logger = logging.getLogger(__name__)

# Regex to find 'a=candidate' lines and capture the IP address part.
# Example candidate line:
# a=candidate:4234997325 1 udp 2043278322 192.168.1.100 54321 typ host generation 0 ufrag abcdefgh network-id 1
# We need to capture the IP address (5th element in "foundation ... ip port ...")
# The regex focuses on the "typ <type> raddr <ip> rport <port>" or "typ <type>" followed by other attributes,
# with the main candidate IP being the one before the port.

# A simpler regex targeting the IP address in a candidate line:
# `a=candidate:... <ip_address> <port> typ ...`
# Candidate line structure: foundation component-id transport priority connection-address port typ ...
CANDIDATE_LINE_REGEX = re.compile(
    r"^(a=candidate:(?P<foundation>\S+) (?P<component_id>\d+) (?P<transport>\S+) (?P<priority>\d+) (?P<conn_addr>\S+) (?P<conn_port>\d+) typ (?P<typ>\S+).*)$",
    re.MULTILINE
)

# For raddr, which indicates the related address for relayed or server reflexive candidates
# a=candidate:... typ srflx raddr 1.2.3.4 rport 1234
# a=candidate:... typ relay raddr 1.2.3.4 rport 1234
RADDR_REGEX = re.compile(r"raddr (?P<raddr_ip>\S+)")


def is_local_ip(ip_str: str) -> bool:
    """
    Checks if an IP address string is considered 'local' for WebRTC purposes.
    This typically means it's not a globally routable address.
    Includes private, loopback, link-local, and unspecified addresses.
    Also includes a heuristic for ".local" hostnames.
    """
    try:
        ip_obj = ipaddress.ip_address(ip_str)
        # An IP is considered "local" if it's not global, or if it's unspecified (like "::" or "0.0.0.0")
        # which are sometimes filtered. is_global correctly identifies RFC1918, loopback, link-local as not global.
        # is_unspecified handles "0.0.0.0" and "::".
        return (not ip_obj.is_global) or ip_obj.is_unspecified
    except ValueError:
        # Could be a hostname (e.g., mDNS .local) - treat as potentially local if it ends with .local
        # This is a heuristic. For WebRTC, .local hostnames are usually for local candidates.
        if isinstance(ip_str, str) and ip_str.lower().endswith(".local"):
            return True
        logger.debug(f"Could not parse IP string: {ip_str}")
        return False # Or True, depending on strictness. Let's be conservative.

def filter_local_addresses_from_sdp(sdp_str: str, keep_local: bool) -> str:
    """
    Filters ICE candidates with local IP addresses from an SDP string.
    If keep_local is True, no filtering is done.
    This is a regex-based approach due to lack of readily available SDP parsing libraries.
    It's potentially fragile if SDP candidate line formats vary significantly.
    """
    if keep_local:
        return sdp_str

    logger.debug("Filtering local addresses from SDP.")
    lines = sdp_str.splitlines(keepends=False) # Keepends false for easier processing
    filtered_lines = []
    modified_count = 0

    for line in lines:
        match = CANDIDATE_LINE_REGEX.match(line)
        if match:
            candidate_data = match.groupdict()
            conn_addr = candidate_data.get("conn_addr")
            candidate_type = candidate_data.get("typ")

            should_filter = False
            if conn_addr and is_local_ip(conn_addr):
                # Main connection address is local.
                # For host candidates, this means it's a local interface.
                # For srflx/relay, this conn_addr is usually the local base of the srflx/relay candidate.
                # The actual external IP for srflx/relay is in raddr.
                if candidate_type == "host":
                    should_filter = True
                    logger.debug(f"Filtering local host candidate: {line}")
                elif candidate_type in ["srflx", "relay"]:
                    # For srflx/relay, the conn_addr is the base. If base is local, it's fine.
                    # We should check raddr if it's also local (though that's unlikely for srflx/relay).
                    raddr_match = RADDR_REGEX.search(line)
                    if raddr_match:
                        raddr_ip = raddr_match.group("raddr_ip")
                        if is_local_ip(raddr_ip):
                            # This is unusual: a reflexive or relayed candidate pointing to a local IP.
                            # This might happen in some NAT hairpinning scenarios or misconfigurations.
                            logger.debug(f"Filtering {candidate_type} candidate with local raddr: {line}")
                            should_filter = True

            if not should_filter:
                filtered_lines.append(line)
            else:
                modified_count += 1
        else:
            filtered_lines.append(line)

    if modified_count > 0:
        logger.info(f"Filtered {modified_count} local ICE candidates from SDP.")
        # Need to re-add newlines. Original SDPs usually use \r\n.
        return "\r\n".join(filtered_lines) + "\r\n"
    else:
        logger.debug("No local candidates found to filter or all were kept.")
        return sdp_str


if __name__ == '__main__': # pragma: no cover
    logging.basicConfig(level=logging.DEBUG)

    sample_sdp_offer_raw = """\
v=0
o=- 5486970968481347889 2 IN IP4 127.0.0.1
s=-
t=0 0
a=msid-semantic: WMS
m=application 9 UDP/DTLS/SCTP webrtc-datachannel
c=IN IP4 0.0.0.0
a=mid:0
a=sctp-port:5000
a=setup:actpass
a=ice-ufrag:someufrag
a=ice-pwd:somepassword
a=candidate:foundation1 1 udp 2130706431 192.168.1.100 12345 typ host generation 0 ufrag someufrag
a=candidate:foundation2 1 udp 1694498815 1.2.3.4 54321 typ srflx raddr 192.168.1.100 rport 12345 generation 0 ufrag someufrag
a=candidate:foundation3 1 udp 1694498815 203.0.113.45 54322 typ srflx raddr 203.0.113.45 rport 54322 generation 0 ufrag someufrag
a=candidate:foundation4 1 udp 2130706430 fe80::1234:5678:9abc:def0 55555 typ host generation 0 ufrag someufrag
a=candidate:foundation5 1 udp 1800000000 10.0.0.5 60000 typ host generation 0 ufrag someufrag
a=candidate:foundation6 1 udp 1800000001 my-machine.local 60001 typ host generation 0 ufrag someufrag
a=candidate:foundation7 1 tcp 1518280447 172.16.0.10 9 typ host tcptype active generation 0 ufrag someufrag
a=rtcp-mux
"""
    # Ensure SDP strings end with \r\n if that's what aiortc produces/expects
    sample_sdp_offer = sample_sdp_offer_raw.replace("\n", "\r\n")


    print("--- Original SDP ---")
    print(sample_sdp_offer)

    print("\n--- SDP with local addresses filtered (keep_local=False) ---")
    filtered_sdp = filter_local_addresses_from_sdp(sample_sdp_offer, keep_local=False)
    print(filtered_sdp)
    assert "192.168.1.100" not in filtered_sdp # host candidate with this IP should be gone
    assert "raddr 192.168.1.100" in filtered_sdp # srflx candidate with this raddr (base) should remain, as its main IP 1.2.3.4 is public
    assert "203.0.113.45" in filtered_sdp # public srflx should remain
    assert "fe80::1234" not in filtered_sdp # link-local IPv6 host candidate should be gone
    assert "10.0.0.5" not in filtered_sdp # private 10.x host candidate
    assert "my-machine.local" not in filtered_sdp # .local hostname candidate
    assert "172.16.0.10" not in filtered_sdp # private 172.16.x host candidate


    print("\n--- SDP with local addresses kept (keep_local=True) ---")
    unfiltered_sdp = filter_local_addresses_from_sdp(sample_sdp_offer, keep_local=True)
    print(unfiltered_sdp)
    assert "192.168.1.100" in unfiltered_sdp
    assert "fe80::1234" in unfiltered_sdp

    # Test a candidate with local raddr (should be filtered if type is srflx/relay and raddr itself is local)
    sdp_with_local_raddr_srflx = """\
v=0
o=- 1 1 IN IP4 127.0.0.1
s=-
t=0 0
m=application 9 UDP/DTLS/SCTP webrtc-datachannel
c=IN IP4 0.0.0.0
a=ice-ufrag:test
a=ice-pwd:test
a=candidate:1 1 udp 100 1.2.3.4 10000 typ srflx raddr 192.168.1.5 rport 10000
a=candidate:2 1 udp 100 2.3.4.5 10001 typ host
""".replace("\n", "\r\n")
    print("\n--- SDP with srflx candidate having local raddr ---")
    print(sdp_with_local_raddr_srflx)
    filtered_raddr_sdp = filter_local_addresses_from_sdp(sdp_with_local_raddr_srflx, keep_local=False)
    print("\n--- Filtered version ---")
    print(filtered_raddr_sdp)
    assert "raddr 192.168.1.5" not in filtered_raddr_sdp # This srflx candidate should be removed
    assert "2.3.4.5" in filtered_raddr_sdp # The public host candidate should remain


    logger.info("Utility tests completed.")
