import unittest
import logging

# Ensure client_py is in path for testing if tests are run from root or tests/
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from client_py.utils import filter_local_addresses_from_sdp, is_local_ip

# Suppress logging during tests unless specifically needed
logging.basicConfig(level=logging.CRITICAL)

class TestUtils(unittest.TestCase):

    def test_is_local_ip(self):
        self.assertTrue(is_local_ip("192.168.1.1"))
        self.assertTrue(is_local_ip("10.0.0.1"))
        self.assertTrue(is_local_ip("172.16.0.1"))
        self.assertTrue(is_local_ip("172.31.255.254"))
        self.assertTrue(is_local_ip("127.0.0.1"))
        self.assertTrue(is_local_ip("::1"))
        self.assertTrue(is_local_ip("fe80::1234:5678:9abc:def0"))
        self.assertTrue(is_local_ip("my-machine.local")) # Heuristic for .local

        self.assertFalse(is_local_ip("8.8.8.8"))
        self.assertFalse(is_local_ip("203.0.113.45"))
        self.assertFalse(is_local_ip("2001:db8::1"))
        self.assertFalse(is_local_ip("example.com"))
        self.assertFalse(is_local_ip("notanip")) # Should not raise error, just return False

    def test_filter_local_addresses_from_sdp_keep_local_true(self):
        sample_sdp = """\
v=0
o=- 1 1 IN IP4 127.0.0.1
a=candidate:foundation1 1 udp 2130706431 192.168.1.100 12345 typ host
a=candidate:foundation2 1 udp 1694498815 8.8.8.8 54321 typ srflx raddr 192.168.1.100 rport 12345
""".replace("\n", "\r\n") + "\r\n"

        filtered_sdp = filter_local_addresses_from_sdp(sample_sdp, keep_local=True)
        self.assertEqual(filtered_sdp, sample_sdp)

    def test_filter_local_addresses_from_sdp_filter_host(self):
        sample_sdp = """\
v=0
o=- 1 1 IN IP4 127.0.0.1
a=ice-ufrag:someufrag
a=candidate:foundation1 1 udp 2130706431 192.168.1.100 12345 typ host
a=candidate:foundation2 1 udp 1694498815 8.8.8.8 9000 typ host
a=candidate:foundation3 1 udp 2130706430 fe80::1 55555 typ host
""".replace("\n", "\r\n") + "\r\n"

        expected_sdp_after_filter = """\
v=0
o=- 1 1 IN IP4 127.0.0.1
a=ice-ufrag:someufrag
a=candidate:foundation2 1 udp 1694498815 8.8.8.8 9000 typ host
""".replace("\n", "\r\n") + "\r\n"

        filtered_sdp = filter_local_addresses_from_sdp(sample_sdp, keep_local=False)
        # Normalizing by splitting lines and removing empty ones for robust comparison
        self.assertEqual(
            [line for line in filtered_sdp.splitlines() if line.strip()],
            [line for line in expected_sdp_after_filter.splitlines() if line.strip()]
        )
        self.assertNotIn("192.168.1.100", filtered_sdp)
        self.assertNotIn("fe80::1", filtered_sdp)
        self.assertIn("8.8.8.8", filtered_sdp)

    def test_filter_local_addresses_from_sdp_srflx_relay(self):
        # srflx/relay candidates should NOT be filtered based on their conn_addr (base) being local,
        # but SHOULD be filtered if their raddr (reflexive/relayed address) is local.
        sample_sdp = """\
v=0
o=- 1 1 IN IP4 127.0.0.1
a=candidate:f1 1 udp 100 192.168.1.10 1000 typ host
a=candidate:f2 1 udp 100 192.168.1.11 1001 typ srflx raddr 8.8.8.8 rport 1001
a=candidate:f3 1 udp 100 8.8.4.4 1002 typ srflx raddr 8.8.4.4 rport 1002
a=candidate:f4 1 udp 100 192.168.1.12 1003 typ relay raddr 10.0.0.1 rport 1003
a=candidate:f5 1 udp 100 7.7.7.7 1004 typ relay raddr 7.7.7.7 rport 1004
a=candidate:f6 1 udp 100 192.168.1.13 1005 typ prflx raddr 6.6.6.6 rport 1005
""".replace("\n", "\r\n") + "\r\n"

        filtered_sdp = filter_local_addresses_from_sdp(sample_sdp, keep_local=False)

        self.assertNotIn("192.168.1.10", filtered_sdp) # Local host filtered
        self.assertIn("raddr 8.8.8.8", filtered_sdp)    # srflx with public raddr remains (even if base is local)
        self.assertIn("raddr 8.8.4.4", filtered_sdp)    # srflx with public raddr and public base remains
        self.assertNotIn("raddr 10.0.0.1", filtered_sdp) # relay with local raddr filtered
        self.assertIn("raddr 7.7.7.7", filtered_sdp)    # relay with public raddr remains
        self.assertIn("raddr 6.6.6.6", filtered_sdp)    # prflx (treat like srflx) remains

    def test_no_candidates_to_filter(self):
        sample_sdp = """\
v=0
o=- 1 1 IN IP4 127.0.0.1
a=candidate:foundation2 1 udp 1694498815 8.8.8.8 9000 typ host
""".replace("\n", "\r\n") + "\r\n"
        filtered_sdp = filter_local_addresses_from_sdp(sample_sdp, keep_local=False)
        self.assertEqual(
            [line for line in filtered_sdp.splitlines() if line.strip()],
            [line for line in sample_sdp.splitlines() if line.strip()]
        )

    def test_sdp_without_candidates(self):
        sample_sdp = "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\n"
        filtered_sdp = filter_local_addresses_from_sdp(sample_sdp, keep_local=False)
        self.assertEqual(filtered_sdp, sample_sdp)

if __name__ == '__main__':
    unittest.main()
