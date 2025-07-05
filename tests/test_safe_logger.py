import unittest
import logging
from io import StringIO

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from client_py.safe_logger import SafeLogFormatter, REDACTED_IP

class TestSafeLogFormatter(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger("test_safe_logger_output")
        self.logger.handlers = [] # Clear any existing handlers
        self.logger.setLevel(logging.INFO)
        self.stream = StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.logger.addHandler(self.handler)

    def tearDown(self):
        self.logger.removeHandler(self.handler)
        self.handler.close()

    def test_safe_logging_redacts_ips(self):
        formatter = SafeLogFormatter("[%(levelname)s] %(message)s")
        formatter.set_unsafe_logging(False) # Ensure safe logging is on
        self.handler.setFormatter(formatter)

        self.logger.info("IPs 192.168.1.1 and 10.0.0.2 should be redacted.")
        self.logger.info("IPv6 fe80::1 and 2001:db8::dead:beef also.")
        self.logger.info("No IP here.")
        # Test with IP at the beginning and end
        self.logger.info("172.16.0.1 is at the start.")
        self.logger.info("Ends with 172.31.255.254")


        log_output = self.stream.getvalue()

        self.assertIn(f"IPs {REDACTED_IP} and {REDACTED_IP} should be redacted.", log_output)
        self.assertIn(f"IPv6 {REDACTED_IP} and {REDACTED_IP} also.", log_output)
        self.assertIn("No IP here.", log_output)
        self.assertIn(f"{REDACTED_IP} is at the start.", log_output)
        self.assertIn(f"Ends with {REDACTED_IP}", log_output)

        self.assertNotIn("192.168.1.1", log_output)
        self.assertNotIn("10.0.0.2", log_output)
        self.assertNotIn("fe80::1", log_output)
        self.assertNotIn("2001:db8::dead:beef", log_output)

    def test_unsafe_logging_does_not_redact_ips(self):
        formatter = SafeLogFormatter("[%(levelname)s] %(message)s")
        formatter.set_unsafe_logging(True) # Ensure unsafe logging is on
        self.handler.setFormatter(formatter)

        self.logger.info("IPs 192.168.1.1 and 10.0.0.2 should NOT be redacted.")
        self.logger.info("IPv6 fe80::1 and 2001:db8::dead:beef also NOT.")

        log_output = self.stream.getvalue()

        self.assertNotIn(REDACTED_IP, log_output)
        self.assertIn("192.168.1.1", log_output)
        self.assertIn("10.0.0.2", log_output)
        self.assertIn("fe80::1", log_output)
        self.assertIn("2001:db8::dead:beef", log_output)

    def test_default_formatter_is_safe(self):
        formatter = SafeLogFormatter("[%(levelname)s] %(message)s")
        # By default, unsafe_logging should be False
        self.handler.setFormatter(formatter)

        self.logger.info("Default test with IP 192.168.10.1.")
        log_output = self.stream.getvalue()
        self.assertIn(REDACTED_IP, log_output)
        self.assertNotIn("192.168.10.1", log_output)


if __name__ == '__main__':
    unittest.main()
