import logging
import re

# Basic regex for IPv4 and common forms of IPv6
# This is NOT exhaustive and may not cover all cases or may have false positives.
# For production, a more robust IP address detection library might be needed.
IP_REGEX = re.compile(
    r"(\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b|"  # IPv4
    r"\b(?:[A-F0-9]{1,4}:){7}[A-F0-9]{1,4}\b|"  # Full IPv6
    r"\b(?:[A-F0-9]{1,4}:){1,7}:(?:[A-F0-9]{1,4}:){1,7}\b|" # Compressed IPv6 (::)
    r"::(?:[A-F0-9]{1,4}:){0,5}[A-F0-9]{1,4}\b|" # Leading ::
    r"(?:[A-F0-9]{1,4}:){1,6}::\b" # Trailing ::
    r")",
    re.IGNORECASE
)
REDACTED_IP = "[IP_REDACTED]"

# TODO: Add regex for URLs or other sensitive info if needed.
# BROKER_URL_REGEX = re.compile(r"https://snowflake-broker[^/\s]+")
# REDACTED_BROKER = "[BROKER_URL_REDACTED]"

class SafeLogFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', validate=True, *, defaults=None):
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        self.unsafe_logging = False # Default to safe logging

    def set_unsafe_logging(self, unsafe: bool):
        self.unsafe_logging = unsafe

    def formatMessage(self, record: logging.LogRecord) -> str:
        # Get the normally formatted message string
        s = super().formatMessage(record)

        if self.unsafe_logging:
            return s
        else:
            # Apply scrubbing rules
            s = IP_REGEX.sub(REDACTED_IP, s)
            # s = BROKER_URL_REGEX.sub(REDACTED_BROKER, s) # Example for other redactions
            return s

class UnsafeLogFormatter(logging.Formatter):
    """A standard formatter, used when unsafe logging is enabled."""
    def __init__(self, fmt=None, datefmt=None, style='%', validate=True, *, defaults=None):
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)

if __name__ == '__main__': # pragma: no cover
    # Example usage:
    logger = logging.getLogger("safe_logger_test")
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()

    # Test safe formatter
    safe_formatter = SafeLogFormatter("[%(levelname)s] %(message)s")
    console_handler.setFormatter(safe_formatter)
    logger.addHandler(console_handler)

    logger.info("This is a log with an IP 192.168.1.1 and another 2001:0db8:85a3:0000:0000:8a2e:0370:7334.")
    logger.info("Broker URL might be https://snowflake-broker.torproject.net/ but not redacted by default.")

    # Test unsafe formatter
    logger.info("Switching to unsafe logging...")
    safe_formatter.set_unsafe_logging(True)
    logger.info("This is an unsafe log with IP 10.0.0.1 and broker https://snowflake-broker.torproject.net/.")

    # Test direct unsafe formatter (if preferred over toggling)
    # unsafe_formatter = UnsafeLogFormatter("[%(levelname)s] %(message)s (unsafe)")
    # console_handler.setFormatter(unsafe_formatter)
    # logger.info("This is a test from the direct unsafe formatter with IP 172.16.0.1.")

    # Example of how pt_launcher might set it up:
    # log_formatter = SafeLogFormatter(...)
    # if config.unsafe_logging:
    #     log_formatter.set_unsafe_logging(True)
    # else:
    #     log_formatter.set_unsafe_logging(False) # Default
    # handler.setFormatter(log_formatter)

    # To test with different format strings:
    detailed_formatter = SafeLogFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(detailed_formatter)
    logger.info("A detailed message with IP 2.3.4.5 for safe formatter.")
    detailed_formatter.set_unsafe_logging(True)
    logger.info("A detailed message with IP 6.7.8.9 for unsafe formatter.")
