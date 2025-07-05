import logging
import re

# Basic regex for IPv4 and common forms of IPv6
# This is NOT exhaustive and may not cover all cases or may have false positives.
# For production, a more robust IP address detection library might be needed.

# Regex for IPv4
IPV4_REGEX_STR = r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"

# Regex for IPv6 (covering various forms including compressed)
# Order: Full, then general compressed. Specific short forms like '::' alone are covered by general.
IPV6_REGEX_STR = (
    # 1. Full, uncompressed (8 hex groups)
    r"\b(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}\b|"
    # 2. General compressed form (contains ::)
    #    Matches X::Y where X and Y can be empty or multiple hex groups.
    #    e.g., ::1, fe80::1, 1:2::3:4, 1::, ::
    #    Pattern: (optional_left_part) :: (optional_right_part)
    #    Left part: hex_group followed by optional more hex_groups_with_colons
    #    Right part: similar
    #    This should be non-greedy for the hex_group parts if possible, or structured to avoid issues.
    #    A common robust regex for general IPv6 (including all :: forms):
    r"\b((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?::(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)\b|"
    # Handle cases like :: by itself if the above doesn't catch it due to word boundaries or group presence.
    # The above general compressed form should catch '::' if word boundaries are appropriate.
    # Also, specific forms if the general one has issues with edge cases like `::` or `1:2:3:4:5:6:7::`
    r"\b::\b" # Just "::"
)

IP_REGEX = re.compile(f"({IPV4_REGEX_STR})|({IPV6_REGEX_STR.strip('|')})", re.IGNORECASE)
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
