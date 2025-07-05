# Python Snowflake Client

This directory contains a Python implementation of the Snowflake client, designed to be a pluggable transport for Tor. It aims to replicate the functionality of the Go-based Snowflake client.

## Overview

The Python Snowflake client allows users to connect to the Tor network by routing traffic through ephemeral Snowflake proxies. It operates as a SOCKS proxy that, when launched as a Tor Pluggable Transport, helps users bypass internet censorship.

**Current Status:** This implementation now includes functional KCP and SMUX layers, enabling stream multiplexing over a reliable KCP session on top of WebRTC data channels. This brings its core transport mechanism closer to the Go client's architecture. While these are minimal implementations focused on compatibility, they provide the necessary functionality. Other features like rendezvous mechanisms (HTTP, SQS), SOCKS proxying, and PT protocol integration remain.

## Features

*   SOCKS5 proxy server.
*   KCP protocol layer for reliable packet delivery over WebRTC.
*   SMUX protocol layer for stream multiplexing over KCP.
*   WebRTC communication via `aiortc`.
*   Multiple rendezvous mechanisms for discovering Snowflake proxies:
    *   HTTP/HTTPS Broker
    *   AWS SQS (Note: Requires `aioboto3` and valid AWS credentials/configuration)
    *   AMP Cache (Placeholder, needs constructor refinement based on Go client)
*   Pluggable Transport v2.1 integration for use with Tor.
*   Configuration via command-line arguments.
*   Basic log scrubbing for IP addresses.
*   Filtering of local ICE candidates from SDP offers.

## Limitations

*   **KCP and SMUX Layers**: Minimal Python implementations of KCP and SMUX (Version 2) are now included in `kcp_handler.py` and `smux_handler.py` respectively. These aim for wire-compatibility with the Go client's `xtaci/kcp-go` and `xtaci/smux` libraries, providing stream multiplexing over a KCP-managed reliable session. While functional, they are not full feature ports of the Go libraries and may have different performance characteristics or lack some advanced features of the original KCP/SMUX (e.g., detailed SACK, complex congestion control beyond what's configured, sophisticated SMUX send-side flow control).
*   **NAT Type Detection**: Automatic NAT type detection (like the Go client's `updateNATType`) is not yet implemented. It defaults to "unknown".
*   **TOR_PT_PROXY**: Full support for using a proxy specified by `TOR_PT_PROXY` for rendezvous communication is work-in-progress.
*   **Comprehensive Testing**: While unit tests for utilities and some components exist, full integration and end-to-end tests are pending.
*   **Log Scrubbing**: IP address scrubbing is basic. More comprehensive scrubbing is needed for production environments.
*   **AmpCacheRendezvous**: The AMP Cache rendezvous method needs its constructor updated to accept the target broker URL, similar to the Go client, before it can be fully functional.

## Dependencies

Python 3.8+ is recommended.

Core dependencies are listed in `requirements.txt`. Install them using pip:
```bash
pip install -r requirements.txt
```
This includes:
*   `aiortc`: For WebRTC.
*   `aiortc`: For WebRTC.
*   `curl_cffi`: For HTTP/S requests with browser impersonation (used by HTTP and AMPCache rendezvous).
*   `boto3` and `aioboto3`: For AWS SQS rendezvous (optional, only needed if using SQS).
*   `socksio`: For SOCKS protocol handling.

## Directory Structure

```
client_py/
├── config.py               # Command-line argument parsing and configuration
├── kcp_handler.py          # KCP protocol implementation
├── pt_launcher.py          # Main script for Pluggable Transport integration
├── rendezvous.py           # Rendezvous mechanisms (HTTP, SQS, AMPCache)
├── safe_logger.py          # Safe log formatter for redacting sensitive info
├── smux_handler.py         # SMUX (v2) protocol implementation
├── snowflake_conn.py       # Manages Snowflake connections (Peers, WebRTC, KCP, SMUX)
├── socks_proxy.py          # SOCKS5 proxy implementation
├── utils.py                # Helper utilities (e.g., SDP filtering)
├── webrtc_handler.py       # WebRTC peer connection and data channel logic
├── requirements.txt        # Python package dependencies
└── README.md               # This file
tests/                      # Unit tests
├── test_config.py
├── test_rendezvous.py
├── test_safe_logger.py
└── test_utils.py
```

## Usage

### As a Standalone SOCKS Proxy (for testing)

You can run the SOCKS proxy directly. This is useful for testing the connection logic without Tor.
```bash
python -m client_py.socks_proxy --url <BROKER_URL> --ice <STUN_SERVER_URL>
```
Example:
```bash
python -m client_py.socks_proxy --url https://snowflake-broker.torproject.net/ --ice stun:stun.l.google.com:19302
```
This will start a SOCKS proxy (default: `127.0.0.1:1080`). Configure your application to use this SOCKS proxy.

### As a Tor Pluggable Transport

The client is designed to be launched by Tor.
1.  Ensure the Python client (`pt_launcher.py` and other modules) is accessible.
2.  Configure Tor (typically `torrc`) to use the Python Snowflake client as a pluggable transport.

Example `torrc` lines:
```torrc
UseBridges 1
ClientTransportPlugin snowflake exec python3 /path/to/client_py/pt_launcher.py --url <BROKER_URL> --ice <STUN_SERVER_URL> [other options]
Bridge snowflake <FINGERPRINT> # Get bridge line from BridgeDB or other sources
```
Replace `/path/to/client_py/pt_launcher.py` with the actual path to the script.
You can pass Snowflake client options (like `--url`, `--ice`, `--fronts`, etc.) after the script path.

Tor will set environment variables like `TOR_PT_CLIENT_TRANSPORTS=snowflake` and `TOR_PT_STATE_LOCATION`. The `pt_launcher.py` script handles these according to the PT specification.

### Configuration Options

Run `python -m client_py.pt_launcher --help` (or `python -m client_py.config --help`) to see all available command-line options:

*   `--ice`: Comma-separated list of ICE servers.
*   `--url`: URL of the Snowflake broker.
*   `--fronts`: Comma-separated list of front domains for domain fronting.
*   `--ampcache`: URL of AMP cache for rendezvous.
*   `--sqsqueue`: URL of SQS Queue for rendezvous.
*   `--sqscreds`: Credentials for SQS Queue (base64 encoded JSON or path to file).
*   `--log`: Name of the log file.
*   `--log-to-state-dir`: Resolve log file relative to Tor's PT state directory.
*   `--keep-local-addresses`: Keep local LAN address ICE candidates in SDP.
*   `--unsafe-logging`: Disable log scrubbing (logs IP addresses, etc.).
*   `--max`: Maximum number of WebRTC peers to connect to (default: 1).
*   `--impersonate-profile`: Browser profile for HTTP/S rendezvous (e.g., "chrome120", "firefox110", "none"; default: "chrome120").
*   `--version`: Display version information and quit.

## Testing

Unit tests are located in the `tests/` directory and can be run using Python's `unittest` module:
```bash
python -m unittest discover tests/
```
Or run individual test files:
```bash
python -m unittest tests.test_utils
```

## Contributing

(Placeholder for contribution guidelines if this were a public project)
```
