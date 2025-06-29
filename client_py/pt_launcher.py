import asyncio
import logging
import os
import sys
import signal
import ipaddress
from typing import List, Optional, Dict

from client_py.config import ClientConfig, parse_args, DEFAULT_SNOWFLAKE_CAPACITY
from client_py.socks_proxy import run_socks_server

# PT Output Functions
# The main logger for the pt_launcher module.
# We'll use this to emit PT-formatted log messages.
pt_logger_instance = logging.getLogger("pt_launcher.pt_protocol") # Dedicated child logger

# Custom LogRecord attribute to distinguish PT protocol messages
class PTLogRecord(logging.LogRecord):
    def __init__(self, *args, **kwargs):
        self.pt_severity = kwargs.pop('pt_severity', 'notice')
        super().__init__(*args, **kwargs)

class PTLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        if hasattr(record, 'pt_severity'):
            # This is a PT protocol log message
            return f"LOG SEVERITY={record.pt_severity} MESSAGE={record.getMessage()}"
        else:
            # Standard application log message (already formatted by main handler)
            # This formatter should only be used for a handler dedicated to PT LOGS if we split them.
            # For simplicity, if the main handler gets this, it should already be formatted.
            # Let's assume this formatter is ONLY for PT messages sent to stdout.
            # If pt_log uses the main logger, the main formatter needs to handle this.
            # This design is getting complicated.

            # Simpler: pt_log uses print directly if it's only for PT stdout.
            # If we want PT LOG messages to also go to file log, then integration is needed.
            # The Go client's pt.Log prints directly. Let's stick to that for PT protocol messages.
            # Application logs use the standard logging system.

            # Reverting to direct print for PT protocol messages for simplicity and spec adherence.
            # The logging setup below is for application diagnostics, not PT spec LOG lines.
            return super().format(record)


def pt_protocol_message(message: str):
    """Prints a raw message to stdout for PT communication."""
    print(message, flush=True)
    # Optionally, also log this to the application log for debugging PT interactions
    # logger.debug(f"PT_STDOUT: {message}")


# Redefine pt_log to use the above for clarity
def pt_log_msg(message: str, severity: str = "notice"):
    # This is for emitting "LOG SEVERITY=..." lines as part of PT protocol
    pt_protocol_message(f"LOG SEVERITY={severity} MESSAGE={message}")

def pt_version(version_string: str):
    pt_protocol_message(f"VERSION {version_string}")

def pt_cmethod(transport_name: str, listener_addr: str, port: int, options: Optional[Dict[str,str]] = None):
    # options are key=value,key=value
    options_str = ""
    if options:
        options_str = ",".join([f"{k}={v}" for k,v in options.items()])
        print(f"CMETHOD {transport_name} socks5 {listener_addr}:{port} ARGS:{options_str}", flush=True)
    else:
        print(f"CMETHOD {transport_name} socks5 {listener_addr}:{port}", flush=True)


def pt_cmethod_error(transport_name: str, message: str):
    print(f"CMETHOD-ERROR {transport_name} {message}", flush=True)

def pt_cmethods_done():
    print("CMETHODS DONE", flush=True)

def pt_proxy_done():
    print("PROXY DONE", flush=True)

def pt_proxy_error(message: str):
    print(f"PROXY ERROR {message}", flush=True)


# Global shutdown event
shutdown_event = asyncio.Event()

def signal_handler(sig, frame):
    logger.warning(f"Received signal {sig}, initiating shutdown.")
    shutdown_event.set()

async def watch_stdin_close():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    try:
        await reader.read(-1) # Wait for EOF
    except Exception as e:
        logger.debug(f"Stdin watch error (expected on close): {e}")
    finally:
        logger.info("Stdin closed, signaling shutdown.")
        shutdown_event.set()


logger = logging.getLogger("pt_launcher") # Using a specific logger for PT launcher

async def main_pt():
    # 0. Initial logging setup (minimal, actual logging config comes from args)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("Python Snowflake Client PT Launcher starting...")

    # 1. Parse command line arguments first
    cli_args = parse_args() # This now uses argparse directly

    # Override ClientConfig with PT environment variables
    # This is a bit of a mix, usually PT env vars dictate behavior more strongly.
    # The Go client seems to let command line args override PT state sometimes.
    # For now, let cli_args be the base, and PT env vars can fill in gaps or guide PT-specific behavior.

    pt_config = ClientConfig(**cli_args.__dict__) # Start with CLI args

    from client_py.safe_logger import SafeLogFormatter

    # PT Environment Variables
    state_location = os.environ.get("TOR_PT_STATE_LOCATION", ".")
    client_transports_str = os.environ.get("TOR_PT_CLIENT_TRANSPORTS", "")
    # managed_transport_ver = os.environ.get("TOR_PT_MANAGED_TRANSPORT_VER", "1") # We aim for 2.1
    exit_on_stdin_close = os.environ.get("TOR_PT_EXIT_ON_STDIN_CLOSE") == "1"
    pt_proxy_env = os.environ.get("TOR_PT_PROXY")

    # Version reporting (use a fixed version or import from a version file)
    # TODO: Centralize version string
    client_version = "0.1.0-python-dev"
    pt_version(client_version) # Report our client version

    # Configure logging based on args (log file, unsafe logging)
    log_file_path = pt_config.log_file
    if pt_config.log_to_state_dir and log_file_path:
        if not os.path.exists(state_location):
            try:
                os.makedirs(state_location, exist_ok=True)
                logger.info(f"Created state directory: {state_location}")
            except OSError as e:
                logger.error(f"Failed to create state directory {state_location}: {e}")
                # PT spec doesn't have a specific error for this. Maybe log and continue?
                # For now, let it potentially fail on log file open.
        log_file_path = os.path.join(state_location, log_file_path)


    # Configure logging (application logs, not PT protocol LOG lines)
    # Clear any existing handlers (e.g., from basicConfig)
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    log_formatter = SafeLogFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_formatter.set_unsafe_logging(pt_config.unsafe_logging) # Toggle scrubbing

    if log_file_path:
        try:
            file_handler = logging.FileHandler(log_file_path, mode='a')
            file_handler.setFormatter(log_formatter)
            root_logger.addHandler(file_handler)
            logger.info(f"Application logging to file: {log_file_path} (Unsafe: {pt_config.unsafe_logging})")
        except Exception as e:
            pt_log_msg(f"Failed to open log file {log_file_path} for application logs: {e}", severity="err")
            # Fallback to stderr for application logs if file logging fails
            stderr_handler = logging.StreamHandler(sys.stderr)
            stderr_handler.setFormatter(log_formatter)
            root_logger.addHandler(stderr_handler)
            logger.warning(f"Application logging to stderr due to file error. (Unsafe: {pt_config.unsafe_logging})")
    else:
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(log_formatter)
        root_logger.addHandler(stderr_handler)
        logger.info(f"Application logging to stderr. (Unsafe: {pt_config.unsafe_logging})")

    # Set global log level for application logs
    # PT protocol LOG lines are printed directly and not affected by this level.
    root_logger.setLevel(logging.DEBUG if pt_config.unsafe_logging else logging.INFO)


    logger.info(f"PT State Location: {state_location}")
    logger.info(f"PT Client Transports: {client_transports_str}")
    # logger.info(f"PT Managed Transport Version: {managed_transport_ver}") # We are 2.1 compliant by spec
    logger.info(f"PT Exit on Stdin Close: {exit_on_stdin_close}")
    logger.info(f"PT Proxy: {pt_proxy_env if pt_proxy_env else 'Not set'}")

    if pt_proxy_env:
        # The Go client checks TOR_PT_PROXY and if set, uses it for broker comms.
        # It also does a proxy test (SOCKS5 UDP).
        # Our current HTTP/SQS clients don't use this TOR_PT_PROXY yet.
        # This needs to be plumbed into httpx/boto3 configurations.
        # For now, just acknowledge and report.
        logger.warning(f"TOR_PT_PROXY ({pt_proxy_env}) is set, but Python client doesn't fully support it for rendezvous yet.")
        # Simulate proxy check (very basic)
        if "socks5" in pt_proxy_env.lower(): # Very naive check
            pt_log("TOR_PT_PROXY seems to be a SOCKS5 proxy. Python client support for this is WIP.")
            pt_proxy_done() # Assuming it would work for now.
        else:
            pt_proxy_error(f"Unsupported proxy type in TOR_PT_PROXY: {pt_proxy_env}")
            # This should be a fatal error for Tor if proxy is mandatory.
            # For now, we might continue but rendezvous might fail.
            # Let's make it fatal to match expected Tor behavior if proxy fails.
            pt_cmethods_done() # Signal no methods will start
            return 1


    requested_transports = [t.strip() for t in client_transports_str.split(',') if t.strip()]
    if not requested_transports or "snowflake" not in requested_transports:
        pt_log("No recognized client transports requested (expected 'snowflake'). Exiting.", severity="err")
        pt_cmethods_done()
        return 1

    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if exit_on_stdin_close:
        asyncio.create_task(watch_stdin_close())
        logger.info("Watching stdin for close.")

    # Start SOCKS server for "snowflake" transport
    socks_server_task = None
    try:
        # Listen on 127.0.0.1:0 to get a dynamic port
        # The ClientConfig used by run_socks_server should be the one derived from CLI + PT env vars.
        # `pt_config` holds this merged configuration.

        # `run_socks_server` prints the "SOCKS proxy active on host:port" message.
        # We need to capture that to report CMETHOD.
        # Modify run_socks_server or use a queue to get the address.
        # For now, let's assume run_socks_server is modified to return the listener address.

        # Let's make run_socks_server take a ready_event and set it with host/port
        server_ready_event = asyncio.Event()
        server_details = {} # To store host/port

        async def socks_server_wrapper():
            nonlocal server_details
            try:
                # Pass a callback to run_socks_server to get host/port
                def on_server_started(host, port):
                    server_details['host'] = host
                    server_details['port'] = port
                    server_ready_event.set()

                # This requires run_socks_server to accept on_started_callback
                # Temporarily, we will hardcode 127.0.0.1 and assume dynamic port will be found
                # This is a simplification for now.
                # A better way is for run_socks_server to return the server object,
                # from which we get the port.

                # Simplification: start server and assume it binds to 127.0.0.1, port will be known after start.
                # The `run_socks_server` already prints the address.
                # We need to start it, get the port, then print CMETHOD.

                # We'll start it on 127.0.0.1:0 and then try to get the actual port.
                # This is tricky as `serve_forever` blocks.
                # Solution: `start_server` returns a Server object.

                server = await asyncio.start_server(
                    lambda r, w: handle_socks_client(r, w, pt_config), # handle_socks_client is from socks_proxy
                    "127.0.0.1", 0 # Listen on 127.0.0.1, dynamic port
                )
                bound_addr = server.sockets[0].getsockname()
                logger.info(f"SOCKS server for Snowflake listening on {bound_addr[0]}:{bound_addr[1]}")

                # Report CMETHOD
                # The Go client can pass SOCKS args via CMETHOD ARGS: field.
                # Example: CMETHOD snowflake socks5 127.0.0.1:43067 ARGS:ampcache=...,front=...
                # These would be the *default* args for SOCKS if client doesn't override.
                # For now, not passing ARGS via CMETHOD as our config is global.
                pt_cmethod("snowflake", bound_addr[0], bound_addr[1])
                pt_cmethods_done() # Signal that all methods are up

                async with server:
                    await server.serve_forever()

            except Exception as e:
                logger.error(f"SOCKS server failed to start or run: {e}", exc_info=True)
                pt_cmethod_error("snowflake", f"SOCKS server error: {e}")
                pt_cmethods_done() # Still need to send this if other methods might have succeeded (none here)
                shutdown_event.set() # Trigger shutdown if server fails critically

        socks_server_task = asyncio.create_task(socks_server_wrapper())

        # Wait for shutdown signal
        await shutdown_event.wait()
        logger.info("Shutdown signal received, terminating SOCKS server task.")

        if socks_server_task and not socks_server_task.done():
            socks_server_task.cancel()
            await asyncio.wait_for(socks_server_task, timeout=5.0) # Wait for it to finish

        logger.info("Python Snowflake Client PT Launcher finished.")
        return 0

    except asyncio.CancelledError:
        logger.info("Main PT task cancelled.")
    except Exception as e:
        pt_log(f"Fatal error in PT launcher: {e}", severity="err")
        logger.error(f"Fatal error in PT launcher: {e}", exc_info=True)
        # Ensure CMETHODS DONE is sent if we error out before it.
        # This depends on where the error occurs.
        # If socks_server_task is not created or fails early, CMETHODS DONE might not be sent.
        # Consider a try/finally for pt_cmethods_done if critical.
        return 1
    finally:
        # Final cleanup if any
        pass


if __name__ == "__main__":
    # This script is intended to be run by Tor.
    # Example of how Tor might run it (simplified):
    # TOR_PT_STATE_LOCATION=/path/to/tor/pt_state/snowflake-py
    # TOR_PT_CLIENT_TRANSPORTS=snowflake
    # TOR_PT_EXIT_ON_STDIN_CLOSE=1
    # python -m client_py.pt_launcher --log client.log --log-to-state-dir --url <broker_url> ...

    # To run manually for testing PT aspects:
    # Set env vars like TOR_PT_CLIENT_TRANSPORTS=snowflake
    # Then run: python client_py/pt_launcher.py --ice stun:stun.l.google.com:19302 --url https://snowflake-broker.torproject.net/

    # Ensure the current directory is in PYTHONPATH if running as a script
    # For example, run from the project root: python -m client_py.pt_launcher ...

    if sys.platform == "win32": # Proactor event loop for subprocesses, signals on Windows
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    try:
        return_code = asyncio.run(main_pt())
        sys.exit(return_code)
    except KeyboardInterrupt:
        logger.info("Launcher interrupted by KeyboardInterrupt.")
        sys.exit(1)
    except Exception as e: # Catch-all for unexpected errors during asyncio.run itself
        print(f"Critical error launching PT client: {e}", file=sys.stderr)
        sys.exit(1)

def main_cli_entry():
    """
    Entry point for the console script defined in setup.py.
    Handles setting up the asyncio loop and running the main PT logic.
    """
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    try:
        return_code = asyncio.run(main_pt())
        sys.exit(return_code)
    except KeyboardInterrupt:
        # main_pt already logs this if it's caught there.
        # This is a fallback if KeyboardInterrupt happens during asyncio.run setup/teardown.
        print("Launcher interrupted by KeyboardInterrupt (main_cli_entry).", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Critical error in PT client CLI entry: {e}", file=sys.stderr)
        sys.exit(1)
