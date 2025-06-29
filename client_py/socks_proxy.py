import asyncio
import logging
import ipaddress
from typing import Optional

import socksio # type: ignore
from socksio import socks5 # type: ignore

from client_py.config import ClientConfig, DEFAULT_SNOWFLAKE_CAPACITY
from client_py.snowflake_conn import SnowflakeConn
from client_py.rendezvous import create_rendezvous_from_config # Use the factory

logger = logging.getLogger(__name__)

# Helper to parse SOCKS options if present (similar to Go client)
def _apply_socks_options(base_config: ClientConfig, socks_args: dict) -> ClientConfig:
    # Create a new config object to avoid modifying the base one for other connections
    # This is a shallow copy, careful if config objects become more complex
    config = ClientConfig(**base_config.__dict__)

    if arg := socks_args.get("ampcache"):
        config.ampcache_url = arg
    if arg := socks_args.get("sqsqueue"):
        config.sqs_queue_url = arg
    if arg := socks_args.get("sqscreds"):
        config.sqs_creds_str = arg

    fronts_str = None
    if arg := socks_args.get("fronts"): # Plural first
        fronts_str = arg
    elif arg := socks_args.get("front"): # Singular legacy
        fronts_str = arg

    if fronts_str is not None: # Allow empty string to clear fronts
        config.front_domains = [f.strip() for f in fronts_str.split(',') if f.strip()] if fronts_str else []

    if arg := socks_args.get("ice"):
        config.ice_servers = [s.strip() for s in arg.split(',') if s.strip()] if arg else []

    if arg := socks_args.get("max"):
        try:
            max_val = int(arg)
            config.max_peers = max_val
        except ValueError:
            logger.warning(f"Invalid SOCKS arg for 'max': {arg}. Using default: {config.max_peers}")

    if arg := socks_args.get("url"):
        config.broker_url = arg

    # utls-nosni, utls-imitate, fingerprint are not yet in ClientConfig, add if needed
    # if arg := socks_args.get("utls-nosni"): ...
    # if arg := socks_args.get("utls-imitate"): ...
    # if arg := socks_args.get("fingerprint"): ...
    logger.info(f"Applied SOCKS options. New config (sample): broker_url='{config.broker_url}', max_peers={config.max_peers}")
    return config


async def copy_data(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, connection_name: str):
    """Copies data from reader to writer until EOF or error."""
    try:
        while not reader.at_eof():
            data = await reader.read(4096)  # Read in chunks
            if not data:
                break  # EOF
            writer.write(data)
            await writer.drain()
    except asyncio.CancelledError:
        logger.info(f"Copy task for {connection_name} cancelled.")
        raise
    except ConnectionResetError:
        logger.info(f"{connection_name} connection reset.")
    except Exception as e:
        logger.error(f"Error in {connection_name} copy_data: {e}", exc_info=True)
    finally:
        writer.close()
        # await writer.wait_closed() # wait_closed can hang if peer doesn't close nicely
        logger.info(f"{connection_name} copy task finished.")


async def handle_socks_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    base_app_config: ClientConfig
):
    conn = socksio. νέος_σύνδεσμος() # Connection object from socksio
    client_addr = client_writer.get_extra_info('peername')
    logger.info(f"SOCKS: New client connection from {client_addr}")

    try:
        # 1. SOCKS Handshake
        server_hello = socksio.SOCKS5ServerHello(method=socksio.SOCKS5AuthMethod.NO_AUTH_REQUIRED)
        client_writer.write(server_hello.send())
        await client_writer.drain()

        data = await client_reader.read(1024)
        event = conn.receive_data(data)
        if not event: # Needs more data
            data = await client_reader.read(1024)
            event = conn.receive_data(data)

        if not isinstance(event, socksio.SOCKS5Request):
            logger.warning(f"SOCKS: Expected SOCKS5Request, got {type(event)}. Closing.")
            client_writer.close()
            return

        logger.info(f"SOCKS: Received request: {event.command} for {event.address}:{event.port}")

        if event.command != socksio.SOCKS5Command.CONNECT:
            logger.warning(f"SOCKS: Unsupported command {event.command}. Sending error.")
            reply = socksio.SOCKS5Reply(
                reply_type=socksio.SOCKS5ReplyType.COMMAND_NOT_SUPPORTED,
                address=ipaddress.ip_address("0.0.0.0"), # Dummy address
                port=0
            )
            client_writer.write(reply.send())
            await client_writer.drain()
            client_writer.close()
            return

        # Parse SOCKS options (from event.address if it's a domain name with parameters)
        # socksio doesn't directly parse Tor's SOCKS arg format.
        # This is a Tor-specific extension where the domain name in the CONNECT request
        # can be "example.com foo=bar baz=qux".
        # We'll assume for now that event.address is just the target host and options are not passed this way
        # by standard SOCKS clients. If PT mode passes them, that's handled by goptlib parsing.
        # The Go client parses them from conn.Req.Args.
        # For a standalone Python SOCKS server, we might not get these easily.
        # For now, we'll use the base_app_config, potentially overridden by PT env vars later.

        # TODO: Figure out how SOCKS options are passed and parsed here if not via PT mechanism.
        # For now, let's assume base_app_config is what we use, possibly modified by PT variables.
        current_config = base_app_config
        # If conn.Req.Args were available (as in Go's PT library context):
        # current_config = _apply_socks_options(base_app_config, conn.Req.Args)


        # 2. Initiate Snowflake Connection
        logger.info(f"SOCKS: Attempting to dial Snowflake for target {event.address}:{event.port}")


        rendezvous_method = create_rendezvous_from_config(current_config)
        if not rendezvous_method:
            # Error already logged by create_rendezvous_from_config
            raise ConnectionRefusedError("Failed to initialize rendezvous method from configuration.")

        snowflake_conn = SnowflakeConn(current_config, rendezvous_method)

        # This is where it gets tricky with KCP/SMUX stubs.
        # `dial()` returns None because SMUX stream isn't really created.
        # For testing SOCKS, we might temporarily make `dial` return the raw `PeerConnection`
        # or its `WebRTCHandler` and try to proxy data over that *single* data channel.
        # This is NOT the final architecture.

        # Conceptual final goal:
        # smux_stream = await snowflake_conn.dial() # Returns a SmuxStream-like object
        # if not smux_stream:
        #     raise ConnectionRefusedError("Failed to dial Snowflake (SMUX stream not established).")

        # Temporary workaround for testing SOCKS layer:
        # Let's assume dial sets up snowflake_conn.underlying_connection (PeerConnection)
        await snowflake_conn.dial()
        if not snowflake_conn.underlying_connection or \
           not snowflake_conn.underlying_connection.is_connected or \
           not snowflake_conn.underlying_connection.web_rtc_handler.data_channel:
            logger.error("SOCKS: Snowflake dial completed but no usable underlying WebRTC connection found.")
            raise ConnectionRefusedError("Snowflake dial failed to provide a usable WebRTC channel.")

        logger.info("SOCKS: Snowflake connection (conceptual WebRTC peer) established.")

        # 3. Send SOCKS success reply
        # The address and port in the reply should be the bound address on the server side,
        # typically 0.0.0.0:0 if not meaningful.
        reply = socksio.SOCKS5Reply(
            reply_type=socksio.SOCKS5ReplyType.SUCCESS,
            address=ipaddress.ip_address("0.0.0.0"),
            port=0
        )
        client_writer.write(reply.send())
        await client_writer.drain()
        logger.info(f"SOCKS: Success reply sent to {client_addr}. Starting data proxy.")

        # 4. Proxy data
        # This requires adapting the SmuxStream (or WebRTC DC) to asyncio.StreamReader/Writer interface
        # or using their send/receive methods directly in copy_data.

        # For the temporary workaround (direct WebRTC DC):
        # We need to create reader/writer wrappers around the WebRTC data channel.
        # aiortc's RTCDataChannel has `send(str)` and `on("message", func)`

        # Create pipe-like objects for the WebRTC data channel
        web_rtc_reader_pipe, web_rtc_writer_pipe = os.pipe() # os.pipe for local IPC

        # Forward messages from WebRTC DC to the reader end of the pipe
        # And data written to writer end of pipe should be sent via WebRTC DC

        # This is getting complex. A simpler way for the stub:
        # Have two copy tasks.
        # SOCKS_Client -> WebRTC_DC
        # WebRTC_DC -> SOCKS_Client

        # Reusing the WebRTCHandler from the snowflake_conn's underlying_connection
        dc = snowflake_conn.underlying_connection.web_rtc_handler.data_channel
        if not dc: # Should not happen if dial worked
            raise ConnectionError("Data channel is None after successful dial.")

        # Task 1: SOCKS client_reader -> WebRTC DataChannel
        async def socks_to_webrtc():
            try:
                while not client_reader.at_eof():
                    data = await client_reader.read(4096)
                    if not data: break
                    # logger.debug(f"SOCKS->WebRTC: Sending {len(data)} bytes")
                    dc.send(data.decode('utf-8', errors='replace')) # Assuming text for now, or use bytes if DC configured for binary
            except asyncio.CancelledError:
                logger.info("socks_to_webrtc cancelled.")
            except Exception as e:
                logger.error(f"Error in socks_to_webrtc: {e}")
            finally:
                logger.info("socks_to_webrtc finished.")
                # Consider closing the WebRTC DC or SnowflakeConn here if this direction ends.
                # Or let the main handler do it.

        # Task 2: WebRTC DataChannel -> SOCKS client_writer
        # Need a queue to pass messages from DC's on("message") to this task
        webrtc_incoming_queue = asyncio.Queue()

        original_on_message = snowflake_conn.underlying_connection.web_rtc_handler._on_message_callback
        async def new_on_message(message: str): # Or bytes if binary
            # logger.debug(f"WebRTC->SOCKS: Queuing {len(message)} bytes")
            await webrtc_incoming_queue.put(message.encode('utf-8', errors='replace')) # Assuming text from DC
            if original_on_message: # If KCP stub was wired, it would be called too
                 # This part is messy, shows the problem with direct DC use + stubs
                 # await original_on_message(message)
                 pass


        snowflake_conn.underlying_connection.web_rtc_handler._on_message_callback = new_on_message

        async def webrtc_to_socks():
            try:
                while True: # Loop until an exit condition
                    # Wait for a message from the WebRTC DC via the queue
                    # Add a timeout to prevent hanging if DC closes without a final message
                    try:
                        data = await asyncio.wait_for(webrtc_incoming_queue.get(), timeout=SNOWFLAKE_TIMEOUT * 2)
                    except asyncio.TimeoutError:
                        logger.warning("webrtc_to_socks: Timeout waiting for message from WebRTC queue. Assuming DC closed.")
                        break

                    if data is None: # Sentinel for close
                        logger.info("webrtc_to_socks: Received None sentinel, closing.")
                        break

                    client_writer.write(data)
                    await client_writer.drain()
                    webrtc_incoming_queue.task_done()
            except asyncio.CancelledError:
                logger.info("webrtc_to_socks cancelled.")
            except Exception as e:
                logger.error(f"Error in webrtc_to_socks: {e}")
            finally:
                logger.info("webrtc_to_socks finished.")
                # If this direction ends, close the SOCKS writer.
                if not client_writer.is_closing():
                    client_writer.close()


        task1 = asyncio.create_task(socks_to_webrtc(), name="socks_to_webrtc_copy")
        task2 = asyncio.create_task(webrtc_to_socks(), name="webrtc_to_socks_copy")

        # Wait for both copy tasks to complete
        # If one errors, we should cancel the other.
        done, pending = await asyncio.wait([task1, task2], return_when=asyncio.FIRST_COMPLETED)

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True) # Wait for pending to actually cancel

        logger.info(f"SOCKS: Data proxying finished for {client_addr}.")

    except socksio.SOCKSException as e:
        logger.error(f"SOCKS: Protocol error with {client_addr}: {e}", exc_info=True)
    except ConnectionRefusedError as e: # From Snowflake dial failure
        logger.error(f"SOCKS: Connection refused for Snowflake: {e}")
        reply = socksio.SOCKS5Reply(
            reply_type=socksio.SOCKS5ReplyType.CONNECTION_REFUSED, # Or GENERAL_FAILURE
            address=ipaddress.ip_address("0.0.0.0"), port=0)
        if not client_writer.is_closing():
            client_writer.write(reply.send())
            await client_writer.drain()
    except Exception as e:
        logger.error(f"SOCKS: Unhandled error with client {client_addr}: {e}", exc_info=True)
        reply_type = socksio.SOCKS5ReplyType.GENERAL_FAILURE
        # Map other specific errors if needed
        reply = socksio.SOCKS5Reply(reply_type=reply_type, address=ipaddress.ip_address("0.0.0.0"), port=0)
        if not client_writer.is_closing():
            try:
                client_writer.write(reply.send())
                await client_writer.drain()
            except Exception as e_reply:
                 logger.error(f"SOCKS: Error sending error reply: {e_reply}")
    finally:
        logger.info(f"SOCKS: Closing client connection for {client_addr}")
        if not client_writer.is_closing():
            client_writer.close()
        # await client_writer.wait_closed() # Can hang

        if 'snowflake_conn' in locals() and snowflake_conn:
            logger.info(f"SOCKS: Cleaning up Snowflake connection for {client_addr}")
            # Signal the message queue to stop for webrtc_to_socks if it's still running
            if 'webrtc_incoming_queue' in locals():
                await webrtc_incoming_queue.put(None) # Sentinel
            await snowflake_conn.close()


async def run_socks_server(host: str, port: int, app_config: ClientConfig):
    server = await asyncio.start_server(
        lambda r, w: handle_socks_client(r, w, app_config),
        host,
        port
    )
    addr = server.sockets[0].getsockname()
    logger.info(f"SOCKS proxy server listening on {addr}")
    print(f"SOCKS proxy active on {addr[0]}:{addr[1]}") # For PT reporting

    async with server:
        await server.serve_forever()

# --- Example Usage ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Load configuration (e.g., from config.py or PT environment variables)
    # For this example, using a default config.
    # In PT mode, this config would be initialized by PT settings.
    app_conf = ClientConfig(
        broker_url="https://snowflake-broker.torproject.net/", # Public broker
        ice_servers=["stun:stun.l.google.com:19302"],
        max_peers=1
    )
    # To test SQS (requires AWS setup and queue):
    # app_conf.sqs_queue_url = "your_sqs_queue_url"
    # app_conf.sqs_creds_str = "base64_encoded_creds_json_or_rely_on_env"
    # app_conf.broker_url = None # Ensure only one rendezvous is primary

    socks_host = "127.0.0.1"
    socks_port = 1080 # Common SOCKS port, or let OS pick with 0

    logger.info(f"Starting SOCKS server on {socks_host}:{socks_port} with Snowflake client backend.")

    try:
        asyncio.run(run_socks_server(socks_host, socks_port, app_conf))
    except KeyboardInterrupt:
        logger.info("SOCKS server shutting down.")
    except Exception as e:
        logger.error(f"Failed to run SOCKS server: {e}", exc_info=True)

import os # For pipe trick, though it's not fully implemented due to complexity.
# Removing os.pipe as it's not the right tool for async stream adaptation.
# A proper async Queue or custom StreamReader/Writer adapter for the DataChannel is needed.
# The current copy_loop with a queue for WebRTC->SOCKS is a more direct approach.
