import abc
import asyncio
import logging
import random
from typing import List, Tuple, Optional, Dict
from urllib.parse import urlparse, urlunparse

from curl_cffi.requests import AsyncSession as CurlAsyncSession, RequestsError # Import RequestsError
# Fallback or alternative client if needed, though curl_cffi is preferred now.
# import httpx

logger = logging.getLogger(__name__)

class RendezvousMethod(abc.ABC):
    """Abstract base class for Snowflake rendezvous mechanisms."""

    @abc.abstractmethod
    async def exchange_offer_answer(self, offer_sdp: str, client_nat_type: str) -> Tuple[str, str]:
        """
        Exchanges SDP offer for an SDP answer with a Snowflake proxy via the broker.

        Args:
            offer_sdp: The client's SDP offer string.
            client_nat_type: A string indicating the client's NAT type (e.g., "restricted", "unrestricted", "unknown").

        Returns:
            A tuple containing (proxy_sdp_answer, proxy_id).
            Raises an exception on failure.
        """
        pass

class HttpRendezvous(RendezvousMethod):
    """
    Implements the HTTP rendezvous method to exchange SDP messages with the Snowflake broker using curl_cffi for browser impersonation.
    Supports direct communication and domain fronting.
    """
    def __init__(self, broker_url: str,
                 front_domains: Optional[List[str]] = None,
                 impersonate_profile: Optional[str] = "chrome120"):
        """
        Initializes HttpRendezvous.

        Args:
            broker_url: The direct URL of the Snowflake broker.
            front_domains: An optional list of front domain URLs for domain fronting.
            impersonate_profile: The browser profile to impersonate (e.g., "chrome120", "firefox110").
                                 Set to None to disable specific impersonation.
        """
        if not broker_url:
            raise ValueError("Broker URL cannot be empty for HttpRendezvous.")
        self.broker_url = broker_url
        self.front_domains = front_domains if front_domains else []
        self.impersonate_profile = impersonate_profile
        if impersonate_profile:
            logger.info(f"HttpRendezvous will use impersonation profile: {impersonate_profile}")
        else:
            logger.info("HttpRendezvous will not use a specific impersonation profile.")


    async def exchange_offer_answer(self, offer_sdp: str, client_nat_type: str) -> Tuple[str, str]:
        """
        Sends the offer to the broker and expects an answer.
        Implements the client side of: https://github.com/torproject/snowflake/blob/main/broker/README.md#http-rendezvous
        """
        target_url = self.broker_url
        headers = {"Content-Type": "application/json"}

        actual_request_url = target_url

        # For curl_cffi, domain fronting is typically handled by setting the Host header
        # and ensuring the URL points to the front domain, while DNS resolves the front domain
        # to an IP that can route to the true backend based on the Host header.
        # curl_cffi itself doesn't have a separate "front" option like some libraries;
        # it relies on standard HTTP mechanisms (Host header, correct DNS for front).
        # If a front domain is used, the `actual_request_url` should be the front domain's URL,
        # and the `Host` header should be the original broker's host.

        original_broker_host = ""
        if self.front_domains:
            from urllib.parse import urlparse, urlunparse # Import here or at top of file
            chosen_front_domain_base = random.choice(self.front_domains)

            original_broker_url_parsed = urlparse(self.broker_url)
            original_broker_host = original_broker_url_parsed.hostname
            if not original_broker_host: # Should not happen for valid http URLs
                raise ValueError(f"Could not parse hostname from broker URL: {self.broker_url}")

            # Construct the new URL using the front domain's base and original path/query
            path_and_query = urlunparse(('', '', original_broker_url_parsed.path, original_broker_url_parsed.params, original_broker_url_parsed.query, original_broker_url_parsed.fragment))
            actual_request_url = f"{chosen_front_domain_base.rstrip('/')}{path_and_query}"

            headers["Host"] = original_broker_host
            logger.info(f"Using domain fronting: Requesting {actual_request_url}, Host header: {headers['Host']}")
        else:
            logger.info(f"Requesting broker directly: {target_url}")
            # In curl_cffi, if not fronting, Host header is derived from the URL automatically.

        # Construct the JSON payload as expected by the Snowflake broker
        # {
        #   "type": "client",
        #   "nat": "restricted" | "unrestricted" | "unknown", # example value
        #   "version": "2.6.0", # example value, /* protocol version */
        #   "offer": "...",     # example value /* SDP */
        #   "delete": "..."     # example value /* an poll-id to be deleted (optional) */
        # }
        payload = {
            "type": "client",
            "nat": client_nat_type,
            "version": "2.8.0-python", # TODO: Use a proper version string
            "offer": offer_sdp,
        }

        # Use CurlAsyncSession for the request
        async with CurlAsyncSession() as session:
            try:
                # curl_cffi uses 'impersonate' parameter at the session or request level.
                # We pass it per request for flexibility, though session-level might be fine too.
                response = await session.post(
                    actual_request_url,
                    headers=headers,
                    json=payload,
                    timeout=30.0,
                    impersonate=self.impersonate_profile if self.impersonate_profile else None
                )
                response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

                # Expected successful response:
                # {
                #   "answer": "...",    /* SDP */
                #   "proxy_id": "...",  /* an opaque ID for the proxy (optional) */
                #   "poll_id": "...",   /* an ID to use for future polls (optional) */
                #   "error": "..."      /* an error message (optional) */
                # }
                data = response.json()

                if "error" in data and data["error"]:
                    logger.error(f"Broker returned an error: {data['error']}")
                    raise Exception(f"Broker error: {data['error']}")

                if "answer" not in data:
                    logger.error(f"Broker response missing 'answer': {data}")
                    raise Exception("Broker response missing 'answer'")

                proxy_sdp_answer = data["answer"]
                proxy_id = data.get("proxy_id", "unknown_proxy") # proxy_id is optional

                logger.info(f"Successfully received answer from broker for proxy: {proxy_id}")
                return proxy_sdp_answer, proxy_id

            except RequestsError as e: # Corrected exception type
                # This includes status errors (like HTTPStatusError) and connection errors
                status_code = e.response.status_code if hasattr(e, 'response') and e.response else "N/A"
                response_text = e.response.text if hasattr(e, 'response') and e.response else "N/A" # .text should be fine on actual Response
                logger.error(f"curl_cffi HTTP error occurred while contacting broker: Status {status_code} - {response_text} (Error: {e})")
                # Distinguish between status errors and other request errors if possible
                if hasattr(e, 'response') and e.response is not None: # Likely an HTTP status error
                     raise Exception(f"Broker HTTP error: {status_code} - {response_text}") from e
                else: # Likely a connection or other network error
                     raise Exception(f"Broker request error (curl_cffi): {type(e).__name__} - {e}") from e
            except Exception as e: # Catch other potential errors (e.g., JSON decoding)
                logger.error(f"An unexpected error occurred during HTTP rendezvous: {e}", exc_info=True)
                raise


# --- SQS Rendezvous ---
import base64
import json

class SqsRendezvous(RendezvousMethod):
    """
    Implements the AWS SQS rendezvous method.
    Client sends offer to an SQS queue and polls the same queue for an answer.
    Note: This method's robustness depends on the broker's SQS handling, especially
    regarding message correlation if multiple clients use the same queue.
    Requires `aioboto3` and appropriate AWS credentials.
    """
    def __init__(self, queue_url: str, creds_str: Optional[str] = None, region_name: Optional[str] = None):
        """
        Initializes SqsRendezvous.

        Args:
            queue_url: The URL of the AWS SQS queue.
            creds_str: Optional base64 encoded JSON string of AWS credentials
                       ({"AccessKeyID": "...", "SecretAccessKey": "..."}).
                       If None, relies on boto3's default credential chain.
            region_name: Optional AWS region name. If None, attempts to parse from queue_url.
        """
        if not queue_url:
            raise ValueError("SQS Queue URL cannot be empty for SqsRendezvous.")
        self.queue_url = queue_url

        # boto3 will try to find credentials in various places if not provided explicitly:
        # 1. Passing credentials to the Session constructor (access_key, secret_key, token)
        # 2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
        # 3. Shared credential file (~/.aws/credentials)
        # 4. AWS config file (~/.aws/config)
        # 5. Assume Role provider
        # 6. Instance metadata service on an EC2 instance

        aws_access_key_id = None
        aws_secret_access_key = None

        if creds_str:
            try:
                # Try parsing as JSON: {"AccessKeyID": "...", "SecretAccessKey": "..."}
                creds_json = json.loads(base64.b64decode(creds_str).decode('utf-8'))
                aws_access_key_id = creds_json.get("AccessKeyID")
                aws_secret_access_key = creds_json.get("SecretAccessKey")
                logger.info("Parsed SQS credentials from base64 encoded JSON.")
            except Exception as e:
                logger.error(f"Failed to parse SQS credentials string: {e}. Will rely on default credential chain.")
                # Potentially treat creds_str as a file path if JSON parsing fails?
                # For now, we assume it's base64 JSON as per Go client's SOCKS option.

        # Determine region from queue_url if not provided.
        # e.g., https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue
        if not region_name:
            try:
                self.region_name = queue_url.split('.')[1]
                if not self.region_name or "amazonaws" not in queue_url: # basic sanity check
                    raise ValueError("Could not determine region from SQS URL and no region_name provided.")
                logger.info(f"Determined SQS region from queue URL: {self.region_name}")
            except Exception as e:
                logger.error(f"Could not parse region from SQS URL '{queue_url}': {e}. Please specify region_name.")
                raise ValueError(f"Invalid SQS URL or region not determinable: {queue_url}") from e
        else:
             self.region_name = region_name

        # It's better to create the client session when needed, or ensure proper async resource management.
        # For now, storing config. The actual client will be created in the async method.
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key


    async def exchange_offer_answer(self, offer_sdp: str, client_nat_type: str) -> Tuple[str, str]:
        """
        Uses SQS for rendezvous. The client sends an offer to one SQS queue (often the broker's input queue)
        and polls another SQS queue for an answer (often a client-specific temporary queue or one indicated by broker).
        However, the Go client's SQS rendezvous seems to use a single queue URL provided by the user,
        implying the broker might handle routing or the client simply polls the same queue it sent to,
        looking for a correlated message.

        The Go broker's SQS implementation (broker/sqs.go) suggests:
        - Client sends a message with `Type: "clientOffer"`, `OfferSDP`, `NATType`.
        - Broker (or some intermediary) picks this up.
        - Broker finds a proxy, sends offer to proxy.
        - Proxy sends answer back to broker.
        - Broker sends a message with `Type: "proxyAnswer"`, `AnswerSDP`, `ProxyID` to the *same queue*.
        - Client needs to poll this queue and filter for messages intended for it (how? Correlation ID?).
          The Go client doesn't seem to implement complex polling/filtering logic in `rendezvous_sqs.go`
          It sends a message and then immediately tries to receive messages.
          This implies a short-lived interaction or a broker that quickly places the answer.

        Let's assume a simplified model for now: send offer, then poll for an answer that might appear.
        This is highly dependent on the specific broker's SQS behavior.
        The Go `client/lib/rendezvous_sqs.go` `Poll` method just does a `ReceiveMessage`.

        A more robust SQS rendezvous would typically involve:
        1. Client creates a temporary SQS queue for its answers.
        2. Client sends its offer to the main broker SQS queue, including the `ReplyToQueueURL` (its temp queue).
        3. Broker processes, gets answer from proxy.
        4. Broker sends the answer to the `ReplyToQueueURL`.
        5. Client polls its temp queue.

        Given the existing Go client's simplicity, it might be expecting the broker to reply on the same queue
        and the client just grabs the next available relevant message. This is fragile.

        For now, mirroring the apparent simplicity: send to queue, then try to receive from the same queue.
        This part will likely need refinement based on actual broker interaction.
        """
        try:
            # Dynamically import boto3 and aioboto3 to make them optional dependencies
            # if SQS is not used.
            import aioboto3 # type: ignore
            from botocore.exceptions import ClientError # type: ignore
        except ImportError:
            logger.error("aioboto3 or botocore is not installed. SQS rendezvous requires 'pip install aioboto3'.")
            raise Exception("SQS libraries not installed.")

        session = aioboto3.Session(
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            region_name=self.region_name
        )

        request_payload = {
            "Type": "clientOffer", # Based on Go broker SQS handler
            "OfferSDP": offer_sdp,
            "NATType": client_nat_type,
            "Version": "2.8.0-python-sqs" # TODO: Proper versioning
            # Client ID or ReplyToQueueURL would be good here for correlation
        }
        message_body = json.dumps(request_payload)

        # Using a single asyncio.TaskGroup for better resource management if available (Python 3.11+)
        # For broader compatibility, manage client lifecycle explicitly.
        sqs_client = None
        try:
            # The `async with` construct ensures the client is properly closed.
            async with session.client("sqs") as sqs_client:
                logger.info(f"Sending client offer to SQS queue: {self.queue_url}")
                await sqs_client.send_message(
                    QueueUrl=self.queue_url,
                    MessageBody=message_body
                )
                logger.info("Offer sent to SQS. Now attempting to receive answer...")

                # Polling for an answer. This is the tricky part without a correlation ID.
                # The Go client polls in a loop with a timeout.
                # Let's try a few times with a short wait.
                # This needs to be robust against receiving unrelated messages.
                attempts = 10  # Poll for up to ~20 seconds
                for i in range(attempts):
                    logger.debug(f"Polling SQS queue (attempt {i+1}/{attempts})")
                    response = await sqs_client.receive_message(
                        QueueUrl=self.queue_url,
                        MaxNumberOfMessages=1,
                        WaitTimeSeconds=2 # Short poll, can be up to 20 for long poll
                    )

                    if "Messages" in response and response["Messages"]:
                        message = response["Messages"][0]
                        receipt_handle = message["ReceiptHandle"]

                        try:
                            message_data = json.loads(message["Body"])
                            logger.debug(f"Received SQS message: {message_data.get('Type', 'Unknown type')}")

                            # TODO: How to ensure this message is for *this* client and *this* offer?
                            # The Go broker's SQS logic (broker/sqs.go) for answers:
                            # out := brokerPollTaskOutput{Type: "proxyAnswer", AnswerSDP: answerSDP, ProxyID: proxyID}
                            # It does not seem to include any correlation ID back to the client's offer.
                            # This implies the client just takes the first "proxyAnswer" it sees. This is problematic
                            # if multiple clients use the same SQS queue.

                            if message_data.get("Type") == "proxyAnswer":
                                answer_sdp = message_data.get("AnswerSDP")
                                proxy_id = message_data.get("ProxyID", "unknown_sqs_proxy")

                                if not answer_sdp:
                                    logger.warning("SQS proxyAnswer message missing AnswerSDP. Discarding.")
                                    # Delete the problematic message
                                    await sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
                                    continue # Try next message or poll

                                logger.info(f"Received proxy answer from SQS for proxy: {proxy_id}")
                                # Delete the successfully processed message
                                await sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
                                return answer_sdp, proxy_id
                            else:
                                logger.warning(f"Received unexpected message type '{message_data.get('Type')}' from SQS. Discarding.")
                                # Delete messages not intended for us or malformed
                                await sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)

                        except json.JSONDecodeError:
                            logger.warning("Failed to decode JSON from SQS message body. Discarding.")
                            await sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
                        except Exception as e:
                            logger.error(f"Error processing SQS message: {e}. Discarding.")
                            await sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
                            # Potentially break or continue based on error type
                    else:
                        logger.debug("No messages received in this poll.")

                    await asyncio.sleep(0.1) # Small delay before next poll if no message

                raise TimeoutError("SQS rendezvous timed out waiting for proxy answer.")

        except ClientError as e:
            logger.error(f"AWS SQS client error: {e}")
            raise Exception(f"SQS client error: {e.response.get('Error', {}).get('Code', 'Unknown')}") from e
        except ImportError: # Catch again in case aioboto3 wasn't found by the earlier check (e.g. in testing)
            logger.error("aioboto3 is not installed. SQS rendezvous requires 'pip install aioboto3'.")
            raise Exception("SQS libraries not installed (aioboto3).")
        except Exception as e:
            logger.error(f"An unexpected error occurred during SQS rendezvous: {e}")
            raise

# --- AMP Cache Rendezvous ---
class AmpCacheRendezvous(RendezvousMethod):
    """
    Implements the AMP Cache rendezvous method using curl_cffi.
    The client crafts a special URL for an AMP Cache service, which then fetches
    the actual offer/answer from the Snowflake broker.
    This class requires the AMP Cache service URL and the target Snowflake broker URL.
    """
    def __init__(self, ampcache_url: str, target_broker_url: str,
                 front_domains: Optional[List[str]] = None,
                 impersonate_profile: Optional[str] = "chrome120"):
        """
        Initializes AmpCacheRendezvous.

        Args:
            ampcache_url: The base URL of the AMP Cache service (e.g., "https://cdn.ampproject.org").
            target_broker_url: The direct URL of the Snowflake broker that the AMP cache will query.
            front_domains: Optional list of front domains for domain fronting the AMP Cache request.
            utls_client_id: Placeholder for uTLS client ID.
            utls_remove_sni: Placeholder for uTLS SNI removal.
        """
        if not ampcache_url:
            raise ValueError("AMP Cache URL cannot be empty for AmpCacheRendezvous.")
        if not target_broker_url:
            raise ValueError("Target Broker URL cannot be empty for AmpCacheRendezvous.")

        # The ampcache_url is the base URL of the AMP Cache service.
        # e.g., "https://amp.cloudflare.com" or "https://www.google.com/amp/"
        # The actual request will be to a URL like:
        # <ampcache_url>/v0/s/<broker_host>/<broker_path_prefix>/<broker_query_args_base64_encoded>
        # This needs to be constructed carefully.
        self.ampcache_url = ampcache_url.rstrip('/')
        self.target_broker_url = target_broker_url # Assign target_broker_url

        # Front domains can also be used with AMP Cache, similar to HTTP rendezvous.
        # The Host header would be the AMP Cache's host, and the request URL targets the front.
        # However, the primary purpose of AMP Cache is often to use the AMP provider's domain as the front.
        self.front_domains = front_domains if front_domains else []
        self.impersonate_profile = impersonate_profile # Store impersonate_profile

        # self.utls_client_id = utls_client_id # These are superseded by impersonate_profile
        # self.utls_remove_sni = utls_remove_sni
        # if self.utls_client_id or self.utls_remove_sni:
        #     logger.warning("uTLS features are superseded by impersonate_profile for AMP Cache.")
        logger.info(f"AmpCacheRendezvous initialized. AMP URL: {self.ampcache_url}, Target Broker: {self.target_broker_url}, Impersonate: {self.impersonate_profile}")


    async def exchange_offer_answer(self, offer_sdp: str, client_nat_type: str) -> Tuple[str, str]:
        """
        Uses AMP Cache for rendezvous. The client crafts a special URL pointing to the AMP cache,
        which in turn makes a request to the actual Snowflake broker.
        Reference:
        - Broker Spec: doc/broker-spec.txt (mentions /amp endpoint)
        - Go client: client/lib/rendezvous_ampcache.go
        - Go broker: broker/amp.go

        The broker's /amp endpoint expects parameters in the path like:
        /amp/<base64url_payload> where payload is JSON:
        {
          "type": "client",
          "nat": "...",
          "version": "...",
          "offer": "..."
        }
        The AMP Cache URL structure is typically <amp-cache-host>/v0/s/<broker-host-and-path-b64>...
        This means the broker URL itself needs to be part of the path requested from the AMP cache.

        Let's assume the user provides the *broker's* direct URL as `config.broker_url`
        and the *AMP cache prefix* as `config.ampcache_url`.

        Example:
        Broker base URL: https://snowflake-broker.torproject.net/
        AMP Cache prefix: https://cdn.ampproject.org (this is a common one)

        The client wants to make the broker handle a POST to its typical endpoint (e.g., /),
        but via AMP cache. The broker's `broker/amp.go` suggests it has an endpoint like `/amp`
        that takes the base64 encoded payload.

        which in turn makes a GET request (with payload in query) to the actual Snowflake broker.
        """
        # urllib.parse.urlparse and urlunparse are imported at the top of the file.
        # base64 and json are also imported at the top of the file (for SQS, but useful here too).
        try:
            parsed_target_broker = urlparse(self.target_broker_url)
            broker_host = parsed_target_broker.hostname
            if not broker_host:
                raise ValueError(f"Could not parse hostname from target_broker_url: {self.target_broker_url}")

            broker_path = parsed_target_broker.path if parsed_target_broker.path else "/"
            # Ensure broker_path starts with a slash
            if not broker_path.startswith("/"): # Should always be true from urlparse if path is not empty
                 broker_path = "/" + broker_path


            # Base64 URL-safe encode the broker's hostname
            broker_host_b64 = base64.urlsafe_b64encode(broker_host.encode('utf-8')).decode('utf-8').rstrip("=")

            payload_dict = {
                "type": "client",
                "nat": client_nat_type,
                "offer": offer_sdp,
                "version": "2.8.0-python-amp"
            }
            payload_json_bytes = json.dumps(payload_dict).encode('utf-8')
            payload_b64 = base64.urlsafe_b64encode(payload_json_bytes).decode('utf-8').rstrip("=")

            ampcache_request_url = f"{self.ampcache_url}/v0/s/{broker_host_b64}{broker_path}?payload={payload_b64}"

            headers = {"Accept": "application/json"}
            actual_final_url_for_amp = ampcache_request_url

            if self.front_domains:
                chosen_front_domain_base = random.choice(self.front_domains)
                parsed_amp_url_for_fronting = urlparse(ampcache_request_url)

                path_and_query_for_front = urlunparse(('', '', parsed_amp_url_for_fronting.path, parsed_amp_url_for_fronting.params, parsed_amp_url_for_fronting.query, parsed_amp_url_for_fronting.fragment))
                actual_final_url_for_amp = f"{chosen_front_domain_base.rstrip('/')}{path_and_query_for_front}"

                amp_service_host = parsed_amp_url_for_fronting.hostname
                if amp_service_host:
                    headers["Host"] = amp_service_host
                    logger.info(f"Using AMP Cache with domain fronting: Requesting {actual_final_url_for_amp}, Host header: {headers['Host']}")
                else:
                    logger.warning(f"Could not parse hostname from AMP Cache URL {ampcache_request_url} for fronting. Proceeding without explicit Host header override for front.")
            else:
                logger.info(f"Using AMP Cache directly: {ampcache_request_url}")

            async with CurlAsyncSession() as session:
                response = await session.get(
                    actual_final_url_for_amp,
                    headers=headers,
                    timeout=30.0,
                    impersonate=self.impersonate_profile if self.impersonate_profile else None
                )
                response.raise_for_status()

                data = response.json()

                if "error" in data and data["error"]:
                    logger.error(f"Broker (via AMP Cache) returned an error: {data['error']}")
                    raise Exception(f"Broker/AMP error: {data['error']}")

                if "answer" not in data:
                    logger.error(f"Broker response (via AMP Cache) missing 'answer': {data}")
                    raise Exception("Broker/AMP response missing 'answer'")

                proxy_sdp_answer = data["answer"]
                proxy_id = data.get("proxy_id", "unknown_amp_proxy")

                logger.info(f"Successfully received answer via AMP Cache for proxy: {proxy_id}")
                return proxy_sdp_answer, proxy_id

        except RequestsError as e:
            status_code = e.response.status_code if hasattr(e, 'response') and e.response else "N/A"
            response_text = e.response.text if hasattr(e, 'response') and e.response else "N/A"
            logger.error(f"curl_cffi HTTP error with AMP Cache: Status {status_code} - {response_text} (Error: {e})")
            if hasattr(e, 'response') and e.response is not None:
                 raise Exception(f"AMP Cache HTTP error: {status_code} - {response_text}") from e
            else:
                 raise Exception(f"AMP Cache request error (curl_cffi): {type(e).__name__} - {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred during AMP Cache rendezvous: {e}", exc_info=True)
            raise


# --- Factory function for creating rendezvous method ---
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .config import ClientConfig # Use . for relative import to avoid circularity at runtime if config imports rendezvous

def create_rendezvous_from_config(config: 'ClientConfig') -> Optional[RendezvousMethod]:
    """
    Factory function to create the appropriate rendezvous client based on configuration.
    """
    # Prioritize AMP Cache if both ampcache_url and broker_url are set
    if config.ampcache_url and config.broker_url:
        logger.info(f"Creating AMP Cache rendezvous: AMP URL={config.ampcache_url}, Target Broker={config.broker_url}")
        return AmpCacheRendezvous(
            ampcache_url=config.ampcache_url,
            target_broker_url=config.broker_url, # target_broker_url is the main one from config
            front_domains=config.front_domains,
            impersonate_profile=config.impersonate_profile
        )
    # Then SQS if sqs_queue_url is set
    elif config.sqs_queue_url:
        logger.info(f"Creating SQS rendezvous: Queue URL={config.sqs_queue_url}")
        try:
            import aioboto3 # type: ignore
        except ImportError:
            logger.error("SQS rendezvous selected, but 'aioboto3' is not installed. Please install it via 'pip install aioboto3'.")
            return None # Or raise ConfigurationError
        return SqsRendezvous(
            queue_url=config.sqs_queue_url,
            creds_str=config.sqs_creds_str
            # region_name could be added to ClientConfig if needed
            # impersonate_profile is not used by SQS
        )
    # Fallback to HTTP if only broker_url is set
    elif config.broker_url:
        logger.info(f"Creating HTTP rendezvous: Broker URL={config.broker_url}")
        return HttpRendezvous(
            broker_url=config.broker_url,
            front_domains=config.front_domains,
            impersonate_profile=config.impersonate_profile
        )
    else:
        logger.error("No rendezvous method could be determined from configuration. "
                     "Requires one of: (ampcache_url and broker_url), sqs_queue_url, or broker_url.")
        return None


# --- Example Usage (for testing this module directly) ---
async def example_http_rendezvous():
    logging.basicConfig(level=logging.INFO)

    # Replace with a real broker URL for testing.
    # This is a public test broker, but availability is not guaranteed.
    # It's better to use your own broker or one from the Tor Project for real tests.
    broker_url = "https://snowflake-broker.torproject.net/"
    # front_domains = ["https://cdn.example.com"] # Optional: for domain fronting

    # Dummy SDP offer for testing
    dummy_offer_sdp = """\
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
a=rtcp-mux
a=sendrecv
"""
    client_nat_type = "unknown" # or "restricted", "unrestricted"

    rendezvous_client = HttpRendezvous(broker_url=broker_url) #, front_domains=front_domains)

    try:
        logger.info(f"Attempting to exchange offer/answer with broker: {broker_url}")
        answer_sdp, proxy_id = await rendezvous_client.exchange_offer_answer(dummy_offer_sdp, client_nat_type)

        print("\n----- Received Answer SDP -----")
        print(answer_sdp)
        print(f"Proxy ID: {proxy_id}")
        print("-----------------------------\n")

    except Exception as e:
        print(f"Rendezvous failed: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(example_http_rendezvous())
    except KeyboardInterrupt:
        pass
