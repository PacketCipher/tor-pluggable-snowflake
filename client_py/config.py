import argparse
from dataclasses import dataclass, field
from typing import List, Optional

DEFAULT_SNOWFLAKE_CAPACITY = 1

@dataclass
class ClientConfig:
    ice_servers: List[str] = field(default_factory=list)
    broker_url: Optional[str] = None
    front_domains: List[str] = field(default_factory=list)
    ampcache_url: Optional[str] = None
    sqs_queue_url: Optional[str] = None
    sqs_creds_str: Optional[str] = None
    log_file: Optional[str] = None
    log_to_state_dir: bool = False
    keep_local_addresses: bool = False
    unsafe_logging: bool = False
    max_peers: int = DEFAULT_SNOWFLAKE_CAPACITY
    display_version: bool = False
    impersonate_profile: Optional[str] = "chrome120" # Default impersonation profile
    # For PT integration
    pt_proxy_url: Optional[str] = None # From TOR_PT_PROXY env var or SOCKS args
    # SMUX config
    smux_max_stream_buffer: int = 1048576 # 1MB, matching Go client's StreamSize
    smux_keep_alive_interval: int = 2 * 60 + 30 # 2.5 minutes (150s), derived from Go's 10 min timeout / 4
    smux_max_frame_size: int = 32768 # Matches xtaci/smux default MaxFrameSize

def parse_args() -> ClientConfig:
    parser = argparse.ArgumentParser(description="Snowflake Client in Python", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--ice",
        type=str,
        default="",
        help="Comma-separated list of ICE servers (e.g., stun:stun.example.com:3478)",
    )
    parser.add_argument(
        "--url", type=str, help="URL of the signaling broker"
    )
    parser.add_argument(
        "--front",
        type=str,
        help="Front domain (legacy, use --fronts instead)",
    )
    parser.add_argument(
        "--fronts",
        type=str,
        default="",
        help="Comma-separated list of front domains",
    )
    parser.add_argument(
        "--ampcache", type=str, help="URL of AMP cache for signaling"
    )
    parser.add_argument(
        "--sqsqueue", type=str, help="URL of SQS Queue for signaling"
    )
    parser.add_argument(
        "--sqscreds", type=str, help="Credentials for SQS Queue (base64 encoded JSON or path to file)"
    )
    parser.add_argument("--log", type=str, help="Name of the log file")
    parser.add_argument(
        "--log-to-state-dir",
        action="store_true",
        help="Resolve the log file relative to Tor's PT state directory",
    )
    parser.add_argument(
        "--keep-local-addresses",
        action="store_true",
        help="Keep local LAN address ICE candidates",
    )
    parser.add_argument(
        "--unsafe-logging",
        action="store_true",
        help="Disable log scrubbing (log IP addresses and other sensitive info)",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=DEFAULT_SNOWFLAKE_CAPACITY,
        help="Maximum number of WebRTC peers to connect to",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Display version information and quit",
    )
    parser.add_argument(
        "--impersonate-profile",
        type=str,
        default="chrome120", # Default defined in ClientConfig and here for help text
        help="Browser profile to impersonate for HTTP/S rendezvous (e.g., chrome120, firefox110, safari170, none). 'none' disables specific impersonation.",
    )

    args = parser.parse_args()

    ice_servers_list = [s.strip() for s in args.ice.split(",") if s.strip()]

    front_domains_list = []
    if args.fronts:
        front_domains_list = [f.strip() for f in args.fronts.split(",") if f.strip()]
    elif args.front: # Legacy support
        front_domains_list = [args.front.strip()]


    return ClientConfig(
        ice_servers=ice_servers_list,
        broker_url=args.url,
        front_domains=front_domains_list,
        ampcache_url=args.ampcache,
        sqs_queue_url=args.sqsqueue,
        sqs_creds_str=args.sqscreds,
        log_file=args.log,
        log_to_state_dir=args.log_to_state_dir,
        keep_local_addresses=args.keep_local_addresses,
        unsafe_logging=args.unsafe_logging,
        max_peers=args.max,
        display_version=args.version,
        impersonate_profile=None if args.impersonate_profile.lower() == 'none' else args.impersonate_profile,
    )

if __name__ == "__main__":
    # Example usage:
    config = parse_args()
    print("Parsed Configuration:")
    import json
    print(json.dumps(config.__dict__, indent=2))

    if config.display_version:
        # In a real app, you'd import version info
        print("Snowflake Client Python Version: X.Y.Z")
        exit(0)

    # Further client logic would use this config object
    if config.log_file:
        print(f"Logging to: {config.log_file}")
        if config.log_to_state_dir:
            print(" (Relative to PT state directory)")

    if config.ice_servers:
        print(f"ICE Servers: {config.ice_servers}")
    # ... and so on for other configurations
    print(f"Max peers: {config.max_peers}")
