import unittest
import asyncio
import os
import sys
import subprocess
import re
import time
import logging
import json

# Ensure client_py is in path for testing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from curl_cffi.requests import Session as CurlSession, RequestsError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variable to skip E2E tests
SKIP_E2E_TESTS = os.environ.get("SKIP_E2E_TESTS") == "1" # Standard skip logic
PYTHON_EXECUTABLE = sys.executable

DEFAULT_BROKER_URL = "https://snowflake-broker.torproject.net/"
DEFAULT_STUN_URL = "stun:stun.l.google.com:19302"
DEFAULT_IMPERSONATE_PROFILE = "chrome120"

IP_CHECK_URL = "https://api.ipify.org?format=json"

class SnowflakeClientProcess:
    """Manages launching and terminating the Snowflake client subprocess."""
    def __init__(self, python_exe: str, launcher_script_path: str, args: list): # launcher_script_path not used if -m
        self.python_exe = python_exe
        # self.launcher_script_path = launcher_script_path
        self.args = args
        self.process: Optional[subprocess.Popen] = None
        self.socks_host: Optional[str] = None
        self.socks_port: Optional[int] = None
        self.stdout_lines: list[str] = []
        self.stderr_lines: list[str] = []

    async def start(self) -> tuple[str, int]:
        env = os.environ.copy()
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

        env["PYTHONPATH"] = project_root
        env["TOR_PT_CLIENT_TRANSPORTS"] = "snowflake"
        env["TOR_PT_STATE_LOCATION"] = "."
        env["TOR_PT_EXIT_ON_STDIN_CLOSE"] = "0"

        logger.debug(f"Updated ENV for subprocess: PYTHONPATH={env.get('PYTHONPATH')}, TOR_PT_CLIENT_TRANSPORTS={env.get('TOR_PT_CLIENT_TRANSPORTS')}")

        # Optional Diagnostic (commented out for normal runs)
        # diag_cmd = [self.python_exe, "-c", "import sys; print(f'Diag sys.path: {sys.path}'); import socksio; print('socksio imported successfully for diag')"]
        # logger.info(f"Running diagnostic command: {' '.join(diag_cmd)}")
        # try:
        #     diag_proc = subprocess.run(diag_cmd, capture_output=True, text=True, env=env, timeout=10, check=False)
        #     logger.info(f"Diagnostic STDOUT: {diag_proc.stdout.strip()}")
        #     logger.info(f"Diagnostic STDERR: {diag_proc.stderr.strip()}")
        #     if diag_proc.returncode != 0 or "socksio imported successfully" not in diag_proc.stdout:
        #         logger.warning(f"Diagnostic: Subprocess could not import socksio.")
        # except Exception as e:
        #     logger.warning(f"Diagnostic command failed: {e}")

        cmd = [self.python_exe, "-u", "-m", "client_py.pt_launcher"] + self.args
        logger.info(f"Starting Snowflake client subprocess with command: {' '.join(cmd)}")

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            preexec_fn=os.setsid if sys.platform != "win32" else None,
            env=env
        )

        cmethod_pattern = re.compile(r"CMETHOD snowflake socks5 (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d+)")

        if self.process.stdout is None or self.process.stderr is None:
            if self.process: self.process.kill()
            raise RuntimeError("Subprocess stdout or stderr is None.")

        logger.info("Waiting for CMETHOD line from Snowflake client...")
        start_time = time.time()
        timeout = 60  # Timeout for client to report CMETHOD

        try:
            for line in iter(self.process.stdout.readline, ''):
                self.stdout_lines.append(line.strip())
                logger.debug(f"Client STDOUT: {line.strip()}")
                match = cmethod_pattern.search(line)
                if match:
                    self.socks_host = match.group(1)
                    self.socks_port = int(match.group(2))
                    logger.info(f"Snowflake client SOCKS proxy listening on {self.socks_host}:{self.socks_port}")
                    await asyncio.sleep(3)
                    return self.socks_host, self.socks_port

                if time.time() - start_time > timeout:
                    logger.error("Timeout waiting for CMETHOD line.")
                    raise TimeoutError("Timeout waiting for CMETHOD line.")

                if self.process.poll() is not None:
                    logger.error(f"Client process exited prematurely with code {self.process.returncode}.")
                    break

        finally:
            if self.process and self.process.stderr:
                try:
                    if sys.platform != "win32" and self.process.stderr.fileno() >= 0:
                        fd = self.process.stderr.fileno()
                        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

                    while True:
                        err_line = self.process.stderr.readline()
                        if err_line:
                            self.stderr_lines.append(err_line.strip())
                            logger.debug(f"Client STDERR (on exit/error/finally): {err_line.strip()}")
                        else:
                            break
                except (OSError, ValueError, AttributeError) as e_stderr:
                    logger.debug(f"Could not read full stderr: {e_stderr}")

        self.terminate()

        error_message = "Snowflake client did not report CMETHOD as expected.\n"
        error_message += "Last 20 STDOUT lines:\n" + "\n".join(self.stdout_lines[-20:])
        if self.stderr_lines:
            error_message += "\nLast 20 STDERR lines:\n" + "\n".join(self.stderr_lines[-20:])
        else:
            error_message += "\nNo STDERR output captured or available."

        if time.time() - start_time >= timeout and not (self.socks_host and self.socks_port):
            raise TimeoutError(error_message)
        raise RuntimeError(error_message)

    def terminate(self):
        if self.process:
            logger.info(f"Terminating Snowflake client subprocess (PID: {self.process.pid})...")
            try:
                if sys.platform == "win32":
                    subprocess.call(['taskkill', '/F', '/T', '/PID', str(self.process.pid)])
                else:
                    if self.process.pid is not None:
                        try:
                            pgid = os.getpgid(self.process.pid)
                            os.killpg(pgid, signal.SIGTERM)
                        except ProcessLookupError:
                            logger.info(f"Process group for PID {self.process.pid} not found, possibly already exited.")

                self.process.wait(timeout=10)
            except ProcessLookupError:
                logger.info("Process already terminated.")
            except subprocess.TimeoutExpired:
                logger.warning(f"Snowflake client (PID: {self.process.pid}) did not terminate gracefully, killing.")
                if sys.platform == "win32":
                     subprocess.call(['taskkill', '/F', '/T', '/PID', str(self.process.pid)])
                else:
                    if self.process.pid is not None:
                        try:
                            pgid = os.getpgid(self.process.pid)
                            os.killpg(pgid, signal.SIGKILL)
                        except ProcessLookupError:
                            logger.info(f"Process group for PID {self.process.pid} not found for kill, possibly already exited.")
            except Exception as e:
                logger.error(f"Error during subprocess termination: {e}")
            finally:
                if self.process:
                    if self.process.stdout: self.process.stdout.close()
                    if self.process.stderr: self.process.stderr.close()
                self.process = None
                logger.info("Snowflake client subprocess termination sequence complete.")
        else:
            logger.info("No Snowflake client subprocess to terminate.")


@unittest.skipIf(SKIP_E2E_TESTS, "Skipping E2E tests that launch subprocesses and hit external services.") # Reinstated skip
class TestBasicE2EProxying(unittest.TestCase):

    def test_http_request_via_snowflake(self):
        """
        Basic E2E test: Starts the Snowflake client as a subprocess,
        makes an HTTP GET request via its SOCKS proxy to an IP checking service.
        Verifies that the request is successful and an IP is returned.
        The IP should be that of a Snowflake proxy (ultimately a Tor exit), not the local machine's IP.
        """
        asyncio.run(self._test_http_request_via_snowflake_async())

    async def _test_http_request_via_snowflake_async(self):
        # launcher_script_path is not used when calling with -m

        client_args = [
            '--url', DEFAULT_BROKER_URL,
            '--ice', DEFAULT_STUN_URL,
            '--impersonate-profile', DEFAULT_IMPERSONATE_PROFILE,
            '--unsafe-logging'
        ]

        client_process_manager = SnowflakeClientProcess(PYTHON_EXECUTABLE, "", client_args)

        try:
            socks_host, socks_port = await client_process_manager.start()

            self.assertIsNotNone(socks_host, "SOCKS host should be identified.")
            self.assertIsNotNone(socks_port, "SOCKS port should be identified.")

            proxy_url = f"socks5://{socks_host}:{socks_port}"
            logger.info(f"Attempting GET request to {IP_CHECK_URL} via SOCKS proxy {proxy_url}")

            response_ip = None
            response_status = None

            try:
                with CurlSession() as s:
                    socks5h_proxy_url = f"socks5h://{socks_host}:{socks_port}"
                    logger.info(f"Using socks5h proxy URL for curl_cffi: {socks5h_proxy_url}")

                    r = s.get(IP_CHECK_URL, proxy=socks5h_proxy_url, timeout=90)
                    response_status = r.status_code
                    r.raise_for_status()

                    data = r.json()
                    response_ip = data.get("ip")
                    logger.info(f"Successfully fetched IP via Snowflake proxy: {response_ip} (Status: {response_status})")

            except RequestsError as e:
                err_resp_text = e.response.text if hasattr(e, 'response') and e.response else "N/A"
                logger.error(f"curl_cffi request via proxy failed: {e}. Response text: {err_resp_text}", exc_info=True)
                self.fail(f"HTTP request via SOCKS proxy failed: {e}. Response: {err_resp_text}")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON from IP check response: {e}. Status: {response_status}", exc_info=True)
                self.fail(f"Failed to decode JSON from IP check response. Status: {response_status}, Error: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during HTTP request via proxy: {e}", exc_info=True)
                self.fail(f"Unexpected error during HTTP request: {e}")

            self.assertEqual(response_status, 200, "Request to IP check URL should be successful.")
            self.assertIsNotNone(response_ip, "IP address should be present in the response.")
            self.assertTrue(isinstance(response_ip, str) and len(response_ip) > 0, "Returned IP should be a non-empty string.")

            logger.info(f"E2E Test: Request exited via IP: {response_ip}. If Snowflake worked, this should not be your local public IP.")

        finally:
            client_process_manager.terminate()
            if client_process_manager.stdout_lines or client_process_manager.stderr_lines:
                 logger.info("--- Client Subprocess Logs (Last 20 lines) ---")
                 if client_process_manager.stdout_lines:
                     logger.info("STDOUT:")
                     for line in client_process_manager.stdout_lines[-20:]: logger.info(line)
                 if client_process_manager.stderr_lines:
                     logger.info("STDERR:")
                     for line in client_process_manager.stderr_lines[-20:]: logger.info(line)
                 logger.info("--- End Client Subprocess Logs ---")

if __name__ == "__main__":
    if not SKIP_E2E_TESTS:
        print("Attempting to run basic E2E proxying test...")
        print(f"  Python: {PYTHON_EXECUTABLE}")
        print(f"  Broker: {DEFAULT_BROKER_URL}")
        print(f"  STUN: {DEFAULT_STUN_URL}")
        print(f"  Target: {IP_CHECK_URL}")
        print("Ensure you have network connectivity and necessary permissions.")
    else:
        print(f"Skipping E2E tests (SKIP_E2E_TESTS={os.environ.get('SKIP_E2E_TESTS')}).")

    unittest.main()

import signal
from typing import Optional
import fcntl # For non-blocking stderr read (Unix-only)
# Note: direct use of signal in SnowflakeClientProcess might be problematic if it's imported by main app.
# It's generally fine for a test-specific helper class.
