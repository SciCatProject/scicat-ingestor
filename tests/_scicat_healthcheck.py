#!/usr/bin/env python3
"""Standalone integration test that verifies the health endpoint only."""

import argparse
import logging
import signal
import subprocess
import sys
import time
from pathlib import Path

import requests

DEFAULT_CONFIG_FILE = Path("tests/integration/config.test.yml")
HEALTH_CHECK_URL = "http://localhost:8080/health"
HEALTH_CHECK_TIMEOUT = 60  # seconds
HEALTH_CHECK_INTERVAL = 1  # seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify the online ingestor health endpoint"
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_FILE),
        help="Path to ingestor config file (default: %(default)s)",
    )
    return parser.parse_args()


def wait_for_health_endpoint(
    url: str = HEALTH_CHECK_URL,
    timeout_seconds: int = HEALTH_CHECK_TIMEOUT,
    poll_interval: int = HEALTH_CHECK_INTERVAL,
) -> bool:
    """Poll the health endpoint until it returns HTTP 200 or the timeout elapses."""

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                logging.info("Health endpoint reachable at %s", url)
                return True
            logging.warning(
                "Health endpoint unhealthy (status %s): %s",
                response.status_code,
                response.text,
            )
        except requests.RequestException as err:
            logging.warning("Health endpoint not reachable yet: %s", err)

        time.sleep(poll_interval)

    logging.error(
        "Health endpoint %s not ready after %s seconds.", url, timeout_seconds
    )
    return False


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()

    if not config_path.exists():
        logging.error("Config file not found: %s", config_path)
        sys.exit(1)

    logging.info("Starting Online Ingestor for health check only...")
    cmd = [
        sys.executable,
        "-m",
        "scicat_online_ingestor",
        "-c",
        str(config_path),
        "--logging.verbose",
    ]
    process = subprocess.Popen(cmd, shell=False)  # noqa: S603

    try:
        time.sleep(5)
        if not wait_for_health_endpoint():
            logging.error("Health endpoint verification FAILED")
            sys.exit(1)

        logging.info("Health endpoint verification PASSED")

    finally:
        logging.info("Stopping ingestor...")
        process.send_signal(signal.SIGINT)
        process.wait()


if __name__ == "__main__":
    main()
