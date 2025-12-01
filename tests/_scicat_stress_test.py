#!/usr/bin/env python3
"""
Stress test the OnlineIngestor with concurrent health polling and Kafka traffic.
With the current template configuration, this will create a new dataset in SciCat
every time a WRDN message is sent.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import requests
import yaml
from confluent_kafka import Producer
from streaming_data_types import serialise_wrdn

DEFAULT_CONFIG_FILE = Path("tests/integration/config.test.yml")
DEFAULT_DATA_FILE = Path("test-data/small-coda.hdf")
DEFAULT_HEALTH_URL = "http://localhost:8080/health"
DEFAULT_DURATION = 300  # seconds
DEFAULT_HEALTH_INTERVAL = 0.2  # seconds
DEFAULT_MESSAGE_INTERVAL = 1.0  # seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a stress test that hammers the health endpoint while producing "
            "Kafka messages for a configurable duration."
        )
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_FILE),
        help="Path to ingestor config file (default: %(default)s)",
    )
    parser.add_argument(
        "--data-file",
        default=str(DEFAULT_DATA_FILE),
        help=(
            "Path to the Nexus/HDF file referenced in generated WRDN messages "
            "(default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help="Stress duration in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--health-url",
        default=DEFAULT_HEALTH_URL,
        help="Health endpoint URL (default: %(default)s)",
    )
    parser.add_argument(
        "--health-interval",
        type=float,
        default=DEFAULT_HEALTH_INTERVAL,
        help="Interval between health checks in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--message-interval",
        type=float,
        default=DEFAULT_MESSAGE_INTERVAL,
        help="Interval between produced Kafka messages in seconds (default: %(default)s)",
    )
    return parser.parse_args()


def load_config(config_path: Path) -> dict:
    with config_path.open() as handle:
        return yaml.safe_load(handle)


def wait_for_health(url: str, timeout_seconds: int = 60) -> bool:
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
        time.sleep(1)
    return False


def start_ingestor(config_path: Path) -> subprocess.Popen:
    cmd = [
        sys.executable,
        "-m",
        "scicat_online_ingestor",
        "-c",
        str(config_path),
        "--logging.verbose",
    ]
    return subprocess.Popen(cmd, shell=False)  # noqa: S603


def _build_producer(config: dict) -> Producer:
    kafka_config = config["kafka"]
    conf = {"bootstrap.servers": kafka_config["bootstrap_servers"]}
    return Producer(conf)


def _resolve_topic(config: dict) -> str:
    topics = config["kafka"]["topics"]
    return topics[0] if isinstance(topics, list) else topics


def message_sender(
    *,
    config: dict,
    data_file: Path,
    stop_event: threading.Event,
    interval: float,
) -> None:
    producer = _build_producer(config)
    topic = _resolve_topic(config)

    if not data_file.exists():
        logging.error("Data file not found for stress test: %s", data_file)
        stop_event.set()
        return

    while not stop_event.is_set():
        job_id = f"stress-{uuid.uuid4()}"
        message = serialise_wrdn(
            job_id=job_id,
            error_encountered=False,
            file_name=str(data_file.resolve()),
            metadata=json.dumps({"stress": True}),
            message="Stress test message",
            service_id="stress-test",
        )
        try:
            producer.produce(topic, message)
            producer.poll(0)
            logging.info("Produced WRDN message for job_id %s", job_id)
        except Exception as err:
            logging.error("Kafka produce failed: %s", err)
        stop_event.wait(interval)

    producer.flush()


def health_hammer(
    *,
    url: str,
    interval: float,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            response = requests.get(url, timeout=5)
            logging.info("Health status %s", response.status_code)
        except requests.RequestException as err:
            logging.error("Health request failed: %s", err)
        stop_event.wait(interval)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()
    data_file = Path(args.data_file).expanduser().resolve()

    if not config_path.exists():
        logging.error("Config file not found: %s", config_path)
        sys.exit(1)

    config = load_config(config_path)

    logging.info("Starting online ingestor for stress test")
    process = start_ingestor(config_path)

    try:
        if not wait_for_health(args.health_url):
            logging.error("Health endpoint never became ready")
            sys.exit(1)

        stop_event = threading.Event()
        threads = [
            threading.Thread(
                target=health_hammer,
                kwargs={
                    "url": args.health_url,
                    "interval": args.health_interval,
                    "stop_event": stop_event,
                },
                name="health-hammer",
                daemon=True,
            ),
            threading.Thread(
                target=message_sender,
                kwargs={
                    "config": config,
                    "data_file": data_file,
                    "stop_event": stop_event,
                    "interval": args.message_interval,
                },
                name="kafka-producer",
                daemon=True,
            ),
        ]

        for thread in threads:
            thread.start()

        end_time = time.time() + args.duration
        logging.info("Running stress test for %s seconds", args.duration)

        while time.time() < end_time:
            if process.poll() is not None:
                logging.error(
                    "Ingestor process exited early with code %s", process.returncode
                )
                stop_event.set()
                break
            time.sleep(1)

        stop_event.set()
        for thread in threads:
            thread.join(timeout=10)

    finally:
        logging.info("Stopping ingestor...")
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                logging.warning("Ingestor did not exit after SIGINT; sending SIGTERM")
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logging.error(
                        "Ingestor did not exit after SIGTERM; sending SIGKILL"
                    )
                    process.kill()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        logging.error("Ingestor did not exit after SIGKILL; giving up")


if __name__ == "__main__":
    main()
