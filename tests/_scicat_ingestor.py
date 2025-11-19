#!/usr/bin/env python3
import json
import logging
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

import h5py
import requests
import yaml
from confluent_kafka import Producer
from streaming_data_types import serialise_wrdn

# Constants
CONFIG_FILE = Path("tests/integration/config.test.yml")
DATA_FILE = Path("test-data/small-coda.hdf").absolute()
TIMEOUT = 60  # seconds


def load_config(config_path):
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_dataset_pid_from_file(file_path):
    try:
        with h5py.File(file_path, "r") as f:
            # Access the dataset directly
            if "entry/entry_identifier_uuid" in f:
                val = f["entry/entry_identifier_uuid"][()]  # type: ignore # noqa: PGH003
                if isinstance(val, bytes):
                    return val.decode("utf-8")
                return str(val)
            else:
                logging.error("entry/entry_identifier_uuid not found in file")
                sys.exit(1)
    except Exception as e:
        logging.error("Error reading HDF5 file: %s", e)
        sys.exit(1)


def send_kafka_message(config, file_path, job_id):
    kafka_config = config["kafka"]
    conf = {"bootstrap.servers": kafka_config["bootstrap_servers"]}
    producer = Producer(conf)

    topic = kafka_config["topics"]
    if isinstance(topic, list):
        topic = topic[0]

    logging.info("Sending WRDN message to topic %s for file %s", topic, file_path)

    message = serialise_wrdn(
        job_id=job_id,
        error_encountered=False,
        file_name=str(file_path),
        metadata="",
        message="Integration test file writing completed",
        service_id="integration-test",
    )

    producer.produce(topic, message)
    producer.flush()


def check_scicat_dataset(config, pid):
    scicat_config = config["scicat"]
    base_url = scicat_config["host"]
    token = scicat_config["token"]
    headers = {"Authorization": f"Bearer {token}"}

    url = f"{base_url}/datasets/{quote_plus(pid)}"

    start_time = time.time()
    while time.time() - start_time < TIMEOUT:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.ok:
                dataset = response.json()
                logging.info("✓ Dataset found: %s", dataset["pid"])
                return dataset
            elif response.status_code != 404:
                logging.info("Waiting for dataset... Status: %s", response.status_code)
        except Exception as e:
            logging.error("Error checking SciCat: %s", e)

        time.sleep(2)

    return None


def check_origdatablock(config, dataset_pid):
    scicat_config = config["scicat"]
    base_url = scicat_config["host"]
    token = scicat_config["token"]
    headers = {"Authorization": f"Bearer {token}"}

    filter_dict = {"where": {"datasetId": dataset_pid}}
    filter_str = json.dumps(filter_dict)
    url = f"{base_url}/origdatablocks?filter={filter_str}"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.ok:
            blocks = response.json()
            if blocks:
                logging.info("✓ OrigDatablock found: %s", blocks[0]["_id"])
                return blocks[0]
    except Exception as e:
        logging.error("Error checking OrigDatablock: %s", e)

    return None


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    if not CONFIG_FILE.exists():
        logging.error("Config file not found: %s", CONFIG_FILE)
        sys.exit(1)

    config = load_config(CONFIG_FILE)
    pid = get_dataset_pid_from_file(DATA_FILE)
    logging.info("Target Dataset PID: %s", pid)

    # Start Online Ingestor
    logging.info("Starting Online Ingestor...")
    cmd = [
        sys.executable,
        "-m",
        "scicat_online_ingestor",
        "-c",
        str(CONFIG_FILE),
        "--logging.verbose",
    ]
    process = subprocess.Popen(cmd)  # noqa: S603

    try:
        # Wait for ingestor to start
        time.sleep(5)

        # Send Kafka Message
        # Use a unique job_id for the kafka message
        job_id = f"integration-test-{int(time.time())}"
        send_kafka_message(config, DATA_FILE, job_id)

        # Wait for processing and check SciCat
        logging.info("Waiting for processing...")
        dataset = check_scicat_dataset(config, pid)

        if dataset:
            logging.info("Dataset created successfully.")
            # Check OrigDatablock
            block = check_origdatablock(config, pid)
            if block:
                logging.info("OrigDatablock created successfully.")
                logging.info("Integration test PASSED")
            else:
                logging.error("OrigDatablock NOT found.")
                logging.error("Integration test FAILED")
                sys.exit(1)
        else:
            logging.error("Dataset NOT found after timeout.")
            logging.error("Integration test FAILED")
            sys.exit(1)

    finally:
        logging.info("Stopping ingestor...")
        process.send_signal(signal.SIGINT)
        process.wait()


if __name__ == "__main__":
    main()
