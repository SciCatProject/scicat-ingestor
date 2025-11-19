#!/usr/bin/env python3
import json
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
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_dataset_pid_from_file(file_path):
    try:
        with h5py.File(file_path, "r") as f:
            # Access the dataset directly
            if "entry/entry_identifier_uuid" in f:
                val = f["entry/entry_identifier_uuid"][()]  # type: ignore
                if isinstance(val, bytes):
                    return val.decode("utf-8")
                return str(val)
            else:
                print("entry/entry_identifier_uuid not found in file")
                sys.exit(1)
    except Exception as e:
        print(f"Error reading HDF5 file: {e}")
        sys.exit(1)


def send_kafka_message(config, file_path, job_id):
    kafka_config = config["kafka"]
    conf = {"bootstrap.servers": kafka_config["bootstrap_servers"]}
    producer = Producer(conf)

    topic = kafka_config["topics"]
    if isinstance(topic, list):
        topic = topic[0]

    print(f"Sending WRDN message to topic {topic} for file {file_path}")

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
            response = requests.get(url, headers=headers)
            if response.ok:
                dataset = response.json()
                print(f"✓ Dataset found: {dataset['pid']}")
                return dataset
            elif response.status_code != 404:
                print(f"Waiting for dataset... Status: {response.status_code}")
        except Exception as e:
            print(f"Error checking SciCat: {e}")

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
        response = requests.get(url, headers=headers)
        if response.ok:
            blocks = response.json()
            if blocks:
                print(f"✓ OrigDatablock found: {blocks[0]['_id']}")
                return blocks[0]
    except Exception as e:
        print(f"Error checking OrigDatablock: {e}")

    return None


def main():
    if not CONFIG_FILE.exists():
        print(f"Config file not found: {CONFIG_FILE}")
        sys.exit(1)

    config = load_config(CONFIG_FILE)
    pid = get_dataset_pid_from_file(DATA_FILE)
    print(f"Target Dataset PID: {pid}")

    # Start Online Ingestor
    print("Starting Online Ingestor...")
    cmd = [
        sys.executable,
        "-m",
        "scicat_online_ingestor",
        "-c",
        str(CONFIG_FILE),
        "--logging.verbose",
    ]
    process = subprocess.Popen(cmd)

    try:
        # Wait for ingestor to start
        time.sleep(5)

        # Send Kafka Message
        # Use a unique job_id for the kafka message
        job_id = f"integration-test-{int(time.time())}"
        send_kafka_message(config, DATA_FILE, job_id)

        # Wait for processing and check SciCat
        print("Waiting for processing...")
        dataset = check_scicat_dataset(config, pid)

        if dataset:
            print("Dataset created successfully.")
            # Check OrigDatablock
            block = check_origdatablock(config, pid)
            if block:
                print("OrigDatablock created successfully.")
                print("Integration test PASSED")
            else:
                print("OrigDatablock NOT found.")
                print("Integration test FAILED")
                sys.exit(1)
        else:
            print("Dataset NOT found after timeout.")
            print("Integration test FAILED")
            sys.exit(1)

    finally:
        print("Stopping ingestor...")
        process.send_signal(signal.SIGINT)
        process.wait()


if __name__ == "__main__":
    main()
