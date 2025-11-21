#!/usr/bin/env python3
import argparse
import json
import logging
import signal
import subprocess
import sys
import time
from pathlib import Path

import h5py
import requests
import yaml
from confluent_kafka import Producer
from streaming_data_types import serialise_wrdn

# Constants
DEFAULT_CONFIG_FILE = Path("tests/integration/config.test.yml")
DEFAULT_DATA_DIR = Path("test-data")
TIMEOUT = 60  # seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run integration tests against SciCat ingestor"
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_FILE),
        help="Path to ingestor config file (default: %(default)s)",
    )
    parser.add_argument(
        "--data-dir",
        default=str(DEFAULT_DATA_DIR),
        help="Directory containing HDF files to ingest (default: %(default)s)",
    )
    return parser.parse_args()


def discover_hdf_files(data_dir: Path) -> list[Path]:
    if not data_dir.exists() or not data_dir.is_dir():
        logging.error("Data directory not found: %s", data_dir)
        return []

    data_files = sorted(
        file_path for file_path in data_dir.rglob("*.hdf") if file_path.is_file()
    )
    if not data_files:
        logging.error("No .hdf files found in %s", data_dir)
    return data_files


def load_config(config_path):
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_job_id_from_file(file_path):
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


def _fetch_dataset_by_job_id(
    base_url: str, headers: dict[str, str], job_id: str
) -> dict | None:
    limits_payload = {"limit": 1, "skip": 0, "order": "creationTime:desc"}
    fields_payload = {
        "mode": {},
        "scientific": [{"lhs": "job_id", "relation": "EQUAL_TO_STRING", "rhs": job_id}],
    }
    params = {
        "limits": json.dumps(limits_payload),
        "fields": json.dumps(fields_payload),
    }
    url = f"{base_url}/datasets/fullquery"
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.ok:
            datasets = response.json()
            if datasets:
                return datasets[0]
    except Exception as e:
        logging.error("Error checking SciCat by job_id: %s", e)
    return None


def check_scicat_dataset(config, *, job_id: str):
    scicat_config = config["scicat"]
    base_url = scicat_config["host"]
    token = scicat_config["token"]
    headers = {"Authorization": f"Bearer {token}"}

    start_time = time.time()
    while time.time() - start_time < TIMEOUT:
        dataset = _fetch_dataset_by_job_id(base_url, headers, job_id)
        if dataset:
            logging.info("✓ Dataset found by job_id %s: %s", job_id, dataset["pid"])
            return dataset

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
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()
    data_dir = Path(args.data_dir).expanduser().resolve()

    if not config_path.exists():
        logging.error("Config file not found: %s", config_path)
        sys.exit(1)

    data_files = discover_hdf_files(data_dir)
    if not data_files:
        sys.exit(1)

    config = load_config(config_path)
    total_files = len(data_files)
    logging.info("Found %d HDF file(s) to ingest", total_files)

    # Start Online Ingestor
    logging.info("Starting Online Ingestor...")
    cmd = [
        sys.executable,
        "-m",
        "scicat_online_ingestor",
        "-c",
        str(config_path),
        "--logging.verbose",
    ]
    process = subprocess.Popen(cmd)  # noqa: S603

    try:
        # Wait for ingestor to start
        time.sleep(5)

        for index, data_file in enumerate(data_files, start=1):
            file_path = data_file.resolve()
            file_job_id = get_job_id_from_file(file_path)
            logging.info(
                "Processing file %d/%d: %s (job_id: %s)",
                index,
                total_files,
                file_path,
                file_job_id,
            )

            message_job_id = f"integration-test-{data_file.stem}-{int(time.time())}"
            send_kafka_message(config, file_path, message_job_id)

            logging.info("Waiting for dataset related to job_id %s...", file_job_id)
            dataset = check_scicat_dataset(config, job_id=file_job_id)

            if not dataset:
                logging.error(
                    "Dataset for job_id %s NOT found after timeout.", file_job_id
                )
                logging.error("Integration test FAILED")
                sys.exit(1)

            dataset_pid = dataset["pid"]
            block = check_origdatablock(config, dataset_pid)
            if not block:
                logging.error("OrigDatablock for dataset %s NOT found.", dataset_pid)
                logging.error("Integration test FAILED")
                sys.exit(1)

            logging.info("Dataset %s ingested successfully.", dataset_pid)

        logging.info(
            "All %d datasets ingested successfully. Integration test PASSED",
            total_files,
        )

    finally:
        logging.info("Stopping ingestor...")
        process.send_signal(signal.SIGINT)
        process.wait()


if __name__ == "__main__":
    main()
