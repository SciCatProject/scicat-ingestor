#!/usr/bin/env python3
# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
#
# Script to set up SciCat test environment: authenticate, generate config, create instrument and proposal

import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import h5py
import requests

# Configuration
BACKEND_URL = "http://localhost:3000/api/v3"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FUNCTIONAL_ACCOUNTS_FILE = os.path.join(SCRIPT_DIR, "functionalAccounts.json")
CONFIG_TEMPLATE = os.path.join(SCRIPT_DIR, "ingestor.config.yml.template")
CONFIG_TEST = os.path.join(SCRIPT_DIR, "config.test.yml")
SCHEMA_TEMPLATE = os.path.join(SCRIPT_DIR, "small-coda.imsc.yml.template")
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
SCHEMA_OUTPUT = os.path.join(PROJECT_ROOT, "resources", "small-coda.imsc.yml")
TEST_DATA_DIR = os.path.join(PROJECT_ROOT, "test-data")
HDF5_EXTENSION = ".hdf"


def _read_hdf5_string(h5_obj: h5py.File, path: str) -> str | None:
    try:
        dataset = h5_obj[path]
    except KeyError:
        return None

    value: Any = dataset[()]  # type: ignore[index]
    if isinstance(value, bytes):
        return value.decode("utf-8").strip()
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, list | tuple) and value:
        candidate = value[0]
        if isinstance(candidate, bytes):
            return candidate.decode("utf-8").strip()
        return str(candidate).strip()
    return str(value).strip()


def discover_hdf5_metadata() -> list[dict[str, str]]:
    metadata_entries: list[dict[str, str]] = []
    if not os.path.isdir(TEST_DATA_DIR):
        logging.error("Test data directory not found: %s", TEST_DATA_DIR)
        return metadata_entries

    for filename in sorted(os.listdir(TEST_DATA_DIR)):
        if not filename.lower().endswith(HDF5_EXTENSION):
            continue

        file_path = os.path.join(TEST_DATA_DIR, filename)
        try:
            with h5py.File(file_path, "r") as h5_file:
                proposal_id = _read_hdf5_string(h5_file, "entry/experiment_identifier")
                instrument_name = _read_hdf5_string(h5_file, "entry/instrument/name")

            if not proposal_id or not instrument_name:
                logging.warning(
                    "Skipping %s due to missing metadata (proposal=%s, instrument=%s)",
                    filename,
                    proposal_id,
                    instrument_name,
                )
                continue

            metadata_entries.append(
                {
                    "file_path": file_path,
                    "file_name": filename,
                    "proposal_id": proposal_id,
                    "instrument_name": instrument_name,
                }
            )
        except OSError as err:
            logging.warning("Failed to read %s: %s", filename, err)

    return metadata_entries


def get_admin_credentials() -> tuple[str, str]:
    with open(FUNCTIONAL_ACCOUNTS_FILE) as f:
        accounts = json.load(f)
        for account in accounts:
            if account.get("username") == "admin":
                return account.get("username"), account.get("password")

    logging.error("✗ Admin credentials not found in functional accounts file")
    sys.exit(1)


USERNAME, PASSWORD = get_admin_credentials()


def login_user():
    logging.info("Logging in as %s...", USERNAME)
    url = f"{BACKEND_URL}/auth/login"
    payload = {"username": USERNAME, "password": PASSWORD}
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.ok:
            logging.info("✓ Logged in successfully")
            return response.json().get("id")
        else:
            logging.error(
                "✗ Login failed (HTTP %s): %s", response.status_code, response.text
            )
            return None
    except Exception as e:
        logging.error("✗ Login failed with exception: %s", e)
        return None


def create_instrument(token: str, instrument_name: str) -> bool:
    logging.info("Ensuring instrument %s exists...", instrument_name)
    url = f"{BACKEND_URL}/instruments"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {
        "name": instrument_name,
        "uniqueName": instrument_name,
        "customMetadata": {"source": "integration-tests"},
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.ok:
            data = response.json()
            logging.info("✓ Instrument created: %s", data.get("pid", instrument_name))
            return True

        if response.status_code in {409, 422}:
            logging.info("Instrument %s already exists", instrument_name)
            return True

        logging.error(
            "✗ Failed to create instrument (HTTP %s): %s",
            response.status_code,
            response.text,
        )
        return False
    except Exception as e:
        logging.error("✗ Failed to create instrument with exception: %s", e)
        return False


def get_proposal(token, proposal_id):
    logging.info("Checking for existing proposal with ID %s...", proposal_id)
    url = f"{BACKEND_URL}/proposals/{proposal_id}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.ok:
            logging.info("✓ Proposal found: %s", proposal_id)
            logging.info("Proposal details:")
            logging.info("%s", json.dumps(response.json(), indent=2))
            return True
        else:
            logging.error("✗ Proposal not found (HTTP %s)", response.status_code)
            return False
    except Exception as e:
        logging.error("✗ Failed to get proposal with exception: %s", e)
        return False


def create_proposal(
    token: str,
    *,
    proposal_id: str,
    instrument_name: str,
) -> bool:
    logging.info("Creating proposal %s for instrument %s", proposal_id, instrument_name)
    url = f"{BACKEND_URL}/proposals"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    start_time = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_time = (datetime.now(UTC) + timedelta(days=365)).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )

    payload = {
        "ownerGroup": "ingestor",
        "accessGroups": ["ingestor", "admin"],
        "instrumentGroup": instrument_name,
        "proposalId": proposal_id,
        "pi_email": "pi@example.com",
        "pi_firstname": "Principal",
        "pi_lastname": "Investigator",
        "email": "admin@your.site",
        "firstname": "Test",
        "lastname": "User",
        "title": f"Integration Test Proposal {proposal_id}",
        "abstract": "Proposal generated from integration test HDF5 metadata.",
        "startTime": start_time,
        "endTime": end_time,
        "MeasurementPeriodList": [
            {
                "instrument": instrument_name,
                "start": start_time,
                "end": end_time,
                "comment": "Automated integration test measurement period",
            }
        ],
        "metadata": {"purpose": "integration-test", "facility": instrument_name},
        "type": "Default Proposal",
        "instrumentIds": [instrument_name],
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.ok:
            data = response.json()
            logging.info("✓ Proposal created: %s", data.get("proposalId", proposal_id))
            return True

        if response.status_code in {409, 422}:
            logging.info("Proposal %s already exists", proposal_id)
            return True

        logging.error(
            "✗ Failed to create proposal (HTTP %s): %s",
            response.status_code,
            response.text,
        )
        return False
    except Exception as e:
        logging.error("✗ Failed to create proposal with exception: %s", e)
        return False


def generate_config(token):
    logging.info("Generating config.test.yml...")
    if not os.path.exists(CONFIG_TEMPLATE):
        logging.error("✗ Config template not found: %s", CONFIG_TEMPLATE)
        return False

    try:
        with open(CONFIG_TEMPLATE) as f:
            content = f.read()

        new_content = content.replace("token: <VALID_TOKEN_HERE>", f"token: {token}")

        with open(CONFIG_TEST, "w") as f:
            f.write(new_content)

        logging.info("✓ Config file created: %s", CONFIG_TEST)
        return True
    except Exception as e:
        logging.error("✗ Failed to generate config: %s", e)
        return False


def provision_resources_from_test_data(token: str):
    metadata_entries = discover_hdf5_metadata()
    if not metadata_entries:
        logging.error("No HDF5 files found in %s", TEST_DATA_DIR)
        sys.exit(1)

    created_instruments: set[str] = set()
    for entry in metadata_entries:
        instrument_name = entry["instrument_name"]
        if instrument_name in created_instruments:
            continue
        if create_instrument(token, instrument_name):
            created_instruments.add(instrument_name)

    for entry in metadata_entries:
        proposal_id = entry["proposal_id"]
        if get_proposal(token, proposal_id):
            continue
        create_proposal(
            token,
            proposal_id=proposal_id,
            instrument_name=entry["instrument_name"],
        )


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info("SciCat Ingestor - Test Environment Setup")

    token = login_user()
    if not token:
        logging.error("\n✗ Failed to authenticate")
        sys.exit(1)

    logging.info("Authentication Successful!")
    logging.info("\nYour JWT token:\n%s\n", token)

    if not generate_config(token):
        sys.exit(1)

    provision_resources_from_test_data(token)


if __name__ == "__main__":
    main()
