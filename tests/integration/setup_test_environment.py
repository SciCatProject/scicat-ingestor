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


def create_test_instrument(token):
    logging.info("Creating CODA instrument...")
    url = f"{BACKEND_URL}/instruments"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {
        "name": "coda",
        "customMetadata": {"facility": "ESS", "type": "development"},
        "uniqueName": "coda",
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.ok:
            data = response.json()
            pid = data.get("pid")
            logging.info("✓ Instrument created: %s", pid)
            logging.info("Instrument details:")
            logging.info("%s", json.dumps(data, indent=2))
            return True
        else:
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


def create_test_proposal(token):
    logging.info("Creating test proposal...")
    url = f"{BACKEND_URL}/proposals"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    # Use UTC time
    start_time = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    # 1 year later
    end_time = (datetime.now(UTC) + timedelta(days=365)).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )

    payload = {
        "ownerGroup": "ingestor",
        "accessGroups": ["ingestor", "admin"],
        "instrumentGroup": "ingestor",
        "proposalId": "443503",
        "pi_email": "pi@example.com",
        "pi_firstname": "Principal",
        "pi_lastname": "Investigator",
        "email": "admin@your.site",
        "firstname": "Test",
        "lastname": "User",
        "title": "Test Proposal for Ingestor Development",
        "abstract": "This is a test proposal created for development and testing of the SciCat ingestor.",
        "startTime": start_time,
        "endTime": end_time,
        "MeasurementPeriodList": [
            {
                "instrument": "test-instrument",
                "start": start_time,
                "end": end_time,
                "comment": "Test measurement period",
            }
        ],
        "metadata": {"purpose": "development", "facility": "test-facility"},
        "type": "Default Proposal",
        "instrumentIds": ["test-instrument"],
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.ok:
            data = response.json()
            proposal_id = data.get("proposalId")
            logging.info("✓ Proposal created: %s", proposal_id)
            return True
        else:
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


def generate_schema():
    logging.info("Generating small-coda.imsc.yml...")
    if not os.path.exists(SCHEMA_TEMPLATE):
        logging.error("✗ Schema template not found: %s", SCHEMA_TEMPLATE)
        return False

    try:
        with open(SCHEMA_TEMPLATE) as f:
            content = f.read()

        # Replace <CURRENT_ABSOLUTE_PATH> with the project root path
        new_content = content.replace("<CURRENT_ABSOLUTE_PATH>", PROJECT_ROOT)

        with open(SCHEMA_OUTPUT, "w") as f:
            f.write(new_content)

        logging.info("✓ Schema file created: %s", SCHEMA_OUTPUT)
        return True
    except Exception as e:
        logging.error("✗ Failed to generate schema: %s", e)
        return False


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

    if not generate_schema():
        sys.exit(1)

    create_test_instrument(token)

    if create_test_proposal(token):
        logging.info("\nTest proposal created successfully!")
        logging.info("  Proposal ID: 443503\n")

    get_proposal(token, "443503")


if __name__ == "__main__":
    main()
