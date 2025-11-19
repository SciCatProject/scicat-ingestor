#!/usr/bin/env python3
# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
#
# Script to set up SciCat test environment: authenticate, generate config, create instrument and proposal

import os
import sys
import json
import requests
from datetime import datetime, timedelta

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
    with open(FUNCTIONAL_ACCOUNTS_FILE, "r") as f:
        accounts = json.load(f)
        for account in accounts:
            if account.get("username") == "admin":
                return account.get("username"), account.get("password")

    print("✗ Admin credentials not found in functional accounts file")
    sys.exit(1)


USERNAME, PASSWORD = get_admin_credentials()


def login_user():
    print(f"Logging in as {USERNAME}...")
    url = f"{BACKEND_URL}/auth/login"
    payload = {"username": USERNAME, "password": PASSWORD}
    try:
        response = requests.post(url, json=payload)
        if response.ok:
            print("✓ Logged in successfully")
            return response.json().get("id")
        else:
            print(f"✗ Login failed (HTTP {response.status_code}): {response.text}")
            return None
    except Exception as e:
        print(f"✗ Login failed with exception: {e}")
        return None


def create_test_instrument(token):
    print("Creating CODA instrument...")
    url = f"{BACKEND_URL}/instruments"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {
        "name": "coda",
        "customMetadata": {"facility": "ESS", "type": "development"},
        "uniqueName": "coda",
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.ok:
            data = response.json()
            pid = data.get("pid")
            print(f"✓ Instrument created: {pid}")
            print("Instrument details:")
            print(json.dumps(data, indent=2))
            return True
        else:
            print(
                f"✗ Failed to create instrument (HTTP {response.status_code}): {response.text}"
            )
            return False
    except Exception as e:
        print(f"✗ Failed to create instrument with exception: {e}")
        return False


def get_proposal(token, proposal_id):
    print(f"Checking for existing proposal with ID {proposal_id}...")
    url = f"{BACKEND_URL}/proposals/{proposal_id}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers)
        if response.ok:
            print(f"✓ Proposal found: {proposal_id}")
            print("Proposal details:")
            print(json.dumps(response.json(), indent=2))
            return True
        else:
            print(f"✗ Proposal not found (HTTP {response.status_code})")
            return False
    except Exception as e:
        print(f"✗ Failed to get proposal with exception: {e}")
        return False


def create_test_proposal(token):
    print("Creating test proposal...")
    url = f"{BACKEND_URL}/proposals"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    # Use UTC time
    start_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")
    # 1 year later
    end_time = (datetime.utcnow() + timedelta(days=365)).strftime(
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
        response = requests.post(url, json=payload, headers=headers)
        if response.ok:
            data = response.json()
            proposal_id = data.get("proposalId")
            print(f"✓ Proposal created: {proposal_id}")
            return True
        else:
            print(
                f"✗ Failed to create proposal (HTTP {response.status_code}): {response.text}"
            )
            return False
    except Exception as e:
        print(f"✗ Failed to create proposal with exception: {e}")
        return False


def generate_config(token):
    print("Generating config.test.yml...")
    if not os.path.exists(CONFIG_TEMPLATE):
        print(f"✗ Config template not found: {CONFIG_TEMPLATE}")
        return False

    try:
        with open(CONFIG_TEMPLATE, "r") as f:
            content = f.read()

        new_content = content.replace("token: <VALID_TOKEN_HERE>", f"token: {token}")

        with open(CONFIG_TEST, "w") as f:
            f.write(new_content)

        print(f"✓ Config file created: {CONFIG_TEST}")
        return True
    except Exception as e:
        print(f"✗ Failed to generate config: {e}")
        return False


def generate_schema():
    print("Generating small-coda.imsc.yml...")
    if not os.path.exists(SCHEMA_TEMPLATE):
        print(f"✗ Schema template not found: {SCHEMA_TEMPLATE}")
        return False

    try:
        with open(SCHEMA_TEMPLATE, "r") as f:
            content = f.read()

        # Replace <CURRENT_ABSOLUTE_PATH> with the project root path
        new_content = content.replace("<CURRENT_ABSOLUTE_PATH>", PROJECT_ROOT)

        with open(SCHEMA_OUTPUT, "w") as f:
            f.write(new_content)

        print(f"✓ Schema file created: {SCHEMA_OUTPUT}")
        return True
    except Exception as e:
        print(f"✗ Failed to generate schema: {e}")
        return False


def main():
    print("=" * 50)
    print("SciCat Ingestor - Test Environment Setup")
    print("=" * 50)
    print()

    token = login_user()
    if not token:
        print("\n✗ Failed to authenticate")
        sys.exit(1)

    print("\n" + "=" * 50)
    print("Authentication Successful!")
    print("=" * 50)
    print(f"\nYour JWT token:\n{token}\n")

    if not generate_config(token):
        sys.exit(1)

    if not generate_schema():
        sys.exit(1)

    print()
    create_test_instrument(token)

    print()
    if create_test_proposal(token):
        print("\nTest proposal created successfully!")
        print("  Proposal ID: 443503\n")

    get_proposal(token, "443503")


if __name__ == "__main__":
    main()
