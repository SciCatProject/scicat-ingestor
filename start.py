#!/usr/bin/env python3

from datetime import datetime
import json
import sys

from user_office import UserOffice
from scicat import SciCat

from kafka import KafkaConsumer
from streaming_data_types import deserialise_wrdn


def main():
    # get configuration
    config = get_config()
    # instantiate kafka consumer
    kafka_config = config["kafka"]
    consumer = KafkaConsumer(
        kafka_config["topic"],
        group_id=kafka_config["group_id"],
        bootstrap_servers=kafka_config["bootstrap_servers"],
        auto_offset_reset=kafka_config["auto_offset_reset"],
    )

    # instantiate connector to user office
    # retrieve relevant configuration
    user_office_config = config["user_office"]
    user_office = UserOffice(user_office_config["host"])
    user_office.login(user_office_config["username"],user_office_config["password"])

    # instantiate connector to scicat
    # retrieve relevant configuration
    scicat_config = config["scicat"]
    scicat = SciCat(scicat_config["host"])
    scicat.login(scicat_config["username"],scicat_config["password"])


    # main loop, waiting for messages
    try:
        for message in consumer:
            data_type = message.value[4:8]
            if data_type == b"wrdn":
                print("----------------")
                print("Write Done")
                entry = deserialise_wrdn(message.value)
                if entry.error_encountered:
                    continue
                print(entry)
                if entry.metadata is not None:
                    metadata = json.loads(entry.metadata)
                    print(metadata)
                    if "proposal_id" in metadata:
                        proposal_id = int(metadata["proposal_id"])
                        # proposalId = 169
                        uo_proposal = user_office.get_proposal(proposal_id)
                        print(uo_proposal)

                        instrument = scicat.get_instrument_by_name(
                            uo_proposal["instrument"]["name"]
                        )
                        print(instrument)
                        proposal = scicat.get_proposal(uo_proposal["proposalId"])
                        print(proposal)

                        dataset = create_dataset(metadata, proposal, instrument)
                        print(dataset)
                        #created_dataset = scicat.post_dataset(dataset)
                        #print(created_dataset)
                        orig_datablock = create_origdatablock(
                            "test_dataset", entry
                        #    created_dataset["pid"], entry
                        )
                        print(orig_datablock)
                        #created_origdatablock = scicat.post_dataset_origdatablock(
                        #    created_dataset["pid"], orig_datablock
                        #)
                        #print(created_origdatablock)
    except KeyboardInterrupt:
        sys.exit()


def get_config() -> dict:
    with open("config.json", "r") as config_file:
        data = config_file.read()
        return json.loads(data)


def create_dataset(metadata: dict, proposal: dict, instrument: dict) -> dict:
    principal_investigator = proposal["pi_firstname"] + " " + proposal["pi_lastname"]
    email = proposal["pi_email"]
    sourceFolder = (
        "/nfs/groups/beamlines/" + instrument["name"] + "/" + proposal["proposalId"]
    )
    dataset = {
        "datasetName": "Dataset from FileWriter",
        "description": "Dataset ingested from FileWriter",
        "principalInvestigator": email,
        "creationLocation": instrument["name"],
        "scientificMetadata": metadata,
        "owner": principal_investigator,
        "ownerEmail": email,
        "contactEmail": email,
        "sourceFolder": sourceFolder,
        "creationTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "type": "raw",
        "ownerGroup": "ess",
        "accessGroups": ["loki", "odin"],
        "techniques": [
            {
                "pid": "random-alphanumerical-string",
                "name": "Absorption something or the other",
            }
        ],
        "instrumentId": instrument["pid"],
        "proposalId": proposal["proposalId"],
    }
    return dataset


def create_origdatablock(dataset_pid: str, entry: dict) -> dict:
    metadata = json.loads(entry.metadata)
    orig_datablock = {
        "size": metadata["file_size"],
        "ownerGroup": "ess",
        "accessGroups": ["loki", "odin"],
        "datasetId": dataset_pid,
        "dataFileList": [
            {
                "path": entry.file_name,
                "size": metadata["file_size"],
                "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            }
        ],
    }

    return orig_datablock


if __name__ == "__main__":
    main()
