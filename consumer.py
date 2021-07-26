#!/usr/bin/env python3

from datetime import datetime
import json
import sys

from user_office import UserOffice
from scicat import SciCat

from kafka import KafkaConsumer
import keyring
from streaming_data_types import deserialise_wrdn


def main():
    consumer = KafkaConsumer(
        "UTGARD_writerCommand",
        group_id="group1",
        bootstrap_servers=["dmsc-kafka01.cslab.esss.lu.se:9092"],
        auto_offset_reset="earliest",
    )
    try:
        for message in consumer:
            data_type = message.value[4:8]
            if data_type == b"wrdn":
                entry = deserialise_wrdn(message.value)
                if entry.error_encountered:
                    continue
                print(entry)
                if entry.metadata is not None:
                    metadata = json.loads(entry.metadata)
                    if "proposal_id" in metadata:
                        proposal_id = int(metadata["proposal_id"])
                        # proposalId = 169
                        userOffice = UserOffice()
                        uo_username = "scicat@ess.eu"
                        uo_password = keyring.get_password("useroffice", uo_username)
                        userOffice.login(uo_username, uo_password)
                        proposal = userOffice.get_proposal(proposal_id)
                        print(proposal)

                        scicat = SciCat("http://localhost:3000")
                        sc_username = "ingestor"
                        sc_password = keyring.get_password("scicat", sc_username)
                        scicat.login(sc_username, sc_password)
                        instrument = scicat.get_instrument_by_name(
                            proposal["instrument"]["name"]
                        )
                        print(instrument)

                        dataset = create_dataset(metadata, proposal, instrument)
                        print(dataset)
                        created_dataset = scicat.post_dataset(dataset)
                        print(created_dataset)
                        orig_datablock = create_origdatablock(
                            created_dataset["pid"], entry
                        )
                        print(orig_datablock)
                        created_origdatablock = scicat.post_dataset_origdatablock(
                            created_dataset["pid"], orig_datablock
                        )
                        print(created_origdatablock)
    except KeyboardInterrupt:
        sys.exit()


def create_dataset(metadata: dict, proposal: dict, instrument: dict) -> dict:
    principalInvestigator = (
        proposal["proposer"]["firstname"].replace("-user", "")
        + " "
        + proposal["proposer"]["lastname"]
    )
    email = principalInvestigator.replace(" ", "").lower() + "@ess.eu"
    sourceFolder = (
        "/nfs/groups/beamlines/" + instrument["name"] + "/" + proposal["proposalId"]
    )
    dataset = {
        "datasetName": "Dataset from FileWriter",
        "description": "Dataset ingested from FileWriter",
        "principalInvestigator": principalInvestigator,
        "creationLocation": instrument["name"],
        "scientificMetadata": metadata,
        "owner": principalInvestigator,
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
