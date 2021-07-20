#!/usr/bin/env python3

from datetime import datetime
import json
import sys
from user_office import UserOffice

from kafka import KafkaConsumer
from streaming_data_types import deserialise_wrdn

userOffice = UserOffice()
userOffice.login("scicat@ess.eu", "Test1234!")

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
            # print(entry)
            metadata = json.loads(entry.metadata)
            if "proposal" in metadata:
                proposalId = metadata["proposal"]
                proposalId = 169
                proposal = userOffice.get_proposal(proposalId)
                # print(proposal)
                principalInvestigator = (
                    proposal["proposer"]["firstname"].replace("-user", "")
                    + " "
                    + proposal["proposer"]["lastname"]
                )
                email = principalInvestigator.replace(" ", "").lower() + "@ess.eu"
                instrument = proposal["instrument"]["name"]
                sourceFolder = (
                    "/nfs/groups/beamlines/"
                    + instrument
                    + "/"
                    + proposal["shortCode"]
                    + "/"
                    + entry.file_name
                )
                dataset = {
                    "datasetName": "Dataset from FileWriter",
                    "description": "Dataset ingested from FileWriter",
                    "principalInvestigator": principalInvestigator,
                    "creationLocation": instrument,
                    "scientificMetadata": json.loads(entry.metadata),
                    "owner": principalInvestigator,
                    "contactEmail": email,
                    "sourceFolder": entry.file_name,
                    "creationTime": datetime.utcnow().strftime(
                        "%Y-%m-%dT%H:%M:%S.000Z"
                    ),
                    "type": "raw",
                    "ownerGroup": "ess",
                    "accessGroups": ["loki", "odin"],
                    "techniques": [
                        {
                            "pid": "random-alphanumerical-string",
                            "name": "Absorption something or the other",
                        }
                    ],
                    "instrumentId": proposal["instrument"]["id"],
                    "proposalId": proposal["shortCode"],
                }
                print(dataset)
                # name = entry.source_name
                # value = entry.value
                # print(f"{name}: {value}")
except KeyboardInterrupt:
    sys.exit()
