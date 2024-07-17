# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
# import scippnexus as snx
import datetime
import json
import pathlib
from urllib.parse import urljoin

import h5py
import requests
from scicat_configuration import (
    BackgroundIngestorConfig,
    build_background_ingestor_arg_parser,
    build_scicat_background_ingester_config,
)
from scicat_logging import build_logger
from scicat_metadata import collect_schemas, select_applicable_schema
from system_helpers import exit_at_exceptions


def replace_variables_values(url: str, values: dict) -> str:
    for key, value in values.items():
        url = url.replace("{" + key + "}", str(value))
    return url

def convert_to_type(input_value, value_type: str) :
    output_value = None
    if value_type == "string":
        output_value = str(input_value)
    elif value_type == "string[]":
        output_value = [str(v) for v in input_value]
    elif value_type == "integer":
        output_value = int(input_value)
    elif value_type == "float":
        output_value = float(input_value)
    elif value_type == "date" and isinstance(input_value, int):
        output_value = datetime.datetime.fromtimestamp(input_value, tz=datetime.UTC).isoformat()
    elif value_type == "date" and isinstance(input_value, str):
        output_value = datetime.datetime.fromisoformat(input_value).isoformat()
    else
        raise Exception("Invalid value type")
    return output_value


def extract_variables_values(
    variables: dict, h5file, config: BackgroundIngestorConfig
) -> dict:
    values = {}

    # loop on all the variables defined
    for variable in variables.keys():
        source = variables[variable]["source"]
        value = ""
        if source == "NXS":
            # extract value from nexus file
            # we need to address path entry/user_*/name
            value = h5file[variables[variable]["path"]][...]
        elif source == "SC":
            # build url
            url = replace_variables_values(
                config[""]["scicat_url"] + variables[variable]["url"], values
            )
            # retrieve value from SciCat
            response = requests.get(
                url,
                headers={"token": config[""]["token"]},
                timeout=10,  # TODO: decide timeout. Maybe from configuration?
            )
            # extract value
            value = response.json()[variables[variable]["field"]]
        elif source == "VALUE":
            # the value is the one indicated
            # there might be some substitution needed
            value = replace_variables_values(variables[variable]["value"], values)
            if (
                "operator" in variables[variable].keys()
                and variables[variable]["operator"]
            ):
                operator = variables[variable]["operator"]
                if operator == "join_with_space":
                    value = ", ".join(value)
        else:
            raise Exception("Invalid variable source configuration")

        values[variable] = convert_to_type(value,variables[variable]["value_type"])

    return values


def prepare_scicat_dataset(metadata_schema, values):
    """
    Prepare scicat dataset as dictionary ready to be sent over to scicat as a POST request

    This is an example:
    {
  "pid": "20.500.12269/e3690b21-ee8c-40d6-9409-6b6fdca776d2",
  "datasetName": "this is a dataset",
  "description": "this is the description of the dataset",
  "principalInvestigator": "Massimiliano Novelli",
  "creationLocation": "ESS:CODA",
  "scientificMetadata": {
    "run_number": {
      "value": 18856,
      "unit": "",
      "human_name": "Run Number",
      "type": "integer"
    },
    "sample_temperature": {
      "value": 20.4,
      "unit": "C",
      "human_name": "Sample Temperature",
      "type": "quantity"
    },
    "start_time" : {
      "value" : "2024-07-16T09:30:12.987Z",
      "unit" : "",
      "human_name" : "Start Time",
      "type" : "date"
    }
  },
  "owner": "Massimiliano Novelli",
  "ownerEmail": "max.novelli@ess.eu",
  "sourceFolder": "/ess/data/coda/2024/616254",
  "contactEmail": "max.novelli@ess.eu",
  "creationTime": "2024-07-16T10:00:00.000Z",
  "type": "raw",
  "techniques": [
    {
      "pid": "http://purl.org/pan-science/PaNET/PaNET01155",
      "names": "absorption and phase contrast nanotomography"
    }
  ],
  "instrumentId": "20.500.12269/765b3dc3-f658-410e-b371-04dd1adcd520",
  "sampleId": "bd31725a-dbfd-4c32-87db-1c1ebe61e5ca",
  "proposalId": "616254",
  "ownerGroup": "ess_proposal_616254",
  "accessGroups": [
    "scientific information management systems group"
  ]
}
    """
    schema = metadata_schema["schema"]
    dataset = {}
    scientific_metadata = {
        'ingestor_metadata_schema_id' : {
            "value": metadata_schema["id"],
            "unit": "",
            "human_name": "Ingestor Metadata Schema Id",
            "type": "string"
        }
    }
    for key, field in schema.items():
        machine_name = field["machine_name"]
        field_type = field["type"]
        if field["field_type"] == "high_level":
            dataset[machine_name] = convert_to_type(
                replace_variables_values(field["value"],values),
                field_type
            )
        elif field["field_type"] == "scientific_metadata":
            scientific_metadata[machine_name] = {
                "value" : convert_to_type(
                    replace_variables_values(field["value"],values),
                    field_type
                ),
                "unit" : "",
                "human_name" : field["human_name"] if "human_name" is in field.keys() and field["human_name"] else machine_name,
                "type" : field_type
            }
        else:
            raise Exception("Metadata schema field type invalid")

    dataset["scientific_metadata"] = scientific_metadata

    return dataset


def create_scicat_dataset(dataset,config):
    """
    Execute a POST request to scicat to create a dataset
    """
    response = requests.request(
        method="POST",
        url=urljoin(config["scicat_url"], "datasets"),
        json=dataset,
        headers=config["scicat_headers"],
        timeout=config["timeout_seconds"],
        stream=False,
        verify=True,
    )

    result = response.json()
    if response.ok:

    else:
        err = result.get("error", {})
        raise Exception(f"Error creating new dataset: {err}")

    logger.info(
        "Dataset create successfully. Dataset pid: %s",
        result['pid']
    )
    return result

def prepare_files_list(nexus_file,done_writing_message_file,config): ...
def prepare_scicat_origdatablock(files_list,config): ...
def create_scicat_origdatablock(
    scicat_dataset_pid, nexus_file=None, done_writing_message_file=None
): ...


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_background_ingestor_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_background_ingester_config(arg_namespace)
    ingestion_options = config.ingestion_options
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info(
        'Starting the Scicat background Ingestor with the following configuration:'
    )
    logger.info(config.to_dict())

    # Collect all metadata schema configurations
    schemas = collect_schemas(ingestion_options.schema_directory)

    with exit_at_exceptions(logger, daemon=False):
        logger.info(
            "Nexus file to be ingested : %s",
            (nexus_file_path := pathlib.Path(config.single_run_options.nexus_file)),
        )
        logger.info(
            "Done writing message file linked to nexus file : %s",
            (
                done_writing_message_file := pathlib.Path(
                    config.single_run_options.done_writing_message_file
                )
            ),
        )

        # open and read done writing message input file
        logger.info(json.load(done_writing_message_file.open()))

        # open nexus file with h5py
        with h5py.File(nexus_file_path) as h5file:
            # load instrument metadata configuration
            metadata_schema = select_applicable_schema(nexus_file_path, h5file, schemas)

            # define variables values
            variables_values = extract_variables_values(
                metadata_schema['variables'], h5file, config
            )

        # create files list with b2blake hash of all the files
        files_list = prepare_files_list(nexus_file_path,done_writing_message_file,config)

        # create and populate scicat dataset entry
        scicat_dataset = prepare_scicat_dataset(
            metadata_schema, variables_values
        )

        # create dataset in scicat
        scicat_dataset = create_scicat_dataset(scicat_dataset,config)
        scicat_dataset_pid = scicat_dataset["pid"]

        # create and populate scicat origdatablock entry
        # with files and hashes previously computed
        scicat_origdatablock = create_scicat_origdatablock(
            scicat_dataset_pid, nexus_file_path, done_writing_message_file
        )

        # create origdatablock in scicat
        scicat_origdatablock_id = create_scicat_origdatablock(scicat_origdatablock)

        # return successful code
        return scicat_origdatablock_id
