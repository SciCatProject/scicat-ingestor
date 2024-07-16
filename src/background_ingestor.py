# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
# import scippnexus as snx
import datetime
import json
import pathlib

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
                timeout=10,  # TODO: decide timeout
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

        value_type = variables[variable]["value_type"]
        if value_type == "string":
            value = str(value)
        elif value_type == "string[]":
            value = [str(v) for v in value]
        elif value_type == "integer":
            value = int(value)
        elif value_type == "float":
            value = float(value)
        elif value_type == "date" and isinstance(value, int):
            value = datetime.datetime.fromtimestamp(value, tz=datetime.UTC).isoformat()
        elif value_type == "date" and isinstance(value, str):
            value = datetime.datetime.fromisoformat(value).isoformat()

        values[variable] = value

    return values


def prepare_scicat_dataset(schema, values): ...
def create_scicat_dataset(dataset): ...
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

        # create b2blake hash of all the files

        # create and populate scicat dataset entry
        scicat_dataset = prepare_scicat_dataset(
            metadata_schema['schema'], variables_values
        )

        # create dataset in scicat
        scicat_dataset_pid = create_scicat_dataset(scicat_dataset)

        # create and populate scicat origdatablock entry
        # with files and hashes previously computed

        scicat_origdatablock = create_scicat_origdatablock(
            scicat_dataset_pid, nexus_file_path, done_writing_message_file
        )

        # create origdatablock in scicat
        scicat_origdatablock_id = create_scicat_origdatablock(scicat_origdatablock)

        # return successful code
        return scicat_origdatablock_id
