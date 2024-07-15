# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
# import scippnexus as snx
import datetime
import json
import pathlib
import h5py
import os

from scicat_configuration import (
    build_background_ingestor_arg_parser,
    build_scicat_background_ingester_config, BackgroundIngestorConfig,
)
from scicat_logging import build_logger
from system_helpers import exit_at_exceptions



def list_schema_files(schemas_folder):
    """
    return the list of the metadata schema configuration available in the folder provided
    valid metadata schema configuration ends with imsc.json
    imsc = ingestor metadata schema configuration
    """
    return [file for file in os.listdir(schemas_folder) if file.endswith("imsc.json") and not file.startswith(".")]


def select_applicable_schema(nexus_file, nxs, schemas):
    """
    This function evaluate which metadata schema configuration is applicable to this file.
    Order of the schemas matters and first schema that is suitable is selected.
    """
    for schema in schemas.values():
        if isinstance(schema['selector'], str):
            selector_list = schema['selector'].split(':')
            selector = {
                "operand_1": selector_list[0],
                "operation": selector_list[1],
                "operand_2": selector_list[2],
            }
        elif isinstance(schema['selector'], dict):
            selector = schema['selector']
        else:
            raise Exception("Invalid type for schema selector")

        if selector['operand_1'] in [
            "filename",
            "data_file",
            "nexus_file",
            "data_file_name",
        ]:
            selector['operand_1_value'] = nexus_file

        if selector['operation'] == "starts_with":
            if selector['operand_1_value'].startswith(selector['operand_2']):
                return schema

    raise Exception("No applicable metadata schema configuration found!!")

def extract_variables_values(
        variables: dict,
        h5file,
        config: BackgroundIngestorConfig
) -> dict:
    values = {}

    # loop on all the variables defined
    for variable in variables.keys():
        source = variables[variable]["source"].split(":")
        value = ""
        if source[0] == "NXS":
            # extract value from nexus file
            # we need to address path entry/user_*/name
            value = h5file[source[1]][...]
        elif source[0] == "SC":
            # build url
            url = replace_variables_values(
                config[""]["scicat_url"] + source[1],
                values
            )
            # retrieve value from SciCat
            response = requests.get(
                url,
                headers = {
                    "token": config[""]["token"]
                }
            )
            # extract value
            value = response.json()[source[2]]
        elif source[0] == "VALUE":
            # the value is the one indicated
            # there might be some substitution needed
            value = replace_variables_values(
                source[2],
                values
            )
            if source[1] == "":
                pass
            elif source[1] == "join_with_space":
                value = ", ".join(value)
        else:
            raise Exception("Invalid variable source configuration")

        if variables[variable]["type"] == "string":
            value = str(value)
        elif variables[variable]["type"] == "string[]":
            value = [str(v) for v in value]
        elif variables[variable]["type"] == "integer":
            value = int(value)
        elif variables[variable]["type"] == "float":
            value = float(value)
        elif variables[variable]["type"] == "date" and isinstance(value,int):
            value = datetime.datetime.fromtimestamp(value).isoformat()
        elif variables[variable]["type"] == "date" and isinstance(value,str):
            value = datetime.datetime.fromisoformat(value).isoformat()

        values[variable] = value

    return values


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_background_ingestor_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_background_ingester_config(arg_namespace)
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info(
        'Starting the Scicat background Ingestor with the following configuration:'
    )
    logger.info(config.to_dict())

    # load metadata schema configurations
    # list files in schema folders
    schemas = {}
    for schema_file in list_schema_files():
        with open(schema_file, 'r') as fh:
            current_schema = json.load(fh)
            schemas[current_schema['id']] = current_schema

    with exit_at_exceptions(logger, daemon=False):
        nexus_file = pathlib.Path(config.single_run_options.nexus_file)
        logger.info("Nexus file to be ingested : ")
        logger.info(nexus_file)

        done_writing_message_file = pathlib.Path(
            config.single_run_options.done_writing_message_file
        )
        logger.info("Done writing message file linked to nexus file : ")
        logger.info(done_writing_message_file)

        # open and read done writing message input file
        done_writing_message = json.load(done_writing_message_file.open())
        logger.info(done_writing_message)

        # open nexus file with h5py
        h5file = h5py.File(nexus_file)

        # load instrument metadata configuration
        metadata_schema = select_applicable_schema(nexus_file, h5file, schemas)

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
        scicat_dataset_pid = create_Scicat_dataset(scicat_dataset)

        # create and populate scicat origdatablock entry
        # with files and hashes previously computed
        scicat_origdatablock = create_scicat_origdatablock(
            scicat_dataset_pid, nexus_file, done_writing_message_file
        )

        # create origdatablock in scicat
        scicat_origdatablock_id = create_scicat_origdatablock(scicat_origdatablock)

        # return successful code
