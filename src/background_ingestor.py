# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
# import scippnexus as snx
import json
import pathlib

from scicat_configuration import (
    build_background_ingestor_arg_parser,
    build_scicat_background_ingester_config,
)
from scicat_logging import build_logger
from system_helpers import exit_at_exceptions


def list_schema_files(schemas_folder):
    return os.path.dirlist(schemas_folder)


def select_applicable_schema(nexus_file, nxs, schemas):
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

        # open nexus file
        nxs = snx.File(nexus_file)

        # load instrument metadata configuration
        metadata_schema = select_applicable_schema(nexus_file, nxs, schemas)

        # define variables values
        variables_values = assign_variables_values(
            metadata_schema['variables'], nxs, config
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
