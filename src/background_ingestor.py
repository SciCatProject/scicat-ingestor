# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
# import scippnexus as snx
import json
import pathlib

from scicat_configuration import (
    build_background_ingestor_arg_parser,
    build_scicat_config,
)
from scicat_logging import build_logger
from system_helpers import exit_at_exceptions


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_background_ingestor_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_config(arg_namespace)
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info(
        'Starting the Scicat background Ingestor with the following configuration:'
    )
    logger.info(config.to_dict())

    with exit_at_exceptions(logger):
        nexus_file = pathlib.Path(arg_namespace.nexus_file)
        logger.info("Nexus file to be ingested : ")
        logger.info(nexus_file)

        done_writing_message_file = pathlib.Path(
            arg_namespace.arg_namespace.done_writing_message_file
        )
        logger.info("Done writing message file linked to nexus file : ")
        logger.info(done_writing_message_file)

        # open and read done writing message input file
        done_writing_message = json.load(done_writing_message_file.open())
        logger.info(done_writing_message)

        # open nexus file
        # nxs = snx.File(nexus_file)

        # extract instrument

        # load instrument metadata configuration

        # retrieve information regarding the proposal

        # extract and prepare metadata

        # create b2blake hash of all the files

        # create and populate scicat dataset entry

        # create and populate scicat origdatablock entry
        # with files and hashes previously computed

        pass
