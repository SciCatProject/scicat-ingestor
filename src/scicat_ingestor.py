# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
# ruff: noqa: E402, F401

import importlib.metadata

try:
    __version__ = importlib.metadata.version(__package__ or __name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

del importlib

from scicat_configuration import build_main_arg_parser, build_scicat_config
from scicat_kafka import build_consumer, wrdn_messages
from scicat_logging import build_logger
from system_helpers import exit_at_exceptions


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_main_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_config(arg_namespace)
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info('Starting the Scicat online Ingestor with the following configuration:')
    logger.info(config.to_dict())

    with exit_at_exceptions(logger):
        # Kafka consumer
        if (consumer := build_consumer(config.kafka_options, logger)) is None:
            raise RuntimeError("Failed to build the Kafka consumer")

        # Receive messages
        for message in wrdn_messages(consumer, logger):
            logger.info("Processing message: %s", message)

            # check if we have received a WRDN message
            # if message is not a WRDN, we get None back
            if message:
                # extract nexus file name from message

                # extract job id from message

                # saves the WRDN message in a file

                # instantiate a new process and runs backeground ingestor
                # on the nexus file
                ...

            # check if we need to commit the individual message
            elif config.kafka_options.individual_message_commit:
                consumer.commit(message=message)
