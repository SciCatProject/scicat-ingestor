# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
# ruff: noqa: E402, F401

import importlib.metadata
import logging
import pathlib

try:
    __version__ = importlib.metadata.version(__package__ or __name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

del importlib

from scicat_configuration import (
    MessageSavingOptions,
    build_main_arg_parser,
    build_scicat_ingester_config,
)
from scicat_kafka import (
    WritingFinished,
    build_consumer,
    compose_message_path,
    save_message_to_file,
    wrdn_messages,
)
from scicat_logging import build_logger
from scicat_path_helpers import select_target_directory
from system_helpers import exit_at_exceptions


def dump_message_to_file_if_needed(
    *,
    logger: logging.Logger,
    message_file_path: pathlib.Path,
    message_saving_options: MessageSavingOptions,
    message: WritingFinished,
) -> None:
    """Dump the message to a file according to the configuration."""
    if not message_saving_options.message_to_file:
        logger.info("Message saving to file is disabled. Skipping saving message.")
        return
    elif not message_file_path.parent.exists():
        logger.info("Message file directory not accessible. Skipping saving message.")
        return

    logger.info("Message will be saved in %s", message_file_path)
    save_message_to_file(
        message=message,
        message_file_path=message_file_path,
    )
    logger.info("Message file saved")


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_main_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_ingester_config(arg_namespace)
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info('Starting the Scicat online Ingestor with the following configuration:')
    logger.info(config.to_dict())

    # Often used options
    message_saving_options = config.kafka_options.message_saving_options

    with exit_at_exceptions(logger):
        # Kafka consumer
        if (consumer := build_consumer(config.kafka_options, logger)) is None:
            raise RuntimeError("Failed to build the Kafka consumer")

        # Receive messages
        for message in wrdn_messages(consumer, logger):
            logger.info("Processing message: %s", message)

            # Check if we have received a WRDN message.
            # ``message: None | WritingFinished``
            if message:
                # Extract nexus file path from the message.
                nexus_file_path = pathlib.Path(message.file_name)
                file_saving_dir = select_target_directory(
                    config.ingestion_options.file_handling_options, nexus_file_path
                )
                dump_message_to_file_if_needed(
                    logger=logger,
                    message_saving_options=message_saving_options,
                    message=message,
                    message_file_path=compose_message_path(
                        target_dir=file_saving_dir,
                        nexus_file_path=nexus_file_path,
                        message_saving_options=message_saving_options,
                    ),
                )
                # instantiate a new process and runs background ingestor
                # on the nexus file
                # use open process and wait for outcome
                """
                background_ingestor
                    -c configuration_file
                    -f nexus_filename
                    -j job_id
                    -m message_file_path  # optional depending on the
                                          # message_saving_options.message_output
                """

                # if background process is successful
                # check if we need to commit the individual message
                """
                if config.kafka_options.individual_message_commit \
                    and background_process is successful:
                    consumer.commit(message=message)
                """
