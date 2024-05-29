# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import logging

from scicat_configuration import build_main_arg_parser, build_scicat_config
from scicat_kafka import build_consumer
from scicat_logging import build_logger


def quit(logger: logging.Logger, unexpected: bool = True) -> None:
    """Log the message and exit the program."""
    import sys

    logger.info("Exiting ingestor")
    sys.exit(1 if unexpected else 0)


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_main_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_config(arg_namespace)
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info('Starting the Scicat Ingestor with the following configuration:')
    logger.info(config.to_dict())

    # Kafka consumer
    if build_consumer(config.kafka_options, logger) is None:
        quit(logger)
