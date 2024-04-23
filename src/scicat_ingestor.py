# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
from scicat_configuration import build_main_arg_parser, build_scicat_config
from scicat_logging import build_logger


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_main_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_config(arg_namespace)
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info('Starting the Scicat Ingestor with the following configuration:')
    logger.info(config.to_dict())
