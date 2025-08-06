# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import datetime
import logging
import logging.handlers

import graypy

from scicat_configuration import OfflineIngestorConfig, OnlineIngestorConfig


def build_devtool_logger(tool_name: str) -> logging.Logger:
    from rich.logging import RichHandler

    logger = logging.getLogger(f'scicat-devtool-{tool_name}')

    # Since it is a dev tool, we always set the level to DEBUG
    logger.addHandler(RichHandler(level=logging.DEBUG, markup=True))
    logger.setLevel(logging.DEBUG)
    return logger


def build_logger(
    config: OnlineIngestorConfig | OfflineIngestorConfig,
) -> logging.Logger:
    """Build a logger and configure it according to the ``config``."""
    logging_options = config.logging

    # Build logger and formatter
    logger = logging.getLogger('esd extract parameters')
    formatter = logging.Formatter(
        " - ".join(
            (
                logging_options.log_message_prefix,
                '%(asctime)s',
                '%(name)s',
                '%(levelname)s',
                '%(message)s',
            )
        )
    )

    # Add FileHandler
    if logging_options.file_log:
        file_name_components = [logging_options.file_log_base_name]
        if logging_options.file_log_timestamp:
            file_name_components.append(
                datetime.datetime.now(datetime.UTC).strftime('%Y%m%d%H%M%S%f')
            )
        file_name_components.append('.log')

        file_name = '_'.join(file_name_components)
        file_handler = logging.FileHandler(file_name, mode='w', encoding='utf-8')
        logger.addHandler(file_handler)

    # Add SysLogHandler
    if logging_options.system_log:
        logger.addHandler(logging.handlers.SysLogHandler(address='/dev/log'))

    # Add graylog handler
    if logging_options.graylog:
        graylog_handler = graypy.GELFUDPHandler(
            logging_options.graylog_host,
            int(logging_options.graylog_port),
            facility=logging_options.graylog_facility,
        )
        logger.addHandler(graylog_handler)

    # Set the level and formatter for all handlers
    logger.setLevel(logging_options.logging_level)
    for handler in logger.handlers:
        handler.setLevel(logging_options.logging_level)
        handler.setFormatter(formatter)

    # Add StreamHandler
    # streamer handler is added last since it is using different formatter
    if logging_options.verbose:
        from rich.logging import RichHandler

        logger.addHandler(RichHandler(level=logging_options.logging_level))

    return logger
