# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
# ruff: noqa: E402, F401

import importlib.metadata

try:
    __version__ = importlib.metadata.version(__package__ or __name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

del importlib

from scicat_configuration import build_main_arg_parser, build_scicat_ingester_config
from scicat_kafka import build_consumer, wrdn_messages
from scicat_logging import build_logger
from system_helpers import exit_at_exceptions


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_main_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_ingester_config(arg_namespace)
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
                nexus_filename = message.file_name

                # extract job id from message
                job_id = message.job_id

                if config["run_options"]["message_to_file"]:
                    # Move this to library file as it is used also in background ingestor
                    ingestor_files_path = (
                        os.path.join(
                            os.path.dirname(path_name),
                            config["run_options"]["ingestor_files_folder"],
                        )
                        if config["run_options"]["hdf_structure_output"]
                        == "SOURCE_FOLDER"
                        else os.path.abspath(
                            config["run_options"]["files_output_folder"]
                        )
                    )
                    logger.info("Ingestor files folder: {}".format(ingestor_files_path))

                    message_file_path = ingestor_files_path
                    logger.info(
                        "message file will be saved in {}".format(message_file_path)
                    )
                    if os.path.exists(message_file_path):
                        message_file_name = (
                            os.path.splitext(filename)[0]
                            + config["run_options"]["message_file_extension"]
                        )
                        logger.info("message file name : " + message_file_name)
                        message_full_file_path = os.path.join(
                            message_file_path, message_file_name
                        )
                        logger.info(
                            "message full file path : " + message_full_file_path
                        )
                        with open(message_full_file_path, 'w') as fh:
                            json.dump(message, fh)
                        logger.info("message saved to file")
                        if config["run_options"]["message_output"] == "SOURCE_FOLDER":
                            files_list += [
                                {
                                    "path": message_full_file_path,
                                    "size": len(json.dumps(entry)),
                                }
                            ]
                        fix_dataset_source_folder = True
                    else:
                        logger.info("Message file path not accessible")

                # instantiate a new process and runs background ingestor
                # on the nexus file
                # use open process and wait for outcome
                """
                background_ingestor
                    -c configuration_file
                    -f nexus_filename
                    -j job_id
                    -m message_file_path
                """

                # if background process is successful
                # check if we need to commit the individual message
                """
                if config.kafka_options.individual_message_commit and background_process is successful:
                    consumer.commit(message=message)
                """
