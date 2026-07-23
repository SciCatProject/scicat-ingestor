# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import logging
import pathlib
from collections.abc import Generator

from confluent_kafka import Consumer
from streaming_data_types import deserialise_pl72, deserialise_wrdn
from streaming_data_types.finished_writing_wrdn import (
    FILE_IDENTIFIER as WRDN_FILE_IDENTIFIER,
)
from streaming_data_types.finished_writing_wrdn import WritingFinished
from streaming_data_types.run_start_pl72 import (
    FILE_IDENTIFIER as RUNSTART_FILE_IDENTIFIER,
)
from streaming_data_types.run_start_pl72 import RunStartInfo

from scicat_configuration import KafkaOptions


def collect_consumer_options(options: KafkaOptions) -> dict:
    """Build a Kafka consumer and configure it according to the ``options``."""
    from dataclasses import asdict

    # Build logger and formatter
    config_dict = {
        key.replace("_", "."): value
        for key, value in asdict(options).items()
        if key not in ("topics", "individual_message_commit") and value != ""
        # We remove empty configurations so that we don't confuse kafka consumer API.
    }
    config_dict["enable.auto.commit"] = (
        not options.individual_message_commit
    ) and options.enable_auto_commit
    if isinstance(bootstrap_servers := options.bootstrap_servers, list):
        # Convert the list to a comma-separated string
        config_dict["bootstrap.servers"] = ",".join(bootstrap_servers)
    else:
        config_dict["bootstrap.servers"] = bootstrap_servers

    return config_dict


def collect_kafka_topics(options: KafkaOptions) -> list[str]:
    """Return the Kafka topics as a list."""
    if isinstance(options.topics, str):
        return options.topics.split(",")
    elif isinstance(options.topics, list):
        return options.topics
    else:
        raise TypeError("The topics must be a list or a comma-separated string.")


def build_consumer(kafka_options: KafkaOptions, logger: logging.Logger) -> Consumer:
    """Build a Kafka consumer and configure it according to the ``options``."""
    consumer_options = collect_consumer_options(kafka_options)
    logger.info(
        # `KafkaOptions` has `__str__` dundermethod for logging.
        # It explicitly choose which configurations can be printed/shown.
        # DO NOT print/log the whole container since it may contain user credentials.
        "Connecting to Kafka with the following parameters: %s",
        str(consumer_options),
    )
    consumer = Consumer(consumer_options)
    if not validate_consumer(consumer, logger):
        return None

    kafka_topics = collect_kafka_topics(kafka_options)
    logger.info("Subscribing to the following Kafka topics: %s", kafka_topics)
    consumer.subscribe(kafka_topics)
    return consumer


def validate_consumer(consumer: Consumer, logger: logging.Logger) -> bool:
    try:
        consumer.list_topics(timeout=1)
    except Exception as err:
        logger.error(
            "Kafka consumer could not be instantiated. "
            "Error message from kafka thread: \n%s",
            err,
        )
        return False
    else:
        logger.info("Kafka consumer successfully instantiated")
        return True


def _validate_wrdn_message_type(message_content: bytes, logger: logging.Logger) -> bool:
    logger.info("Message type: %s", (message_type := message_content[4:8]))
    if message_type == WRDN_FILE_IDENTIFIER:
        logger.info("WRDN message received.")
        return True
    else:
        logger.info("Message of type %s ignored.", message_type)
        return False


def _deserialise_wrdn(
    message_content: bytes, logger: logging.Logger
) -> WritingFinished | None:
    try:
        if not _validate_wrdn_message_type(message_content, logger):
            return
        logger.info("Deserialising WRDN message")
        deserialised_message: WritingFinished = deserialise_wrdn(message_content)
        logger.info(
            "Deserialised WRDN message with job id, %s for file %s.",
            deserialised_message.job_id,
            deserialised_message.file_name,
        )
        logger.debug("Deserialized WRDN message: %.5000s", deserialised_message)
        if deserialised_message.error_encountered:
            logger.info(
                "`error_encountered` is true. Cannot use process the message. %s",
                deserialised_message.message,
            )

        return deserialised_message
    except Exception as e:
        logger.error(
            "Error deserialising message. Error: %s. Raw message: %s",
            e,
            message_content.decode("utf-8", errors="replace"),
        )


def wrdn_messages(
    consumer: Consumer, logger: logging.Logger
) -> Generator[WritingFinished | None, None, None]:
    """Wait for a WRDN message and yield it.

    Yield ``None`` if no message is received or an error is encountered.
    If an error is encountered or fails to deserialise the message,
    the consumer will commit the message
    so that it won't consume the same message again.

    Examples
    --------
    - Structure of a successful writing_done message
        (
          Result=Success
          JobId=99999901-3947-5a87-8377-a85c111f18ba
          File=/ess/raw/coda/999999/raw/coda_estia_999999_00013947.hdf
        )

    - Structure of a failed writing_done message
        (
          Result=Failure
          JobId=99999901-3948-5de6-88ab-085c111f18ba
          File=/ess/raw/coda/999999/raw/coda_freia_999999_00013948.hdf
        ): Unable to set up consumer for source MISSING1 on topic
           freia_MISSING as this topic does not exist.
    """
    num_skipped = 1
    while True:
        # The decision to proceed or stop will be done by the caller.
        timeout = 1.0
        message = consumer.poll(timeout=timeout)
        if message is None:
            num_skipped += 1
            logger.info("Received no messages, %d [s].", timeout * num_skipped)
            yield None
        elif message.error():
            num_skipped = 1  # Reset
            logger.error("Consumer error: %s", message.error())
            consumer.commit(message=message, asynchronous=True)
            yield None
        elif (message_value := message.value()) is None:
            num_skipped = 1  # Reset
            logger.warning(
                "None value received in a message from consumer. "
                "Skipping this message: %s",
                message,
            )
            # It means the produced message has none value...
            consumer.commit(message=message, asynchronous=True)
            yield None
        else:  # A message received without error and with non-None value.
            num_skipped = 1  # Reset
            # retrieve type of message
            message_type = message_value[4:8]
            logger.info("Received message. Type : %s", message_type)

            deserialised_message = _deserialise_wrdn(message_value, logger=logger)
            # If it fails to deserialize the message and return None,
            # or if the error_encountered is True, the message should be committed
            # but should not be processed further.
            if deserialised_message is None or deserialised_message.error_encountered:
                consumer.commit(message=message, asynchronous=True)
                yield None
            else:
                yield deserialised_message


def _validate_pl72_message_type(message_content: bytes, logger: logging.Logger) -> bool:
    logger.info("Message type: %s", (message_type := message_content[4:8]))
    if message_type == RUNSTART_FILE_IDENTIFIER:
        logger.info("RunStart message received.")
        return True
    else:
        logger.info("Message of type %s ignored.", message_type)
        return False


def _deserialise_run_start(
    message_content: bytes, logger: logging.Logger
) -> RunStartInfo | None:
    deserialized_message: RunStartInfo | None = None
    if _validate_pl72_message_type(message_content, logger):
        logger.info("Deserialising PL72(RunStart) message")
        deserialized_message = deserialise_pl72(message_content)
        logger.info(
            "Deserialised PL72(RunStart) message with job id, %s for file %s.",
            deserialized_message.job_id,
            deserialized_message.filename,
        )
        logger.debug(
            "Deserialised PL72(RunStart) message: %.150s", deserialized_message
        )

    return deserialized_message


def run_start_messages(
    consumer: Consumer, logger: logging.Logger
) -> Generator[RunStartInfo | None, None, None]:
    """Wait for a PL72(RunStart) message and yield it.

    Yield ``None`` if no message is received or an error is encountered.
    """
    num_skipped = 1
    while True:
        # The decision to proceed or stop will be done by the caller.
        timeout = 1.0
        message = consumer.poll(timeout=timeout)
        if message is None:
            num_skipped += 1
            logger.info("Received no messages, %d [s].", timeout * num_skipped)
            yield None
        elif message.error():
            num_skipped = 1  # Reset
            logger.error("Consumer error: %s", message.error())
            yield None
        else:
            num_skipped = 1  # Reset
            yield _deserialise_run_start(message.value(), logger)


def save_message_to_file(
    *,
    message: WritingFinished | RunStartInfo,
    message_file_path: pathlib.Path,
) -> None:
    """Dump the ``message`` into ``message_file_path``."""
    import json

    with message_file_path.open("w") as fh:
        json.dump(message, fh)
