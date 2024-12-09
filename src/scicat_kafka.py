# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import logging
import pathlib
from collections.abc import Generator

from confluent_kafka import Consumer
from scicat_configuration import KafkaOptions
from streaming_data_types import deserialise_wrdn
from streaming_data_types.finished_writing_wrdn import (
    FILE_IDENTIFIER as WRDN_FILE_IDENTIFIER,
)
from streaming_data_types.finished_writing_wrdn import WritingFinished


def collect_consumer_options(options: KafkaOptions) -> dict:
    """Build a Kafka consumer and configure it according to the ``options``."""
    from dataclasses import asdict

    # Build logger and formatter
    config_dict = {
        key.replace("_", "."): value
        for key, value in asdict(options).items()
        if key not in ("topics", "individual_message_commit")
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
    logger.info("Connecting to Kafka with the following parameters:")
    logger.info(consumer_options)
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


def _validate_wrdn_message_type(
    message_content: bytes,
    logger: logging.Logger
) -> bool:
    message_type = message_content[4:8]
    logger.info("Message type: %s", message_type)
    if message_type == WRDN_FILE_IDENTIFIER:
        logger.info("WRDN message received.")
        return True
    else:
        logger.info("Message of type %s ignored", message_type)
        return False


def _filter_error_encountered(
    deserialized_message: WritingFinished, 
    logger: logging.Logger
) -> WritingFinished | None:
    """Filter out messages with the ``error_encountered`` flag set to True."""
    if deserialized_message.error_encountered:
        logger.error(
            "Unable to deserialize message. ``error_encountered`` is true. Skipping the message."
        )
        return None
    else:
        logger.info(
            "Message successfully deserialized."
        )
        return deserialized_message


def _deserialise_wrdn(
    message_content: bytes, 
    logger: logging.Logger
) -> WritingFinished | None:
    deserialized_message: WritingFinished | None = None
    if _validate_wrdn_message_type(message_content, logger):
        logger.info("Deserialising WRDN message")
        deserialized_message: WritingFinished = deserialise_wrdn(message_content)
        deserialized_message = _filter_error_encountered(deserialized_message, logger)
        logger.info("Deserialised WRDN message: %.5000s", deserialized_message)

    return deserialized_message

def wrdn_messages(
    consumer: Consumer, 
    logger: logging.Logger
) -> Generator[WritingFinished | None, None, None]:
    """
    Wait for a WRDN message and yield it.

    Yield ``None`` if no message is received or an error is encountered.
    """
    while True:
        # The decision to proceed or stop will be done by the caller.
        message = consumer.poll(timeout=1.0)
        if message is None:
            logger.info("Received no messages")
            yield None
        elif message.error():
            logger.error("Consumer error: %s", message.error())
            yield None
        else:
            # retrieve type of message
            message_value = message.value()
            message_type = message_value[4:8]
            logger.info("Received message. Type : %s", message_type)
            yield _deserialise_wrdn(message_value, logger)


# def compose_message_path(
#     *,
#     target_dir: pathlib.Path,
#     nexus_file_path: pathlib.Path,
#     message_saving_options: MessageSavingOptions,
# ) -> pathlib.Path:
#     """Compose the message path based on the nexus file path and configuration."""
#
#     return target_dir / (
#         pathlib.Path(
#             ".".join(
#                 (
#                     nexus_file_path.stem,
#                     message_saving_options.message_file_extension.removeprefix("."),
#                 )
#             )
#         )
#     )


def save_message_to_file(
    *,
    message: WritingFinished,
    message_file_path: pathlib.Path,
) -> None:
    """Dump the ``message`` into ``message_file_path``."""
    import json

    with message_file_path.open("w") as fh:
        json.dump(message, fh)
