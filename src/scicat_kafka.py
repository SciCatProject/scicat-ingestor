# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import logging

from confluent_kafka import Consumer

from scicat_configuration import kafkaOptions


def collect_consumer_options(options: kafkaOptions) -> dict:
    """Build a Kafka consumer and configure it according to the ``options``."""
    from dataclasses import asdict

    # Build logger and formatter
    config_dict = {
        key.replace('_', '.'): value
        for key, value in asdict(options).items()
        if key not in ('topics', 'individual_message_commit')
    }
    config_dict['enable.auto.commit'] = (
        not options.individual_message_commit
    ) and options.enable_auto_commit
    return config_dict


def collect_kafka_topics(options: kafkaOptions) -> list[str]:
    """Return the Kafka topics as a list."""
    if isinstance(options.topics, str):
        return options.topics.split(',')
    elif isinstance(options.topics, list):
        return options.topics
    else:
        raise TypeError('The topics must be a list or a comma-separated string.')


def build_consumer(kafka_options: kafkaOptions, logger: logging.Logger) -> Consumer:
    """Build a Kafka consumer and configure it according to the ``options``."""
    consumer_options = collect_consumer_options(kafka_options)
    logger.info('Connecting to Kafka with the following parameters:')
    logger.info(consumer_options)
    consumer = Consumer(consumer_options)
    if not validate_consumer(consumer, logger):
        return None

    kafka_topics = collect_kafka_topics(kafka_options)
    logger.info(f'Subscribing to the following Kafka topics: {kafka_topics}')
    consumer.subscribe(kafka_topics)
    return Consumer(consumer_options)


def validate_consumer(consumer: Consumer, logger: logging.Logger) -> bool:
    try:
        consumer.list_topics(timeout=1)
    except Exception as err:
        logger.error(
            "Kafka consumer could not be instantiated. "
            f"Error message from kafka thread: \n{err}"
        )
        return False
    else:
        logger.info('Kafka consumer successfully instantiated')
        return True
