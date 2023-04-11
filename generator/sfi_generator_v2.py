#!/usr/bin/env python3
#
#
import argparse
import copy
import datetime
import json
import time
from uuid import uuid4
import functools

from streaming_data_types import serialise_wrdn
import logging

from confluent_kafka import Producer


def serialize_message(message: dict) -> bytes:
    """
    prepare message to send on kafka message
    :return: serialize wrdn message
    """

    return serialise_wrdn(
        service_id=message["service_id"],
        job_id=message["job_id"],
        error_encountered=message["error_encountered"],
        file_name=message["file_name"],
        metadata=json.dumps(message["metadata"])
    )


def delivery_report(errmsg, msg, log_message=print):
    """
    Reports the Failure or Success of a message delivery.
    Args:
        errmsg  (KafkaError): The Error that occurred while message producing.
        msg    (Actual message): The message that was produced.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if errmsg is not None:
        log_message("Delivery failed for Message: {} : {}\n".format(msg.key(), errmsg))
        return
    log_message("Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}\n".format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def post_message(lconfig: dict, message: bytes, log_message=print):
    """

    :param config:
    :return:
    """
    kafka_config = lconfig["kafka"]
    kafka_topics = kafka_config["topics"]
    kafka_topics = kafka_topics.split(',') if isinstance(kafka_topics,str) else kafka_topics

    log_message(
        "Connecting to Kafka\n" +
        " - server ............: {}\n".format(kafka_config["bootstrap_servers"]) +
        " - topics ............: {}\n".format(kafka_topics)
    )
    producer = Producer({
        'bootstrap.servers': kafka_config["bootstrap_servers"]
    })
    log_message("Kafka producer successfully instantiated... apparently!!!\n")

    try:
        for topic in kafka_topics:
            producer.produce(
                topic=topic,
                value=message,
                on_delivery=functools.partial(
                    delivery_report,
                    log_message=log_message
                )
            )
        producer.flush()
        log_message("Message posted.")

    except Exception as e:
        log_message("Exception occurred during message posting. {}".format(e))


def prep_field(input_value: str, substitute_values: dict):
    """

    :param input_value:
    :param substitute_values:
    :return:
    """
    output_value = input_value
    for k,v in substitute_values.items():
        token = "<" + k + ">"
        output_value = output_value.replace(token, str(v))
    return output_value


def log_message(message, logger= None):
    if logger is not None:
        logger.info(message)

def prep_message(config):

    today = datetime.datetime.now()
    config['values']['end_time'] = time.mktime(today.timetuple())
    config['values']['start_time'] = config['values']['end_time'] - config['defaults']['job_duration']
    # copy options into run options
    dict_message = copy.deepcopy(config["message"])
    dict_message["job_id"] = config["values"]["job_id"]
    dict_message["file_name"] = config["values"]["file_name"]

    for mk in config["refactor_metadata_keys"]:
        dict_message['metadata'][mk] = prep_field(dict_message['metadata'][mk], config['values'])

    return dict_message

        
def send_message(config, log_message=log_message):
    
    dict_message = prep_message(config)
    log_message("Plain Message : {}".format(json.dumps(dict_message)))

    serialized_message = serialize_message(dict_message)
    log_message("Message serialized : {}".format(serialized_message))

    post_message(config, serialized_message)
    log_message("Message posted")
    

def load_config(config_file):
    with open(config_file, "r") as fh:
        data = fh.read()
        config = json.loads(data)
        return config


#
# ======================================
# define arguments
parser = argparse.ArgumentParser()

parser.add_argument(
    '-c', '--cf', '--config', '--config-file',
    default='sfi_generator_config.json',
    dest='config_file',
    help='Configuration file name. Default": sfi_generator_config.json',
    type=str
)

if __name__ == "__main__":
    # get input argumengts
    args = parser.parse_args()

    # get configuration from file and updates with command line options
    config_file = args.config_file if args.config_file else "sfi_generator_config.json"
    config = load_config(config_file)

    # instantiate logger
    logger = logging.getLogger('esd extract parameters')
    logger.setLevel(run_options['logging_level'])
    formatter = logging.Formatter(run_options['log_prefix'] + '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # log to standard output
    ch = logging.StreamHandler()
    ch.setLevel(run_options['logging_level'])
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.info("Configuration : {}".format(json.dumps(config)))
    
    def log_message(message):
        if logger is not None:
            logger.info(message)

    send_message(log_message=log_message)