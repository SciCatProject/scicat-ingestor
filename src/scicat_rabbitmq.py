# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)

import json
import logging
import os
import threading
import queue
import time
from typing import Generator, Optional, Any, Dict

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError
from dotenv import load_dotenv

from scicat_configuration import RabbitMQOptions
from scicat_kafka import WritingFinished


load_dotenv()

def collect_rabbitmq_options(options: RabbitMQOptions) -> Dict:
    """Build RabbitMQ connection parameters from options, using env vars if available."""
    # Get credentials from environment variables if available, otherwise use config
    username = os.environ.get("RABBITMQ_USERNAME") or options.username
    password = os.environ.get("RABBITMQ_PASSWORD") or options.password
    
    # Get connection details from environment variables if available
    host = os.environ.get("RABBITMQ_HOST") or options.host
    port = int(os.environ.get("RABBITMQ_PORT") or options.port)
    virtual_host = os.environ.get("RABBITMQ_VIRTUAL_HOST") or options.virtual_host
    
    return {
        "host": host,
        "port": port,
        "virtual_host": virtual_host,
        "credentials": pika.PlainCredentials(username, password),
        "heartbeat": 60
    }


def build_rabbitmq_consumer(options: RabbitMQOptions, logger: logging.Logger):
    """Build RabbitMQ consumer based on configuration."""
    try:
        # Convert options to connection parameters
        connection_params = pika.ConnectionParameters(**collect_rabbitmq_options(options))
        
        logger.info("Connecting to RabbitMQ with the following parameters:")
        logger.info({
            "host": connection_params.host, 
            "port": connection_params.port, 
            "virtual_host": connection_params.virtual_host,
            "queue": options.queue_name
        })
        
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        
        # If exchange name is provided, declare the exchange first
        if options.exchange_name:
            logger.info("Declaring exchange: %s", options.exchange_name)
            # Using topic exchange type by default, can be made configurable if needed
            channel.exchange_declare(exchange=options.exchange_name, exchange_type='topic', durable=True)
        
        queue_exists = False
        try:
            # First try to access the queue in passive mode (don't create if not exists)
            logger.info("Checking if queue '%s' exists...", options.queue_name)
            channel.queue_declare(queue=options.queue_name, passive=True)
            logger.info("Queue '%s' exists, using existing configuration", options.queue_name)
            queue_exists = True
        except pika.exceptions.ChannelClosedByBroker as e:
            # Reopen channel if it was closed by the broker
            if e.reply_code == 404:  # Queue not found
                logger.info("Queue '%s' does not exist, creating it", options.queue_name)
                # Reconnect since the channel was closed
                connection = pika.BlockingConnection(connection_params)
                channel = connection.channel()
                # Create the queue with basic parameters
                channel.queue_declare(queue=options.queue_name, durable=True)
            else:
                # Some other issue with the queue declaration
                logger.error("Error accessing queue '%s': %s", options.queue_name, e)
                raise
        
        # If both exchange name and routing key are provided, bind the queue to the exchange
        if options.exchange_name:
            routing_key = options.routing_key if options.routing_key else "#"
            logger.info("Binding queue '%s' to exchange '%s' with routing key '%s'", 
                        options.queue_name, options.exchange_name, routing_key)
            channel.queue_bind(
                exchange=options.exchange_name,
                queue=options.queue_name,
                routing_key=routing_key
            )
        
        # Set prefetch count to control parallelism
        channel.basic_qos(prefetch_count=options.prefetch_count)
        
        logger.info("Successfully connected to RabbitMQ at %s:%s", connection_params.host, connection_params.port)
        return {"connection": connection, "channel": channel, "options": options}
    
    except AMQPConnectionError as e:
        logger.error("Failed to connect to RabbitMQ: %s", e)
        return None
    except Exception as e:
        logger.error("Unexpected error building RabbitMQ consumer: %s", e)
        return None


class RabbitMQConsumer:
    """Event-driven RabbitMQ message consumer."""

    def __init__(self, consumer: dict, logger: logging.Logger):
        self.channel = consumer["channel"]
        self.connection = consumer["connection"] 
        self.options = consumer["options"]
        self.logger = logger
        self.message_queue = queue.Queue()
        self._consumer_tag = None
        self.running = False
        self.thread = None

    def start_consuming(self):
        """Start consuming messages in a separate thread."""
        self.running = True
        self.thread = threading.Thread(target=self._consume_thread)
        self.thread.daemon = True
        self.thread.start()
        self.logger.info(f"Started RabbitMQ consumer thread for queue: {self.options.queue_name}")

    def _consume_thread(self):
        """Consumer thread that runs the event loop."""
        try:
            self._consumer_tag = self.channel.basic_consume(
                queue=self.options.queue_name,
                on_message_callback=self._on_message,
                auto_ack=self.options.auto_ack
            )
            self.logger.info(f"RabbitMQ consumer registered with tag: {self._consumer_tag}")
            
            # Start the event loop
            self.channel.start_consuming()
        except Exception as e:
            self.logger.error(f"Error in RabbitMQ consumer thread: {e}")
        finally:
            self.running = False
            self.logger.info("RabbitMQ consumer thread stopping")
            
    def _on_message(self, ch, method, properties, body):
        """Callback when a message is received."""
        try:
            # Parse message
            message_dict = json.loads(body.decode('utf-8'))
            self.logger.debug("Received message: %s", message_dict)
            
            # Check if we're in direct data mode
            direct_data_mode = self.options.direct_data_ingestion
            
            if direct_data_mode:
                # In direct data mode, create a message wrapper dictionary instead of trying to
                # add attributes to the immutable WritingFinished object
                self.logger.debug("Processing message in direct data mode")
                
                # Create WritingFinished object
                writing_finished = WritingFinished(
                    job_id="direct_data", 
                    file_name="direct_data",
                    service_id="rabbitmq", 
                    error_encountered=False,
                    metadata=None,
                    message=json.dumps(message_dict)  # Convert dict back to JSON string
                )
                
                # Create a wrapper dictionary to hold both the message and metadata
                message_wrapper = {
                    "message": writing_finished,
                    "is_direct_data": True,
                    "message_data": message_dict,
                    "delivery_tag": method.delivery_tag
                }
                
                # Add to thread-safe queue as a wrapper
                self.message_queue.put(message_wrapper)
                self.logger.debug("Added direct data message to queue")
                    
            else:
                # Traditional file path mode
                job_id = message_dict.get("job_id")
                file_name = message_dict.get("file_name") or message_dict.get("nexus_file")
                
                if not job_id or not file_name:
                    self.logger.warning("Skipping message without required fields: %s", message_dict)
                    if not self.options.auto_ack:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                    
                # Create WritingFinished object for file path processing
                writing_finished = WritingFinished(
                    job_id=job_id,
                    file_name=file_name,
                    service_id="rabbitmq",
                    error_encountered=False,
                    metadata=None,
                    message=json.dumps(message_dict)
                )
                
                # Create wrapper for file message
                message_wrapper = {
                    "message": writing_finished,
                    "is_direct_data": False,
                    "delivery_tag": method.delivery_tag
                }
                
                # Add wrapper to thread-safe queue
                self.message_queue.put(message_wrapper)
                self.logger.debug(f"Added file path message to queue for job_id {job_id}")
                
        except json.JSONDecodeError as e:
            self.logger.error("Failed to decode message as JSON: %s. Error: %s", body, e)
            if not self.options.auto_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            self.logger.error("Error processing RabbitMQ message: %s", e)
            if not self.options.auto_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)

    def stop_consuming(self):
        """Stop consuming messages."""
        try:
            if self._consumer_tag and self.channel.is_open:
                self.channel.basic_cancel(self._consumer_tag)
                self.logger.info(f"Canceled consumer with tag: {self._consumer_tag}")
            
            if self.channel.is_open:
                self.channel.stop_consuming()
                self.logger.info("Stopped consuming messages")
                
            self.running = False
            
            # Wait for the thread to finish
            if self.thread and self.thread.is_alive():
                self.thread.join(timeout=2.0)
                if self.thread.is_alive():
                    self.logger.warning("Consumer thread did not terminate cleanly")
        except Exception as e:
            self.logger.error(f"Error stopping RabbitMQ consumer: {e}")

    def close(self):
        """Close the connection."""
        try:
            self.stop_consuming()
            if self.connection and self.connection.is_open:
                self.connection.close()
                self.logger.info("RabbitMQ connection closed")
        except Exception as e:
            self.logger.error(f"Error closing RabbitMQ connection: {e}")


def rabbitmq_messages(consumer: dict, logger: logging.Logger) -> Generator[Optional[Dict], None, None]:
    """
    Generator that yields message wrappers from RabbitMQ using event-driven consumption.
    
    Each wrapper contains the WritingFinished instance and additional metadata.
    """
    # Create and start the consumer
    rabbitmq_consumer = RabbitMQConsumer(consumer, logger)
    rabbitmq_consumer.start_consuming()
    
    try:
        # Yield messages as they become available
        while True:
            try:
                # Block with a reasonable timeout to allow for clean shutdown
                message_wrapper = rabbitmq_consumer.message_queue.get(timeout=1.0)
                yield message_wrapper
            except queue.Empty:
                # Continue waiting for messages
                continue
                
    except KeyboardInterrupt:
        logger.info("RabbitMQ consumption interrupted")
    except Exception as e:
        logger.error(f"Error in RabbitMQ message generator: {e}")
    finally:
        # Clean up
        rabbitmq_consumer.close()


def acknowledge_message(consumer: dict, message_wrapper: Dict, logger: logging.Logger):
    """Acknowledge a RabbitMQ message."""
    try:
        if isinstance(message_wrapper, dict) and "delivery_tag" in message_wrapper:
            delivery_tag = message_wrapper["delivery_tag"]
            consumer["channel"].basic_ack(delivery_tag=delivery_tag)
            logger.debug("Acknowledged message with delivery tag %s", delivery_tag)
        else:
            logger.warning("Message wrapper doesn't have delivery_tag, can't acknowledge")
    except Exception as e:
        logger.error("Error acknowledging RabbitMQ message: %s", e)