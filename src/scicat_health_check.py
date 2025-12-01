# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
"""
This module contains the health check server for the online ingestor.
It exposes an HTTP endpoint that checks the status of Kafka, Storage and SciCat.
"""

import json
import logging
import pathlib
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
from time import sleep
from typing import Any

import requests
from confluent_kafka import Consumer

from scicat_configuration import OnlineIngestorConfig


class HealthCheckHandler(BaseHTTPRequestHandler):
    """
    HTTP Handler for the health check endpoint.
    It checks the status of Kafka, Storage and SciCat.
    """

    def __init__(
        self,
        config: OnlineIngestorConfig,
        consumer: Consumer,
        logger: logging.Logger,
        *args: Any,
        **kwargs: Any,
    ):
        self.config: OnlineIngestorConfig = config
        self.consumer: Consumer = consumer
        self.logger: logging.Logger = logger
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/health":
            kafka_status = self._check_kafka()
            storage_status = self._check_storage()
            scicat_status = self._check_scicat()

            health_status = {
                "kafka": kafka_status,
                "storage": storage_status,
                "scicat": scicat_status,
            }

            if all(health_status.values()):
                self.send_response(200)
            else:
                self.send_response(503)

            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(health_status).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def _check_kafka(self) -> bool:
        """Check if Kafka is reachable."""
        try:
            self.consumer.list_topics(timeout=5)
            return True
        except Exception as e:
            self.logger.error("Health check: Kafka connection failed: %s", e)
            return False

    def _check_storage(self) -> bool:
        """Check if the storage directory is accessible."""
        try:
            file_handling = self.config.ingestion.file_handling
            directory = file_handling.data_directory
            if not directory:
                self.logger.warning("Health check: No data_directory configured.")
                return False
            path = pathlib.Path(directory)
            if not path.exists():
                self.logger.error("Health check: Storage path does not exist: %s", path)
                return False

            # Attempt to list the directory to make sure the mount is accessible.
            next(path.iterdir(), None)
            return True
        except Exception as e:
            self.logger.error("Health check: Storage access failed: %s", e)
            return False

    def _check_scicat(self) -> bool:
        """Check if SciCat is reachable."""
        try:
            scicat_config = self.config.scicat
            url = scicat_config.health_url
            response = requests.get(url, timeout=5)
            return response.status_code == 200
        except Exception as e:
            self.logger.error("Health check: SciCat connection failed: %s", e)
            return False

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Disable default logging of BaseHTTPRequestHandler


def _serve_health_server(
    server: HTTPServer,
    logger: logging.Logger,
    restart_delay: float = 5.0,
) -> None:
    """Run the HTTP server forever, restarting if it crashes."""

    while True:
        try:
            server.serve_forever()
        except Exception as exc:
            logger.error(
                "Health check server stopped unexpectedly: %s. Restarting in %s seconds.",
                exc,
                restart_delay,
            )
            sleep(restart_delay)


def start_health_server(
    config: OnlineIngestorConfig, consumer: Consumer, logger: logging.Logger
) -> None:
    """Start the health check server in a daemon thread."""
    handler = partial(HealthCheckHandler, config, consumer, logger)
    host = config.health_check.host
    port = config.health_check.port
    server = HTTPServer((host, port), handler)
    thread = threading.Thread(target=_serve_health_server, args=(server, logger))
    thread.daemon = True
    thread.start()
    logger.info("Health check server started on %s:%s", host, port)
