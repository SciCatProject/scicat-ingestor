import json
import unittest
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from scicat_health_check import (
    HealthCheckHandler,
    _serve_health_server,
    start_health_server,
)


class TestHealthCheckHandler(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_consumer = MagicMock()
        self.mock_logger = MagicMock()
        self.mock_request = MagicMock()
        self.mock_client_address = ("127.0.0.1", 12345)
        self.mock_server = MagicMock()

    def _create_handler(self):
        with patch("scicat_health_check.BaseHTTPRequestHandler.__init__"):
            handler = HealthCheckHandler(
                self.mock_config,
                self.mock_consumer,
                self.mock_logger,
                self.mock_request,
                self.mock_client_address,
                self.mock_server,
            )
            handler.path = ""
            handler.wfile = MagicMock()
            return handler

    def test_do_GET_health_success(self):
        handler = self._create_handler()
        handler.path = "/health"

        handler._check_kafka = MagicMock(return_value=True)
        handler._check_storage = MagicMock(return_value=True)
        handler._check_scicat = MagicMock(return_value=True)

        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_GET()

        handler.send_response.assert_called_with(200)
        handler.send_header.assert_called_with("Content-type", "application/json")
        handler.end_headers.assert_called_once()

        expected_response = json.dumps(
            {"kafka": True, "storage": True, "scicat": True}
        ).encode("utf-8")
        cast(MagicMock, handler.wfile.write).assert_called_with(expected_response)

    def test_do_GET_health_failure(self):
        handler = self._create_handler()
        handler.path = "/health"

        handler._check_kafka = MagicMock(return_value=True)
        handler._check_storage = MagicMock(return_value=False)
        handler._check_scicat = MagicMock(return_value=True)

        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_GET()

        handler.send_response.assert_called_with(503)

        expected_response = json.dumps(
            {"kafka": True, "storage": False, "scicat": True}
        ).encode("utf-8")
        cast(MagicMock, handler.wfile.write).assert_called_with(expected_response)

    def test_do_GET_not_found(self):
        handler = self._create_handler()
        handler.path = "/other"

        handler.send_response = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_GET()

        handler.send_response.assert_called_with(404)
        handler.end_headers.assert_called_once()

    def test_check_kafka_success(self):
        handler = self._create_handler()
        self.mock_consumer.list_topics.return_value = {}

        result = handler._check_kafka()

        assert result
        self.mock_consumer.list_topics.assert_called_with(timeout=5)

    def test_check_kafka_failure(self):
        handler = self._create_handler()
        self.mock_consumer.list_topics.side_effect = Exception("Kafka down")

        result = handler._check_kafka()

        assert not result
        self.mock_logger.error.assert_called()

    def test_check_storage_success(self):
        handler = self._create_handler()
        self.mock_config.ingestion.file_handling.data_directory = "/data"

        with patch("scicat_health_check.pathlib.Path") as mock_path:
            mock_path_instance = mock_path.return_value
            mock_path_instance.exists.return_value = True
            mock_path_instance.iterdir.return_value = iter(["file"])

            result = handler._check_storage()

            assert result

    def test_check_storage_no_directory_configured(self):
        handler = self._create_handler()
        self.mock_config.ingestion.file_handling.data_directory = ""

        result = handler._check_storage()

        assert not result
        self.mock_logger.warning.assert_called_with(
            "Health check: No data_directory configured."
        )

    def test_check_storage_path_does_not_exist(self):
        handler = self._create_handler()
        self.mock_config.ingestion.file_handling.data_directory = "/data"

        with patch("scicat_health_check.pathlib.Path") as mock_path:
            mock_path_instance = mock_path.return_value
            mock_path_instance.exists.return_value = False

            result = handler._check_storage()

            assert not result
            self.mock_logger.error.assert_called()

    def test_check_storage_access_failed(self):
        handler = self._create_handler()
        self.mock_config.ingestion.file_handling.data_directory = "/data"

        with patch("scicat_health_check.pathlib.Path") as mock_path:
            mock_path_instance = mock_path.return_value
            mock_path_instance.exists.return_value = True
            mock_path_instance.iterdir.side_effect = PermissionError("Access denied")

            result = handler._check_storage()

            assert not result
            self.mock_logger.error.assert_called()

    @patch("scicat_health_check.requests.get")
    def test_check_scicat_success(self, mock_get):
        handler = self._create_handler()
        self.mock_config.scicat.health_url = "http://scicat/health"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        result = handler._check_scicat()

        assert result
        mock_get.assert_called_with("http://scicat/health", timeout=5)

    @patch("scicat_health_check.requests.get")
    def test_check_scicat_failure_status_code(self, mock_get):
        handler = self._create_handler()
        self.mock_config.scicat.health_url = "http://scicat/health"

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = handler._check_scicat()

        assert not result

    @patch("scicat_health_check.requests.get")
    def test_check_scicat_exception(self, mock_get):
        handler = self._create_handler()
        self.mock_config.scicat.health_url = "http://scicat/health"

        mock_get.side_effect = Exception("Connection error")

        result = handler._check_scicat()

        assert not result
        self.mock_logger.error.assert_called()


class TestHealthCheck(unittest.TestCase):
    @patch("scicat_health_check.HTTPServer")
    @patch("scicat_health_check.threading.Thread")
    def test_start_health_server(self, mock_thread_cls, mock_http_server_cls):
        # Arrange
        mock_config = MagicMock()
        mock_config.health_check.host = "127.0.0.1"
        mock_config.health_check.port = 8080
        mock_consumer = MagicMock()
        mock_logger = MagicMock()

        mock_server_instance = mock_http_server_cls.return_value
        mock_thread_instance = mock_thread_cls.return_value

        # Act
        start_health_server(mock_config, mock_consumer, mock_logger)

        # Assert
        # Check HTTPServer initialization
        mock_http_server_cls.assert_called_once()
        args, _ = mock_http_server_cls.call_args
        server_address = args[0]
        assert server_address == ("127.0.0.1", 8080)

        # Check Thread initialization
        mock_thread_cls.assert_called_once_with(
            target=_serve_health_server, args=(mock_server_instance, mock_logger)
        )

        # Check thread configuration and start
        assert mock_thread_instance.daemon
        mock_thread_instance.start.assert_called_once()

        # Check logging
        mock_logger.info.assert_called_with(
            "Health check server started on %s:%s", "127.0.0.1", 8080
        )


def test_serve_health_server_restarts_on_error(monkeypatch):
    mock_server = MagicMock()
    mock_logger = MagicMock()

    # Simulate a crash followed by stop signal via raising from sleep
    mock_server.serve_forever.side_effect = [RuntimeError("boom"), None]

    def fake_sleep(_):
        raise StopIteration

    monkeypatch.setattr("scicat_health_check.sleep", fake_sleep)

    with pytest.raises(StopIteration):
        _serve_health_server(mock_server, mock_logger, restart_delay=0)

    mock_logger.error.assert_called_once()
    message, exception_obj, delay = mock_logger.error.call_args[0]
    assert (
        message
        == "Health check server stopped unexpectedly: %s. Restarting in %s seconds."
    )
    assert isinstance(exception_obj, RuntimeError)
    assert delay == 0
