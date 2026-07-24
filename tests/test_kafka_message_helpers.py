# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 ScicatProject contributors (https://github.com/ScicatProject)
from unittest.mock import Mock

import pytest
from confluent_kafka import Consumer, Message

from scicat_kafka import wrdn_messages


@pytest.fixture
def mock_consumer() -> Mock:
    return Mock(spec=Consumer)


def test_wrdn_message_generator_msg_timeout(mock_consumer, fake_logger) -> None:
    mock_consumer.poll.return_value = None

    generator = wrdn_messages(consumer=mock_consumer, logger=fake_logger)
    msg = next(generator)
    mock_consumer.poll.assert_called_once()
    assert msg is None
    assert len(fake_logger._info_list) == 1
    assert fake_logger._info_list[0].msg.startswith("Received no messages, %d [s].")
    assert fake_logger._info_list[0].args[0] == 1

    # Test if the accumulated time out is logged as expected.
    second_msg = next(generator)
    assert second_msg is None
    assert len(fake_logger._info_list) == 2
    assert fake_logger._info_list[1].msg.startswith("Received no messages, %d [s].")
    assert fake_logger._info_list[1].args[0] == 2


def test_wrdn_message_generator_msg_has_none_value(mock_consumer, fake_logger) -> None:
    mock_msg = Mock(spec=Message)
    mock_msg.error.return_value = False
    mock_msg.value.return_value = None
    mock_consumer.poll.return_value = mock_msg

    msg = next(wrdn_messages(consumer=mock_consumer, logger=fake_logger))
    mock_consumer.poll.assert_called_once()
    assert msg is None
    assert len(fake_logger._warning_list) == 1
    assert fake_logger._warning_list[0].msg.startswith("None value received")
