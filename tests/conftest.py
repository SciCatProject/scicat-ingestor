from dataclasses import dataclass

import pytest


class FakeLogger:
    @dataclass
    class MsgArgs:
        msg: str
        args: tuple

    def __init__(self):
        self._info_list = []
        self._warning_list = []
        self._error_list = []

    def _log(self, container: list, msg, args: tuple) -> None:
        container.append(self.MsgArgs(msg, args))

    def info(self, msg, *args) -> None:
        self._log(self._info_list, msg, args)

    def warning(self, msg, *args) -> None:
        self._log(self._warning_list, msg, args)

    def error(self, msg, *args) -> None:
        self._log(self._error_list, msg, args)


@pytest.fixture
def fake_logger() -> FakeLogger:
    return FakeLogger()
