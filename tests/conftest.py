import logging
import pathlib
from dataclasses import dataclass

import pytest

from scicat_metadata import MetadataSchema


@pytest.fixture(scope="module")
def example_schema() -> MetadataSchema:
    from typing import cast

    import yaml

    from scicat_metadata import _validate_file

    # Turn this yaml string into a stream
    _example_schema = (
        pathlib.Path(__file__).parent / "resources/example_schema.imsc.yml"
    )
    # Check if the example schema is valid first
    if not _validate_file(_example_schema, logger=logging.getLogger(__name__)):
        raise ValueError(
            "Invalid example schema. "
            "Use ``scicat_validate_metadata_schema`` to validate it first."
        )

    return MetadataSchema.from_dict(
        cast(dict, yaml.safe_load(_example_schema.read_text()))
    )


class FakeLogger:
    @dataclass
    class MsgArgs:
        msg: str
        args: tuple

    def __init__(self):
        self._info_list = []
        self._warning_list = []
        self._error_list = []
        self.verbose = False

    def _log(self, container: list, msg, args: tuple) -> None:
        new_msg = self.MsgArgs(msg, args)
        container.append(new_msg)
        if self.verbose:
            print(new_msg)  # noqa: T201

    def info(self, msg, *args) -> None:
        self._log(self._info_list, msg, args)

    def warning(self, msg, *args) -> None:
        self._log(self._warning_list, msg, args)

    def error(self, msg, *args) -> None:
        self._log(self._error_list, msg, args)


@pytest.fixture
def fake_logger() -> FakeLogger:
    return FakeLogger()
