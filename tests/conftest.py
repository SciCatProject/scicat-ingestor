import logging
import pathlib
from collections.abc import Generator
from dataclasses import dataclass

import h5py
import pytest

from scicat_configuration import OfflineIngestorConfig
from scicat_metadata import MetadataSchema


@pytest.fixture(scope="module")
def example_nexus_file_for_schema_test(
    tmp_path_factory: pytest.TempdirFactory,
) -> pathlib.Path:
    tmp_path = pathlib.Path(
        tmp_path_factory.mktemp("example_nexus_file_for_schema_tests")
    )
    example_file = tmp_path / "nexus_for_testing.h5"
    with h5py.File(example_file, "w") as f:
        entry_gr = f.create_group("/entry")
        entry_gr.create_dataset("entry_identifier_uuid", data=["supposedly-long-uuid"])
        entry_gr.create_dataset("experiment_identifier", data=["123456"])
        instrument_gr = entry_gr.create_group("instrument")
        instrument_gr.create_dataset("name", data=["Test Instrument"])
        detectors = instrument_gr.create_group("detectors")
        det_1 = detectors.create_group('detector_1')
        det_2 = detectors.create_group('detector_2')
        det_3 = detectors.create_group('zdet_3')  # Purposely not matching pattern
        for i, det in enumerate((det_1, det_2, det_3)):
            det.create_dataset("name", data=[f"Detector Name {i + 1}"])

        sample_gr = entry_gr.create_group("sample")
        temperature = sample_gr.create_dataset("temperature", data=[300.0])
        temperature.attrs["units"] = "K"

    return example_file


@pytest.fixture(scope="module")
def nexus_file(
    example_nexus_file_for_schema_test: pathlib.Path,
) -> Generator[h5py.File, None, None]:
    with h5py.File(example_nexus_file_for_schema_test, "r") as f:
        yield f


@pytest.fixture(scope="module")
def offline_config(
    example_nexus_file_for_schema_test: pathlib.Path,
) -> OfflineIngestorConfig:
    config = OfflineIngestorConfig(
        nexus_file=example_nexus_file_for_schema_test.as_posix(),
        done_writing_message_file="",
        config_file="",
        id="",
    )
    config.ingestion.file_handling.ingestor_files_directory = (
        example_nexus_file_for_schema_test.parent.as_posix()
    )
    return config


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
