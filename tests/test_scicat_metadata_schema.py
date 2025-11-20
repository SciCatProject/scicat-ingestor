# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
from collections import OrderedDict
from collections.abc import Generator
from pathlib import Path

import h5py
import pytest
import yaml

from scicat_configuration import OfflineIngestorConfig
from scicat_metadata import (
    MetadataSchema,
    MetadataVariableValueSpec,
    build_metadata_variables,
    collect_schemas,
    list_schema_file_names,
    select_applicable_schema,
)

ALL_SCHEMA_EXAMPLES = list_schema_file_names(
    Path(__file__).parent.parent / Path("resources")
)


@pytest.fixture
def base_metadata_schema_file() -> Path:
    return Path("resources/base.imsc.yml.example")


@pytest.fixture
def base_metadata_schema_dict(base_metadata_schema_file: Path) -> dict:
    loaded_schema = yaml.safe_load(base_metadata_schema_file.read_text())
    if not isinstance(loaded_schema, dict):
        raise ValueError(
            f"Invalid schema file {base_metadata_schema_file}. Schema must be a dictionary."
        )
    return loaded_schema


def test_build_metadata_variables(base_metadata_schema_dict: dict):
    build_metadata_variables(base_metadata_schema_dict["variables"])


def test_build_metadata_variables_invalid_source_name(base_metadata_schema_dict: dict):
    base_metadata_schema_dict["variables"]["pid"]["source"] = "invalid_source_name"
    with pytest.raises(ValueError, match="Invalid source name"):
        build_metadata_variables(base_metadata_schema_dict["variables"])


@pytest.mark.parametrize("schema_file", ALL_SCHEMA_EXAMPLES)
def test_build_metadata_schema(schema_file: Path) -> None:
    MetadataSchema.from_file(schema_file)


def test_collect_metadata_schema() -> None:
    schemas = collect_schemas(Path(__file__).parent.parent / Path("resources"))
    assert len(schemas) == len(ALL_SCHEMA_EXAMPLES)
    for schema_name, schema in schemas.items():
        assert isinstance(schema, MetadataSchema)
        assert schema_name == schema.id

    assert isinstance(schemas, OrderedDict)
    # Check if the schema is ordered by the schema order and name.
    # The expected keys are hardcoded on purpose.
    # Always hardcode the expected keys to avoid the test being too flexible.
    assert (
        list(schemas.keys())
        == [
            "715ce7ba-3f91-11ef-932f-37a5c6fd60b1",  # Coda, 1, Coda Metadata Schema
            "72a991ee-437a-11ef-8fd2-1f95660accb7",  # Dream, 1, dream Metadata Schema
            "c5bed39a-4379-11ef-ba5a-ffbc783163b6",  # Base, 1, Generic metadata schema
            "891322f6-437a-11ef-980a-7bdc756bd0b3",  # Loki, 1, Loki Metadata Schema
            "bdb3dc66-9c2c-11ef-9ffb-532fdcfe4a31",  # Small-Ymir, 100, Ymir Metadata Schema
            "628b28d6-9c26-11ef-948d-0b2d405fc82f",  # Small-Coda, 110, Small-Coda Metadata Schema
        ]
    )


def test_metadata_schema_selection() -> None:
    schemas = OrderedDict(
        {
            "schema1": MetadataSchema(
                order=1,
                id="schema1",
                name="Schema 1",
                instrument="",
                selector="filename:starts_with:wrong_name",
                variables={},
                schema={},
            ),
            "schema2": MetadataSchema(
                order=2,
                id="schema2",
                name="Schema 2",
                instrument="",
                selector="filename:starts_with:right_name",
                variables={},
                schema={},
            ),
            "schema3": MetadataSchema(
                order=3,
                id="schema3",
                name="Schema 3",
                instrument="",
                selector="filename:starts_with:wrong_name2",
                variables={},
                schema={},
            ),
        }
    )
    assert (
        select_applicable_schema(Path("right_name.nxs"), schemas) == schemas["schema2"]
    )


def test_metadata_schema_selection_contains() -> None:
    schemas = OrderedDict(
        {
            "schema1": MetadataSchema(
                order=1,
                id="schema1",
                name="Schema 1",
                instrument="",
                selector="filename:contains:wrong_part",
                variables={},
                schema={},
            ),
            "schema2": MetadataSchema(
                order=2,
                id="schema2",
                name="Schema 2",
                instrument="",
                selector="filename:contains:right_part",
                variables={},
                schema={},
            ),
        }
    )
    assert (
        select_applicable_schema(Path("some_right_part_in_name.nxs"), schemas)
        == schemas["schema2"]
    )


def test_metadata_schema_selection_contains_no_match() -> None:
    schemas = OrderedDict(
        {
            "schema1": MetadataSchema(
                order=1,
                id="schema1",
                name="Schema 1",
                instrument="",
                selector="filename:contains:missing_part",
                variables={},
                schema={},
            ),
        }
    )
    with pytest.raises(
        Exception, match="No applicable metadata schema configuration found!!"
    ):
        select_applicable_schema(Path("some_file.nxs"), schemas)


def test_metadata_schema_selection_wrong_selector_target_name_raises() -> None:
    with pytest.raises(ValueError, match="Invalid target name"):
        select_applicable_schema(
            Path("right_name.nxs"),
            OrderedDict(
                {
                    "schema1": MetadataSchema(
                        order=1,
                        id="schema1",
                        name="Schema 1",
                        instrument="",
                        selector="data_file:starts_with:wrong_name",
                        variables={},
                        schema={},
                    )
                }
            ),
        )


def test_metadata_schema_selection_wrong_selector_function_name_raises() -> None:
    with pytest.raises(ValueError, match="Invalid function name"):
        select_applicable_schema(
            Path("right_name.nxs"),
            OrderedDict(
                {
                    "schema1": MetadataSchema(
                        order=1,
                        id="schema1",
                        name="Schema 1",
                        instrument="",
                        selector="filename:start_with:wrong_name",
                        variables={},
                        schema={},
                    )
                }
            ),
        )


@pytest.fixture(scope="module")
def example_nexus_file_for_schema_test(tmp_path_factory: pytest.TempdirFactory) -> Path:
    tmp_path = Path(tmp_path_factory.mktemp("example_nexus_file_for_schema_tests"))
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
    example_nexus_file_for_schema_test: Path,
) -> Generator[h5py.File, None, None]:
    with h5py.File(example_nexus_file_for_schema_test, "r") as f:
        yield f


@pytest.fixture(scope="module")
def example_schema() -> MetadataSchema:
    import logging
    from typing import cast

    import yaml

    from scicat_metadata import _validate_file

    # Turn this yaml string into a stream
    _example_schema = Path(__file__).parent / "resources/example_schema.imsc.yml"
    # Check if the example schema is valid first
    if not _validate_file(_example_schema, logger=logging.getLogger(__name__)):
        raise ValueError(
            "Invalid example schema. "
            "Use ``scicat_validate_metadata_schema`` to validate it first."
        )

    return MetadataSchema.from_dict(
        cast(dict, yaml.safe_load(_example_schema.read_text()))
    )


@pytest.fixture(scope="module")
def offline_config(example_nexus_file_for_schema_test: Path) -> OfflineIngestorConfig:
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


def test_metadata_variable_default_variables(
    nexus_file: h5py.File,
    offline_config: OfflineIngestorConfig,
) -> None:
    import datetime
    import uuid

    from scicat_dataset import extract_variables_values

    example_id = uuid.uuid4().hex
    variable_values = extract_variables_values(
        variables={},  # Empty variable configuration maps
        h5file=nexus_file,
        schema_id=example_id,
        config=offline_config,
    )
    nexus_file_path = Path(offline_config.nexus_file)
    assert isinstance(variable_values['ingestor_run_id'].value, str)
    assert variable_values['data_file_path'].value == nexus_file_path.as_posix()
    assert variable_values['data_file_name'].value == nexus_file_path.name
    # Just check if it can be parsed as a datetime
    datetime.datetime.fromisoformat(variable_values['now'].value)
    assert (
        variable_values['ingestor_files_directory'].value
        == nexus_file_path.parent.as_posix()
    )
    assert variable_values['ingestor_metadata_schema_id'].value == example_id
    assert all(
        isinstance(value, MetadataVariableValueSpec)
        for value in variable_values.values()
    )


def test_metadata_variable_nexus(
    nexus_file: h5py.File,
    example_schema: MetadataSchema,
    offline_config: OfflineIngestorConfig,
) -> None:
    from scicat_dataset import extract_variables_values

    variable_values = extract_variables_values(
        variables=example_schema.variables,
        h5file=nexus_file,
        schema_id=example_schema.id,
        config=offline_config,
    )
    assert variable_values['pid'] == MetadataVariableValueSpec(
        value='supposedly-long-uuid'
    )
    assert variable_values['proposal_id'] == MetadataVariableValueSpec(value="123456")
    assert variable_values['instrument_name'] == MetadataVariableValueSpec(
        value='Test Instrument'
    )
    assert variable_values['detector_names_list'] == MetadataVariableValueSpec(
        value=["Detector Name 1", "Detector Name 2"]
    )
    assert variable_values['detector_names_all'] == MetadataVariableValueSpec(
        value=["Detector Name 1", "Detector Name 2", "Detector Name 3"]
    )
    assert variable_values['sample_temperature'] == MetadataVariableValueSpec(
        value=300.0, unit="K"
    )


def test_metadata_variable_raw_values(
    nexus_file: h5py.File,
    example_schema: MetadataSchema,
    offline_config: OfflineIngestorConfig,
) -> None:
    from scicat_dataset import extract_variables_values

    variable_values = extract_variables_values(
        variables=example_schema.variables,
        h5file=nexus_file,
        schema_id=example_schema.id,
        config=offline_config,
    )
    assert variable_values['detector_names'] == MetadataVariableValueSpec(
        value="Detector Name 1, Detector Name 2"
    )
    assert variable_values['access_groups'] == MetadataVariableValueSpec(
        value=["dmsc-staff", "ess_proposal_123456"]
    )


def test_metadata_schema_items(
    nexus_file: h5py.File,
    example_schema: MetadataSchema,
    offline_config: OfflineIngestorConfig,
) -> None:
    """This test is techniqually about creating ScicatDataset instance
    but currently we do not build schema items separately before
    creating a dataset.

    Therefore we test if the schema items are rendered correctly by
    building ``ScicatDataset`` instance.
    """
    import logging

    from scicat_dataset import create_scicat_dataset_instance, extract_variables_values

    variable_values = extract_variables_values(
        variables=example_schema.variables,
        h5file=nexus_file,
        schema_id=example_schema.id,
        config=offline_config,
    )
    dataset = create_scicat_dataset_instance(
        metadata_schema=example_schema.schema,
        variable_map=variable_values,
        data_file_list=[],
        config=offline_config.dataset,
        logger=logging.getLogger(__name__),
    )
    assert dataset.pid == 'supposedly-long-uuid'
    assert dataset.scientificMetadata['sample_temperature']['value'] == '300.0'
    assert dataset.scientificMetadata['sample_temperature']['unit'] == 'K'
