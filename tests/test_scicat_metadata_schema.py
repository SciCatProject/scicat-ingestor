# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
from collections import OrderedDict
from pathlib import Path

import h5py
import pytest
import yaml

from fallback_metadata_schema import get_fallback_schema
from scicat_configuration import OfflineIngestorConfig
from scicat_devtools import validate_schema
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
def ess_fallback_schema() -> MetadataSchema:
    ess_fallback_schema = get_fallback_schema('ess-fallback')
    assert ess_fallback_schema is not None
    return ess_fallback_schema


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


def test_metadata_schema_selection(
    fake_logger, ess_fallback_schema: MetadataSchema
) -> None:
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
        select_applicable_schema(
            Path("right_name.nxs"),
            schemas,
            logger=fake_logger,
            fall_back_schema=ess_fallback_schema,
        )
        == schemas["schema2"]
    )


def test_metadata_schema_selection_contains(
    fake_logger, ess_fallback_schema: MetadataSchema
) -> None:
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
        select_applicable_schema(
            Path("some_right_part_in_name.nxs"),
            schemas,
            logger=fake_logger,
            fall_back_schema=ess_fallback_schema,
        )
        == schemas["schema2"]
    )


def test_metadata_schema_selection_contains_no_match_log_error(
    fake_logger, ess_fallback_schema
) -> None:
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
    select_applicable_schema(
        Path("some_file.nxs"),
        schemas,
        logger=fake_logger,
        fall_back_schema=ess_fallback_schema,
    )
    err_msg_match = (
        "No applicable metadata schema found based on the selectors. "
        "Fallback schema will be used..."
    )
    assert fake_logger._warning_list[-1].msg == err_msg_match


def test_metadata_schema_selection_contains_no_match_fallback(
    fake_logger, ess_fallback_schema
) -> None:
    selected = select_applicable_schema(
        Path("some_file.nxs"),
        {},
        logger=fake_logger,
        fall_back_schema=ess_fallback_schema,
    )
    assert selected == ess_fallback_schema


def test_metadata_schema_selection_wrong_selector_target_log_error(
    fake_logger, ess_fallback_schema
) -> None:
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
        logger=fake_logger,
        fall_back_schema=ess_fallback_schema,
    )

    err_msg_match = "Invalid target name"
    # The last message is expected to be
    # `No applicable metadata schema found...` error message.
    # The wrong target name error message should be the second last one.
    # It is not to enforce the order of logging so feel free to change
    # how it is tested if it becomes too brittle.
    assert fake_logger._warning_list[-2].msg.startswith(err_msg_match)


def test_metadata_schema_selection_wrong_selector_function_log_error(
    fake_logger, ess_fallback_schema
) -> None:
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
        fall_back_schema=ess_fallback_schema,
        logger=fake_logger,
    )
    err_msg_match = "Invalid function name"
    # The last message is expected to be
    # `No applicable metadata schema found...` error message.
    # The wrong target name error message should be the second last one.
    # It is not to enforce the order of logging so feel free to change
    # how it is tested if it becomes too brittle.
    assert fake_logger._warning_list[-2].msg.startswith(err_msg_match)


def test_metadata_variable_default_variables(
    nexus_file: h5py.File, offline_config: OfflineIngestorConfig, fake_logger
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
        logger=fake_logger,
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
    fake_logger,
) -> None:
    from scicat_dataset import extract_variables_values

    variable_values = extract_variables_values(
        variables=example_schema.variables,
        h5file=nexus_file,
        schema_id=example_schema.id,
        config=offline_config,
        logger=fake_logger,
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
    fake_logger,
) -> None:
    from scicat_dataset import extract_variables_values

    variable_values = extract_variables_values(
        variables=example_schema.variables,
        h5file=nexus_file,
        schema_id=example_schema.id,
        config=offline_config,
        logger=fake_logger,
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
    fake_logger,
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
        logger=fake_logger,
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


def test_metadata_schema_validator_invalid_field_type(
    example_schema: MetadataSchema, tmp_path: Path, fake_logger
) -> None:
    from copy import deepcopy

    invalid_schema_path = tmp_path / "invalid_field_type.imsc.yml"

    invalid_schema = deepcopy(example_schema)
    first_field_key = next(iter(invalid_schema.schema.keys()))
    invalid_schema.schema[
        first_field_key
    ].field_type = 'high-five-level'  # Random invalid type
    invalid_schema.save_file(invalid_schema_path)

    with pytest.raises(ValueError, match='One or more schema files are invalid'):
        validate_schema(schema_path=invalid_schema_path, logger=fake_logger)
