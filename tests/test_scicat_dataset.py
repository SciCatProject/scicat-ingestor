# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import re

import h5py
import pytest

from scicat_configuration import DatasetOptions, OfflineIngestorConfig
from scicat_dataset import (
    convert_to_type,
    create_scicat_dataset_instance,
    extract_variables_values,
)
from scicat_metadata import (
    MetadataItemConfig,
    MetadataSchema,
    MetadataVariableValueSpec,
)


def test_dtype_string_converter() -> None:
    assert convert_to_type("test", "string") == "test"
    assert convert_to_type(123, "string") == "123"
    assert convert_to_type(123.456, "string") == "123.456"


def test_dtype_string_array_converter() -> None:
    assert convert_to_type("'test'", "string[]") == ["t", "e", "s", "t"]
    assert convert_to_type("['test']", "string[]") == ["test"]
    assert convert_to_type("['test', 'test2']", "string[]") == ["test", "test2"]
    assert convert_to_type([1, 2, 3], "string[]") == ["1", "2", "3"]
    assert convert_to_type([1.1, 2.2, 3.3], "string[]") == ["1.1", "2.2", "3.3"]


def test_dtype_integer_array_converter() -> None:
    type_name = "integer[]"
    assert convert_to_type("['1']", type_name) == [1]
    assert convert_to_type("['1', '2']", type_name) == [1, 2]
    assert convert_to_type([1, 2, 3], type_name) == [1, 2, 3]
    assert convert_to_type([1.1, 2.2, 3.3], type_name) == [1, 2, 3]

    with pytest.raises(ValueError, match="invalid literal for int"):
        convert_to_type("['1.2', '2.5']", type_name)


def test_dtype_float_array_converter() -> None:
    type_name = "float[]"
    assert convert_to_type("['1']", type_name) == [1]
    assert convert_to_type("['1', '2']", type_name) == [1, 2]
    assert convert_to_type("['1.2', '2.5']", type_name) == [1.2, 2.5]
    assert convert_to_type([1, 2, 3], type_name) == [1, 2, 3]
    assert convert_to_type([1.1, 2.2, 3.3], type_name) == [1.1, 2.2, 3.3]


def test_dtype_integer_converter() -> None:
    assert convert_to_type("123", "integer") == 123
    assert convert_to_type(123, "integer") == 123
    assert convert_to_type(123.456, "integer") == 123


def test_dtype_float_converter() -> None:
    assert convert_to_type("123.456", "float") == 123.456
    assert convert_to_type(123, "float") == 123.0
    assert convert_to_type(123.456, "float") == 123.456


def test_dtype_date_converter() -> None:
    import datetime

    test_datetime_isoformat = "1994-06-28T10:20:30+00:00"
    test_datetime = datetime.datetime.fromisoformat(test_datetime_isoformat)
    assert convert_to_type("1994-06-28T10:20:30Z", "date") == test_datetime_isoformat
    assert convert_to_type(test_datetime.timestamp(), "date") == test_datetime_isoformat
    assert convert_to_type(object(), "date") is None


def test_dtype_converter_invalid_dtype_raises() -> None:
    with pytest.raises(ValueError, match=re.escape("Invalid dtype description.")):
        convert_to_type("test", "invalid_type")


def test_create_scicat_extract_variables_values(
    nexus_file: h5py.File,
    example_schema: MetadataSchema,
    fake_logger,
    offline_config: OfflineIngestorConfig,
) -> None:
    variable_map = extract_variables_values(
        variables=example_schema.variables,
        h5file=nexus_file,
        config=offline_config,
        schema_id=example_schema.id,
        logger=fake_logger,
    )
    variable_recipes = example_schema.variables
    # Should not fail at all
    assert not fake_logger._warning_list
    for nexus_str_variable in ('pid', 'proposal_id', 'instrument_name'):
        assert (
            variable_map[nexus_str_variable].value
            == nexus_file[variable_recipes[nexus_str_variable].path][()][0].decode()
        )
    assert variable_map['sample_temperature'].value == 300.0
    assert variable_map['sample_temperature'].unit == 'K'
    assert variable_map['detector_names_all'].value == [
        f"Detector Name {i + 1}" for i in range(3)
    ]
    assert variable_map['detector_names_list'].value == [
        f"Detector Name {i + 1}" for i in range(2)
    ]
    Spec = MetadataVariableValueSpec
    assert variable_map['detector_1_number'] == Spec(value=10.5, unit='m')
    assert variable_map['detector_2_number'] == Spec(value=12.5, unit='m')
    # combined list of multiple values should have same unit if possible.
    assert variable_map['detector_12_numbers'] == Spec(value=[10.5, 12.5], unit='m')
    # combined list of multipel values should not have any units
    # if the referred values have different units
    assert variable_map['nonsense_numbers'] == Spec(value=[10.5, 300.0])

    assert variable_map['detector_12_numbers_sum'] == Spec(value=23.0, unit='m')


def test_create_scicat_extract_variables_values_failure_okay(
    nexus_file: h5py.File,
    example_schema: MetadataSchema,
    fake_logger,
    offline_config: OfflineIngestorConfig,
) -> None:
    from copy import deepcopy

    invalid_schema = deepcopy(example_schema)
    invalid_schema.variables['pid'].path = '/obviously/wrong/path'

    extract_variables_values(
        variables=invalid_schema.variables,
        h5file=nexus_file,
        config=offline_config,
        schema_id=invalid_schema.id,
        logger=fake_logger,
    )
    # Failures should be ignored
    assert fake_logger._warning_list


@pytest.fixture
def example_variable_map() -> dict[str, MetadataVariableValueSpec]:
    """Matching variable map with the `example_schema`."""
    return {
        'pid': MetadataVariableValueSpec(value='some-random-pid'),
        'proposal_id': MetadataVariableValueSpec(value='proposal-id'),
        'detector_names': MetadataVariableValueSpec(value='ingetsor'),
        'sample_temperature': MetadataVariableValueSpec(value=300.0, unit='K'),
    }


def test_create_scicat_dataset_instance(
    example_schema: MetadataSchema,
    example_variable_map: dict[str, MetadataVariableValueSpec],
    fake_logger,
) -> None:
    scicat_dataset = create_scicat_dataset_instance(
        metadata_schema=example_schema.schema,
        variable_map=example_variable_map,
        data_file_list=[],
        config=DatasetOptions(),
        logger=fake_logger,
    )
    # Should not fail anything
    assert not fake_logger._warning_list
    # Check some of the fields
    assert scicat_dataset.pid == 'some-random-pid'
    assert scicat_dataset.ownerEmail == ''  # Empty in the schema file
    assert scicat_dataset.scientificMetadata['sample_temperature'] == {
        'human_name': 'Sample Temperature',
        'value': '300.0',  # Should be converted to string
        'unit': 'K',
        'type': 'string',
    }


def test_create_scicat_dataset_instance_with_sample_pid_list(
    example_schema: MetadataSchema,
    example_variable_map: dict[str, MetadataVariableValueSpec],
    fake_logger,
) -> None:
    scicat_dataset = create_scicat_dataset_instance(
        metadata_schema=example_schema.schema,
        variable_map=example_variable_map,
        data_file_list=[],
        config=DatasetOptions(),
        sample_dataset_pid_list=['sample-pid-1', 'sample-pid-2'],
        logger=fake_logger,
    )
    # Should not fail anything
    assert not fake_logger._warning_list
    # Check some of the fields
    assert isinstance(scicat_dataset.sampleId, list)
    assert sorted(scicat_dataset.sampleId) == ['sample-pid-1', 'sample-pid-2']


def test_create_scicat_dataset_instance_with_sample_pid_list_merged(
    example_schema: MetadataSchema,
    example_variable_map: dict[str, MetadataVariableValueSpec],
    fake_logger,
) -> None:
    variable_map = {
        **example_variable_map,
        'sample_pid': MetadataVariableValueSpec(value='hardcoded-sample-pid'),
    }
    example_schema.schema['sampleId'] = MetadataItemConfig(
        machine_name='sampleId',
        field_type='high_level',
        value='<sample_pid>',
        type='string',
    )
    scicat_dataset = create_scicat_dataset_instance(
        metadata_schema=example_schema.schema,
        variable_map=variable_map,
        data_file_list=[],
        config=DatasetOptions(),
        sample_dataset_pid_list=['sample-pid-1', 'sample-pid-2'],
        logger=fake_logger,
    )
    # Should not fail anything
    assert not fake_logger._warning_list
    # Check some of the fields
    assert isinstance(scicat_dataset.sampleId, list)
    assert sorted(scicat_dataset.sampleId) == [
        'hardcoded-sample-pid',
        'sample-pid-1',
        'sample-pid-2',
    ]


def test_create_scicat_dataset_instance_with_sample_pid_list_merge_unique(
    example_schema: MetadataSchema,
    example_variable_map: dict[str, MetadataVariableValueSpec],
    fake_logger,
) -> None:
    variable_map = {
        **example_variable_map,
        'sample_pid': MetadataVariableValueSpec(
            value=['hardcoded-sample-pid', 'sample-pid-1']
        ),
    }
    example_schema.schema['sampleId'] = MetadataItemConfig(
        machine_name='sampleId',
        field_type='high_level',
        value='<sample_pid>',
        type='list',
    )
    scicat_dataset = create_scicat_dataset_instance(
        metadata_schema=example_schema.schema,
        variable_map=variable_map,
        data_file_list=[],
        config=DatasetOptions(),
        sample_dataset_pid_list=['sample-pid-1', 'sample-pid-2'],
        logger=fake_logger,
    )
    # Should not fail anything
    assert not fake_logger._warning_list
    # Check some of the fields
    assert isinstance(scicat_dataset.sampleId, list)
    assert sorted(scicat_dataset.sampleId) == [
        'hardcoded-sample-pid',
        'sample-pid-1',
        'sample-pid-2',
    ]


def test_create_scicat_dataset_instance_failure_okay(
    example_schema: MetadataSchema, fake_logger
) -> None:
    variable_map: dict[str, MetadataVariableValueSpec] = {
        'pid': MetadataVariableValueSpec(value='pid')
    }
    create_scicat_dataset_instance(
        metadata_schema=example_schema.schema,
        variable_map=variable_map,
        data_file_list=[],
        config=DatasetOptions(),
        logger=fake_logger,
    )
    assert fake_logger._warning_list


def test_job_pyload(example_schema: MetadataSchema):
    variable_map: dict[str, MetadataVariableValueSpec] = {
        'pid': MetadataVariableValueSpec(value='some-long-pid-value'),
        'owner': MetadataVariableValueSpec(value='some-unique-owner-name'),
    }
    example_schema.jobs["embargo_period"].owner_user = "<owner>"
    payload = example_schema.jobs["embargo_period"].payload(
        variable_registry=variable_map
    )
    assert payload == {
        "type": "embargo_period",
        "ownerUser": "some-unique-owner-name",
        "ownerGroup": "pit_management",
        "contactEmail": "pit[@]mail.eu",
        "jobParams": {"dataset": "some-long-pid-value"},
    }
