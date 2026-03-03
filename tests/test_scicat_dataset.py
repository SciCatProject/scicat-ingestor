# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import re

import pytest

from scicat_configuration import DatasetOptions
from scicat_dataset import convert_to_type, create_scicat_dataset_instance
from scicat_metadata import MetadataSchema, MetadataVariableValueSpec


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


def test_create_scicat_dataset_instance(
    example_schema: MetadataSchema, fake_logger
) -> None:
    variable_map: dict[str, MetadataVariableValueSpec] = {
        'pid': MetadataVariableValueSpec(value='some-random-pid'),
        'proposal_id': MetadataVariableValueSpec(value='proposal-id'),
        'detector_names': MetadataVariableValueSpec(value='ingetsor'),
        'sample_temperature': MetadataVariableValueSpec(value=300.0, unit='K'),
    }
    scicat_dataset = create_scicat_dataset_instance(
        metadata_schema=example_schema.schema,
        variable_map=variable_map,
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
