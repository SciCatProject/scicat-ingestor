# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import json
from collections import OrderedDict
from pathlib import Path

import pytest

from scicat_metadata import (
    MetadataSchema,
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
    return Path("resources/base.imsc.json.example")


@pytest.fixture
def base_metadata_schema_dict(base_metadata_schema_file: Path) -> dict:
    return json.loads(base_metadata_schema_file.read_text())


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
