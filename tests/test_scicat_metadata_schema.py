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


@pytest.fixture()
def base_metadata_schema_file() -> Path:
    return Path("resources/base.imsc.json.example")


@pytest.fixture()
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
        assert schema_name == schema.name

    assert isinstance(schemas, OrderedDict)
    # Check if the schema is ordered by the schema name
    assert list(schemas.keys()) == sorted(schemas.keys())


def test_metadata_schema_selection() -> None:
    schemas = OrderedDict(
        {
            "schema1": MetadataSchema(
                id="schema1",
                name="Schema 1",
                instrument="",
                selector="filename:starts_with:wrong_name",
                variables={},
                schema={},
            ),
            "schema2": MetadataSchema(
                id="schema2",
                name="Schema 2",
                instrument="",
                selector="filename:starts_with:right_name",
                variables={},
                schema={},
            ),
            "schema3": MetadataSchema(
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
