# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import json
import sys
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


def test_metadata_schema_multiple_matches_merge_by_order() -> None:
    """Test that multiple matching schemas are merged based on order priority."""
    schemas = OrderedDict(
        {
            "schema1": MetadataSchema(
                order=2,
                id="schema1",
                name="Schema 1",
                instrument="",
                selector="filename:starts_with:test",
                variables={
                    "common_var": {
                        "source": "VALUE",
                        "value": "schema1_value",
                        "value_type": "string",
                    },
                    "schema1_only": {
                        "source": "VALUE",
                        "value": "unique1",
                        "value_type": "string",
                    },
                },
                schema={
                    "common_field": {"value": "<common_var>", "type": "string"},
                    "schema1_field": {"value": "<schema1_only>", "type": "string"},
                },
            ),
            "schema2": MetadataSchema(
                order=1,
                id="schema2",
                name="Schema 2",
                instrument="",
                selector="filename:starts_with:test",
                variables={
                    "common_var": {
                        "source": "VALUE",
                        "value": "schema2_value",
                        "value_type": "string",
                    },
                    "schema2_only": {
                        "source": "VALUE",
                        "value": "unique2",
                        "value_type": "string",
                    },
                },
                schema={
                    "common_field": {"value": "<common_var>", "type": "string"},
                    "schema2_field": {"value": "<schema2_only>", "type": "string"},
                },
            ),
        }
    )

    merged_schema = select_applicable_schema(Path("test_file.nxs"), schemas)

    # Should return merged schema with schema2 having priority for conflicts
    assert merged_schema.id == "schema2_schema1"
    assert "common_var" in merged_schema.variables
    assert "schema1_only" in merged_schema.variables
    assert "schema2_only" in merged_schema.variables

    assert merged_schema.variables["common_var"]["value"] == "schema2_value"

    assert "common_field" in merged_schema.schema
    assert "schema1_field" in merged_schema.schema
    assert "schema2_field" in merged_schema.schema


def test_metadata_schema_merge_variables_from_multiple_schemas() -> None:
    """Test that variables from multiple matching schemas are properly combined."""
    schemas = OrderedDict(
        {
            "base_schema": MetadataSchema(
                order=2,
                id="base_schema",
                name="Base Schema",
                instrument="",
                selector="filename:starts_with:test_data",
                variables={
                    "pid": {
                        "source": "NXS",
                        "path": "/entry0/user/proposal",
                        "value_type": "string",
                    },
                    "dataset_name": {
                        "source": "NXS",
                        "path": "/entry0/title",
                        "value_type": "string",
                    },
                },
                schema={
                    "pid": {"value": "<pid>", "type": "string"},
                    "dataset_name": {"value": "<dataset_name>", "type": "string"},
                },
            ),
            "powder_schema": MetadataSchema(
                order=1,
                id="powder_schema",
                name="Powder Diffraction Schema",
                instrument="Powder Diffraction",
                selector="filename:starts_with:test_data",
                variables={
                    "wavelength": {
                        "source": "NXS",
                        "path": "/entry0/wavelength",
                        "value_type": "string",
                    },
                    "dataset_name": {
                        "source": "NXS",
                        "path": "/entry0/experiment_title",
                        "value_type": "string",
                    },  # Conflict
                },
                schema={
                    "wavelength": {"value": "<wavelength>", "type": "string"},
                    "dataset_name": {"value": "<dataset_name>", "type": "string"},
                },
            ),
        }
    )

    merged_schema = select_applicable_schema(Path("test_data.nxs"), schemas)

    # Should have variables from both schemas
    assert "pid" in merged_schema.variables
    assert "dataset_name" in merged_schema.variables
    assert "wavelength" in merged_schema.variables

    # Powder schema should win for dataset_name conflict (order=1 vs order=2)
    assert merged_schema.variables["dataset_name"]["path"] == "/entry0/experiment_title"


def test_metadata_schema_no_matching_schemas_returns_none() -> None:
    """Test that when no schemas match, None is returned."""
    schemas = OrderedDict(
        {
            "schema1": MetadataSchema(
                order=1,
                id="schema1",
                name="Schema 1",
                instrument="",
                selector="filename:starts_with:nomatch",
                variables={},
                schema={},
            ),
        }
    )

    result = select_applicable_schema(Path("different_file.nxs"), schemas)
    assert result is None


def test_metadata_schema_three_schemas_priority_resolution() -> None:
    """Test that with 3 schemas, higher priority (lower order) overrides lower priority for conflicting fields."""
    schemas = OrderedDict(
        {
            "low_priority": MetadataSchema(
                order=3,  # Lowest priority (highest order number)
                id="low_priority",
                name="Low Priority Schema",
                instrument="",
                selector="filename:starts_with:test",
                variables={
                    "common_field": {
                        "source": "VALUE",
                        "value": "low_priority_value",
                        "value_type": "string",
                    },
                    "low_only": {
                        "source": "VALUE",
                        "value": "low_unique",
                        "value_type": "string",
                    },
                },
                schema={
                    "common_field": {"value": "<common_field>", "type": "string"},
                    "low_field": {"value": "<low_only>", "type": "string"},
                },
            ),
            "high_priority": MetadataSchema(
                order=1,  # Highest priority (lowest order number)
                id="high_priority",
                name="High Priority Schema",
                instrument="",
                selector="filename:starts_with:test",
                variables={
                    "high_only": {
                        "source": "VALUE",
                        "value": "high_unique",
                        "value_type": "string",
                    },
                },
                schema={
                    "high_field": {"value": "<high_only>", "type": "string"},
                },
            ),
            "middle_priority": MetadataSchema(
                order=2,  # Middle priority
                id="middle_priority",
                name="Middle Priority Schema",
                instrument="",
                selector="filename:starts_with:test",
                variables={
                    "common_field": {
                        "source": "VALUE",
                        "value": "middle_priority_value",
                        "value_type": "string",
                    },
                    "middle_only": {
                        "source": "VALUE",
                        "value": "middle_unique",
                        "value_type": "string",
                    },
                },
                schema={
                    "common_field": {"value": "<common_field>", "type": "string"},
                    "middle_field": {"value": "<middle_only>", "type": "string"},
                },
            ),
        }
    )

    merged_schema = select_applicable_schema(Path("test_data.nxs"), schemas)

    assert "common_field" in merged_schema.variables
    assert "low_only" in merged_schema.variables
    assert "high_only" in merged_schema.variables
    assert "middle_only" in merged_schema.variables

    # High priority schema should win for common_field conflicts (order=1 beats order=2 and order=3)
    # Since high_priority doesn't have common_field, middle_priority (order=2) should beat low_priority (order=3)
    assert merged_schema.variables["common_field"]["value"] == "middle_priority_value"

    assert "common_field" in merged_schema.schema
    assert "low_field" in merged_schema.schema
    assert "high_field" in merged_schema.schema
    assert "middle_field" in merged_schema.schema


def test_metadata_schema_same_order_conflict_raises_error() -> None:
    """Test that when two schemas have the same order and conflict, an error is raised."""
    schemas = OrderedDict(
        {
            "schema1": MetadataSchema(
                order=1,  # Same order
                id="schema1",
                name="Schema 1",
                instrument="",
                selector="filename:starts_with:test",
                variables={
                    "common_var": {
                        "source": "VALUE",
                        "value": "schema1_value",
                        "value_type": "string",
                    },
                    "schema1_only": {
                        "source": "VALUE",
                        "value": "unique1",
                        "value_type": "string",
                    },
                },
                schema={
                    "common_field": {"value": "<common_var>", "type": "string"},
                    "schema1_field": {"value": "<schema1_only>", "type": "string"},
                },
            ),
            "schema2": MetadataSchema(
                order=1,  # Same order - conflict!
                id="schema2",
                name="Schema 2",
                instrument="",
                selector="filename:starts_with:test",
                variables={
                    "common_var": {
                        "source": "VALUE",
                        "value": "schema2_value",
                        "value_type": "string",
                    },  # Conflict
                    "schema2_only": {
                        "source": "VALUE",
                        "value": "unique2",
                        "value_type": "string",
                    },
                },
                schema={
                    "common_field": {
                        "value": "<common_var>",
                        "type": "string",
                    },  # Conflict
                    "schema2_field": {"value": "<schema2_only>", "type": "string"},
                },
            ),
        }
    )

    # Should raise an error due to same order conflict
    with pytest.raises(
        ValueError,
        match="Schema conflict detected: schemas 'schema1' and 'schema2' have the same order \\(1\\) and conflicting variable 'common_var'",
    ):
        select_applicable_schema(Path("test_file.nxs"), schemas)


def test_metadata_schema_no_order_field_treated_as_lowest_priority() -> None:
    """Test that schemas without order field are treated as lowest priority."""
    # Create schema without order field by manually setting attributes
    schema_with_order = MetadataSchema(
        order=1,
        id="schema_with_order",
        name="Schema With Order",
        instrument="",
        selector="filename:starts_with:test",
        variables={
            "common_var": {
                "source": "VALUE",
                "value": "high_priority_value",
                "value_type": "string",
            },
            "ordered_only": {
                "source": "VALUE",
                "value": "ordered_unique",
                "value_type": "string",
            },
        },
        schema={
            "common_field": {"value": "<common_var>", "type": "string"},
            "ordered_field": {"value": "<ordered_only>", "type": "string"},
        },
    )

    schema_no_order = MetadataSchema(
        order=sys.maxsize,  # Simulate no order by using maximum integer value
        id="schema_no_order",
        name="Schema No Order",
        instrument="",
        selector="filename:starts_with:test",
        variables={
            "common_var": {
                "source": "VALUE",
                "value": "low_priority_value",
                "value_type": "string",
            },
            "unordered_only": {
                "source": "VALUE",
                "value": "unordered_unique",
                "value_type": "string",
            },
        },
        schema={
            "common_field": {"value": "<common_var>", "type": "string"},
            "unordered_field": {"value": "<unordered_only>", "type": "string"},
        },
    )

    # Remove order attribute to simulate missing order field
    delattr(schema_no_order, 'order')

    schemas = OrderedDict(
        {
            "schema_no_order": schema_no_order,
            "schema_with_order": schema_with_order,
        }
    )

    merged_schema = select_applicable_schema(Path("test_file.nxs"), schemas)

    # Schema with order should have priority over schema without order
    assert "common_var" in merged_schema.variables
    assert "ordered_only" in merged_schema.variables
    assert "unordered_only" in merged_schema.variables

    # Schema with order should win the conflict
    assert merged_schema.variables["common_var"]["value"] == "high_priority_value"

    assert "common_field" in merged_schema.schema
    assert "ordered_field" in merged_schema.schema
    assert "unordered_field" in merged_schema.schema


def test_metadata_schema_no_order_fields_conflict_raises_error() -> None:
    """Test that when two schemas have no order field and conflict, an error is raised."""
    schema1 = MetadataSchema(
        order=sys.maxsize,  # Will be removed
        id="schema1",
        name="Schema 1",
        instrument="",
        selector="filename:starts_with:test",
        variables={
            "common_var": {
                "source": "VALUE",
                "value": "schema1_value",
                "value_type": "string",
            },
        },
        schema={
            "common_field": {"value": "<common_var>", "type": "string"},
        },
    )

    schema2 = MetadataSchema(
        order=sys.maxsize,  # Will be removed
        id="schema2",
        name="Schema 2",
        instrument="",
        selector="filename:starts_with:test",
        variables={
            "common_var": {
                "source": "VALUE",
                "value": "schema2_value",
                "value_type": "string",
            },
        },
        schema={
            "common_field": {"value": "<common_var>", "type": "string"},
        },
    )

    # Remove order attributes to simulate missing order fields
    delattr(schema1, 'order')
    delattr(schema2, 'order')

    schemas = OrderedDict(
        {
            "schema1": schema1,
            "schema2": schema2,
        }
    )

    # Should raise an error due to no order field conflict
    with pytest.raises(
        ValueError,
        match="Schema conflict detected: schemas 'schema1' and 'schema2' both have no order field and conflicting variable 'common_var'",
    ):
        select_applicable_schema(Path("test_file.nxs"), schemas)
