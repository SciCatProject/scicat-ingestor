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

ALL_SCHEMA_EXAMPLES = [
    schema_file
    for schema_file in list_schema_file_names(
        Path(__file__).parent.parent / Path("resources")
    )
    if "modules/" not in str(schema_file)  # Exclude module files
]


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
            "d17-example-schema",  # D17, 1, D17 Instrument Example Schema
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


def test_schema_import_resolution(tmp_path: Path) -> None:
    """Test that schema imports are correctly resolved."""
    module_schema = {
        "id": "module-schema",
        "name": "Module Schema",
        "variables": {
            "module_var1": {
                "source": "VALUE",
                "value": "module_value1",
                "value_type": "string",
            },
            "module_var2": {
                "source": "VALUE",
                "value": "module_value2",
                "value_type": "string",
            },
        },
        "schema": {
            "module_field1": {
                "machine_name": "module_field1",
                "field_type": "scientific_metadata",
                "value": "<module_var1>",
                "type": "string",
            },
            "module_field2": {
                "machine_name": "module_field2",
                "field_type": "scientific_metadata",
                "value": "<module_var2>",
                "type": "string",
            },
        },
    }

    importing_schema = {
        "id": "importing-schema",
        "name": "Importing Schema",
        "import": ["module_schema.json"],
        "order": 1,
        "instrument": "Test Instrument",
        "selector": "filename:starts_with:test",
        "variables": {
            "importing_var": {
                "source": "VALUE",
                "value": "importing_value",
                "value_type": "string",
            },
            "module_var1": {
                "source": "VALUE",
                "value": "overridden_value",
                "value_type": "string",
            },
        },
        "schema": {
            "importing_field": {
                "machine_name": "importing_field",
                "field_type": "scientific_metadata",
                "value": "<importing_var>",
                "type": "string",
            },
            "module_field1": {
                "machine_name": "module_field1",
                "field_type": "scientific_metadata",
                "value": "<module_var1>",
                "type": "string",
            },
        },
    }

    module_schema_file = tmp_path / "module_schema.json"
    importing_schema_file = tmp_path / "importing_schema.json"

    module_schema_file.write_text(json.dumps(module_schema))
    importing_schema_file.write_text(json.dumps(importing_schema))

    from scicat_metadata import _load_json_schema, _resolve_schema_imports

    loaded_schema = _load_json_schema(importing_schema_file)
    resolved_schema = _resolve_schema_imports(loaded_schema, importing_schema_file)

    assert "module_var1" in resolved_schema["variables"]
    assert "module_var2" in resolved_schema["variables"]
    assert "importing_var" in resolved_schema["variables"]

    assert resolved_schema["variables"]["module_var1"]["value"] == "overridden_value"
    assert resolved_schema["variables"]["module_var2"]["value"] == "module_value2"
    assert resolved_schema["variables"]["importing_var"]["value"] == "importing_value"

    assert "module_field1" in resolved_schema["schema"]
    assert "module_field2" in resolved_schema["schema"]
    assert "importing_field" in resolved_schema["schema"]

    assert "import" not in resolved_schema


def test_schema_import_circular_dependency_raises(tmp_path: Path) -> None:
    """Test that circular dependencies in imports are detected and raise an error."""
    schema_a = {
        "id": "schema-a",
        "name": "Schema A",
        "import": ["schema_b.json"],
        "variables": {
            "var_a": {"source": "VALUE", "value": "a", "value_type": "string"}
        },
        "schema": {},
    }

    schema_b = {
        "id": "schema-b",
        "name": "Schema B",
        "import": ["schema_a.json"],
        "variables": {
            "var_b": {"source": "VALUE", "value": "b", "value_type": "string"}
        },
        "schema": {},
    }

    schema_a_file = tmp_path / "schema_a.json"
    schema_b_file = tmp_path / "schema_b.json"

    schema_a_file.write_text(json.dumps(schema_a))
    schema_b_file.write_text(json.dumps(schema_b))

    from scicat_metadata import _load_json_schema, _resolve_schema_imports

    loaded_schema = _load_json_schema(schema_a_file)

    with pytest.raises(ValueError, match="Circular dependency detected"):
        _resolve_schema_imports(loaded_schema, schema_a_file)


def test_schema_import_triangle_circular_dependency_raises(tmp_path: Path) -> None:
    """Test that triangle circular dependencies (A->B->C->A) are detected and raise an error."""
    schema_a = {
        "id": "schema-a",
        "name": "Schema A",
        "import": ["schema_b.json"],
        "variables": {
            "var_a": {"source": "VALUE", "value": "a", "value_type": "string"}
        },
        "schema": {},
    }

    schema_b = {
        "id": "schema-b",
        "name": "Schema B",
        "import": ["schema_c.json"],
        "variables": {
            "var_b": {"source": "VALUE", "value": "b", "value_type": "string"}
        },
        "schema": {},
    }

    schema_c = {
        "id": "schema-c",
        "name": "Schema C",
        "import": ["schema_a.json"],
        "variables": {
            "var_c": {"source": "VALUE", "value": "c", "value_type": "string"}
        },
        "schema": {},
    }

    schema_a_file = tmp_path / "schema_a.json"
    schema_b_file = tmp_path / "schema_b.json"
    schema_c_file = tmp_path / "schema_c.json"

    schema_a_file.write_text(json.dumps(schema_a))
    schema_b_file.write_text(json.dumps(schema_b))
    schema_c_file.write_text(json.dumps(schema_c))

    from scicat_metadata import _load_json_schema, _resolve_schema_imports

    loaded_schema = _load_json_schema(schema_a_file)

    with pytest.raises(ValueError, match="Circular dependency detected"):
        _resolve_schema_imports(loaded_schema, schema_a_file)


def test_schema_import_missing_file_raises(tmp_path: Path) -> None:
    """Test that importing a non-existent file raises FileNotFoundError."""
    importing_schema = {
        "id": "importing-schema",
        "name": "Importing Schema",
        "import": ["nonexistent.json"],
        "variables": {},
        "schema": {},
    }

    importing_schema_file = tmp_path / "importing_schema.json"
    importing_schema_file.write_text(json.dumps(importing_schema))

    from scicat_metadata import _load_json_schema, _resolve_schema_imports

    loaded_schema = _load_json_schema(importing_schema_file)

    with pytest.raises(FileNotFoundError, match="Import file .* does not exist"):
        _resolve_schema_imports(loaded_schema, importing_schema_file)


def test_schema_import_invalid_format_raises(tmp_path: Path) -> None:
    """Test that invalid import format raises ValueError."""
    importing_schema = {
        "id": "importing-schema",
        "name": "Importing Schema",
        "import": "not_a_list.json",  # Should be a list
        "variables": {},
        "schema": {},
    }

    importing_schema_file = tmp_path / "importing_schema.json"
    importing_schema_file.write_text(json.dumps(importing_schema))

    from scicat_metadata import _load_json_schema, _resolve_schema_imports

    loaded_schema = _load_json_schema(importing_schema_file)

    with pytest.raises(ValueError, match="Import must be a list of file paths"):
        _resolve_schema_imports(loaded_schema, importing_schema_file)


def test_schema_import_multiple_imports(tmp_path: Path) -> None:
    """Test importing from multiple schema files."""
    module_schema_1 = {
        "id": "module-1",
        "variables": {
            "var1": {"source": "VALUE", "value": "value1", "value_type": "string"}
        },
        "schema": {
            "field1": {
                "machine_name": "field1",
                "field_type": "scientific_metadata",
                "value": "<var1>",
                "type": "string",
            }
        },
    }

    module_schema_2 = {
        "id": "module-2",
        "variables": {
            "var2": {"source": "VALUE", "value": "value2", "value_type": "string"}
        },
        "schema": {
            "field2": {
                "machine_name": "field2",
                "field_type": "scientific_metadata",
                "value": "<var2>",
                "type": "string",
            }
        },
    }

    importing_schema = {
        "id": "importing",
        "import": ["module1.json", "module2.json"],
        "order": 1,
        "instrument": "Test",
        "selector": "filename:starts_with:test",
        "variables": {
            "var3": {"source": "VALUE", "value": "value3", "value_type": "string"}
        },
        "schema": {
            "field3": {
                "machine_name": "field3",
                "field_type": "scientific_metadata",
                "value": "<var3>",
                "type": "string",
            }
        },
    }

    module1_file = tmp_path / "module1.json"
    module2_file = tmp_path / "module2.json"
    importing_file = tmp_path / "importing.json"

    module1_file.write_text(json.dumps(module_schema_1))
    module2_file.write_text(json.dumps(module_schema_2))
    importing_file.write_text(json.dumps(importing_schema))

    from scicat_metadata import _load_json_schema, _resolve_schema_imports

    loaded_schema = _load_json_schema(importing_file)
    resolved_schema = _resolve_schema_imports(loaded_schema, importing_file)

    assert "var1" in resolved_schema["variables"]
    assert "var2" in resolved_schema["variables"]
    assert "var3" in resolved_schema["variables"]

    assert "field1" in resolved_schema["schema"]
    assert "field2" in resolved_schema["schema"]
    assert "field3" in resolved_schema["schema"]


def test_schema_with_imports_can_be_loaded_as_metadata_schema(tmp_path: Path) -> None:
    """Test that a schema with imports can be successfully loaded as a MetadataSchema object."""
    module_schema = {
        "variables": {
            "module_var": {
                "source": "VALUE",
                "value": "module_value",
                "value_type": "string",
            }
        },
        "schema": {
            "module_field": {
                "machine_name": "module_field",
                "field_type": "scientific_metadata",
                "value": "<module_var>",
                "type": "string",
            }
        },
    }

    importing_schema = {
        "id": "test-schema",
        "name": "Test Schema",
        "order": 1,
        "instrument": "Test Instrument",
        "selector": "filename:starts_with:test",
        "import": ["module.json"],
        "variables": {
            "main_var": {
                "source": "VALUE",
                "value": "main_value",
                "value_type": "string",
            }
        },
        "schema": {
            "main_field": {
                "machine_name": "main_field",
                "field_type": "scientific_metadata",
                "value": "<main_var>",
                "type": "string",
            }
        },
    }

    module_file = tmp_path / "module.json"
    importing_file = tmp_path / "importing.json"

    module_file.write_text(json.dumps(module_schema))
    importing_file.write_text(json.dumps(importing_schema))

    schema = MetadataSchema.from_file(importing_file)

    assert schema.id == "test-schema"
    assert schema.name == "Test Schema"
    assert "module_var" in schema.variables
    assert "main_var" in schema.variables
    assert "module_field" in schema.schema
    assert "main_field" in schema.schema
