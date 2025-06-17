# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import json
import pathlib
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Any

SCIENTIFIC_METADATA_TYPE = "scientific_metadata"
HIGH_LEVEL_METADATA_TYPE = "high_level"
VALID_METADATA_TYPES = (SCIENTIFIC_METADATA_TYPE, HIGH_LEVEL_METADATA_TYPE)


def load_metadata_extractors(extractor_name: str) -> Callable:
    """Load metadata extractors from the entry points."""

    return entry_points(group="scicat_ingestor.metadata_extractor")[
        extractor_name
    ].load()


def list_schema_file_names(schemas_directory: pathlib.Path) -> list[pathlib.Path]:
    """
    Return a list of the metadata schema file names found in ``schemas_directory``.

    Valid metadata schema configuration ends with imsc.json
    ``imsc`` stands for Ingestor Metadata Schema Configuration
    """
    import os

    return [
        schemas_directory / pathlib.Path(file_name)
        for file_name in os.listdir(schemas_directory)
        if ("imsc.json" in file_name) and not file_name.startswith(".")
    ]


def _load_json_schema(schema_file_name: pathlib.Path) -> dict:
    return json.loads(schema_file_name.read_text())


def _resolve_schema_imports(
    schema: dict[str, Any],
    schema_file_name: pathlib.Path,
    caller_schemas: list[pathlib.Path] | None = None,
) -> dict[str, Any]:
    """
    Resolve imports in the schema dictionary.

    Imports are defined as a list of modules file relative paths to the schema file.
    The importer has priority over imported schemas in case of conflicts.

    Args:
        schema: The schema dictionary to resolve imports for
        schema_file_name: Path to the current schema file
        caller_schemas: List of schema file paths in the import chain to detect circular dependencies
    """
    if caller_schemas is None:
        caller_schemas = []

    if schema_file_name in caller_schemas:
        raise ValueError(
            "Circular dependency detected: "
            f"{' -> '.join(str(p) for p in caller_schemas)} -> {schema_file_name}"
        )

    if "import" in schema:
        if not isinstance(schema["import"], list):
            raise ValueError("Import must be a list of file paths.")
        result_schema = schema.copy()

        current_caller_schemas = [*caller_schemas, schema_file_name]

        for import_path in schema["import"]:
            import_file_path = schema_file_name.parent / pathlib.Path(import_path)
            if not import_file_path.exists():
                raise FileNotFoundError(
                    f"Import file {import_file_path} does not exist."
                )

            imported_schema = _load_json_schema(import_file_path)
            resolved_imported_schema = _resolve_schema_imports(
                imported_schema, import_file_path, current_caller_schemas
            )

            for key, value in resolved_imported_schema.items():
                if key == "import":
                    continue
                elif key in ["variables", "schema"]:
                    if key not in result_schema:
                        result_schema[key] = {}

                    for sub_key, sub_value in value.items():
                        if sub_key not in result_schema[key]:
                            result_schema[key][sub_key] = sub_value
                else:
                    if key not in result_schema:
                        result_schema[key] = value

        result_schema.pop("import", None)
        return result_schema

    return schema


@dataclass(kw_only=True)
class MetadataSchemaVariable:
    source: str
    value_type: str


@dataclass(kw_only=True)
class NexusFileMetadataVariable(MetadataSchemaVariable):
    """Metadata variable that is extracted from the nexus file."""

    path: str


@dataclass(kw_only=True)
class ScicatMetadataVariable(MetadataSchemaVariable):
    """Metadata variable that is extracted from the scicat backend."""

    url: str
    field: str


@dataclass(kw_only=True)
class ValueMetadataVariable(MetadataSchemaVariable):
    """Metadata variable that is from the variable map."""

    operator: str = ""
    value: str
    field: str | None = None
    pattern: str | None = None
    replacement: str | None = None
    # We only allow one field(argument) for now


@dataclass(kw_only=True)
class MetadataItem:
    machine_name: str
    human_name: str
    field_type: str
    value: str
    type: str


def build_metadata_variables(
    variables: dict[str, dict[str, str]],
) -> dict[str, MetadataSchemaVariable]:
    """
    Return a dictionary of metadata variables from the ``variables`` dictionary.
    """

    def _build_metadata_variable(variable: dict[str, str]) -> MetadataSchemaVariable:
        match variable["source"]:
            case "NXS":
                return NexusFileMetadataVariable(**variable)
            case "SC":
                return ScicatMetadataVariable(**variable)
            case "VALUE":
                return ValueMetadataVariable(**variable)
            case _:
                raise ValueError(
                    f"Invalid source name: {variable['source']} for variable {variable}"
                )

    return {
        variable_name: _build_metadata_variable(variable)
        for variable_name, variable in variables.items()
    }


@dataclass(kw_only=True)
class MetadataSchema:
    id: str
    name: str
    instrument: str
    selector: str | dict
    order: int
    variables: dict[str, MetadataSchemaVariable]
    schema: dict[str, MetadataItem]

    @classmethod
    def from_dict(cls, schema: dict) -> "MetadataSchema":
        return cls(
            **{
                **schema,
                "variables": build_metadata_variables(schema["variables"]),
                "schema": {
                    item_name: MetadataItem(
                        **{
                            "human_name": item["machine_name"],
                            # if the human name is not provided, use the machine name
                            **item,
                        },
                    )
                    for item_name, item in schema["schema"].items()
                },
            },
        )

    @classmethod
    def from_file(cls, schema_file_name: pathlib.Path) -> "MetadataSchema":
        resolved_schema = _resolve_schema_imports(
            _load_json_schema(schema_file_name), schema_file_name
        )
        return cls.from_dict(resolved_schema)


def render_variable_value(var_value: Any, variable_registry: dict) -> str:
    # if input is not a string it converts it to string
    output_value = var_value if isinstance(var_value, str) else json.dumps(var_value)

    # If it is only one variable, then it is a simple replacement
    if (
        var_key := output_value.removesuffix(">").removeprefix("<")
    ) in variable_registry:
        return variable_registry[var_key]

    # If it is a complex variable, then it is a combination of variables
    # similar to f-string in python
    for reg_var_name, reg_var_value in variable_registry.items():
        output_value = output_value.replace(
            "<" + reg_var_name + ">", str(reg_var_value)
        )

    if "<" in output_value and ">" in output_value:
        raise Exception(f"Unresolved variable: {var_value}")

    output_value = (
        output_value if isinstance(var_value, str) else json.loads(output_value)
    )
    return output_value


def collect_schemas(dir_path: pathlib.Path) -> OrderedDict[str, MetadataSchema]:
    """
    Return a dictionary of the metadata schema configurations found in ``dir_path``.

    Schemas are sorted by their name.
    """
    metadata_schemas = sorted(
        [
            MetadataSchema.from_file(schema_file_path)
            for schema_file_path in list_schema_file_names(dir_path)
        ],
        key=lambda schema: (schema.order, schema.name.capitalize()),
        # name is capitalized to make sure that the order is
        # alphabetically sorted in a non-case-sensitive way
    )
    schemas = OrderedDict()
    for metadata_schema in metadata_schemas:
        schemas[metadata_schema.id] = metadata_schema
    return schemas


def _select_applicable_schema(
    selector: str | dict, filename: str | None = None
) -> bool:
    if isinstance(selector, str):
        # filename:starts_with:/ess/data/coda
        select_target_name, select_function_name, select_argument = selector.split(":")
        if select_target_name in ["filename"]:
            select_target_value = filename
        else:
            raise ValueError(f"Invalid target name {select_target_name}")

        if select_function_name == "starts_with":
            return select_target_value.startswith(select_argument)
        else:
            raise ValueError(f"Invalid function name {select_function_name}")

    elif isinstance(selector, dict):
        output = True
        for key, conditions in selector.items():
            if key == "or":
                output = output and any(
                    _select_applicable_schema(item, filename) for item in conditions
                )
            elif key == "and":
                output = output and all(
                    _select_applicable_schema(item, filename) for item in conditions
                )
            else:
                raise NotImplementedError("Invalid operator")
        return output
    else:
        raise Exception(f"Invalid type for schema selector {type(selector)}")


def select_applicable_schema(
    nexus_file: pathlib.Path,
    schemas: OrderedDict[str, MetadataSchema],
) -> MetadataSchema:
    """
    Evaluates which metadata schema configuration is applicable to ``nexus_file``.

    Order of the schemas matters and first schema that is suitable is selected.
    """
    for schema in schemas.values():
        if _select_applicable_schema(schema.selector, str(nexus_file)):
            return schema

    raise Exception("No applicable metadata schema configuration found!!")
