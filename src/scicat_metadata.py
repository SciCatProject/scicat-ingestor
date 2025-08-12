# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)

import argparse
import json
import logging
import pathlib
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Any

import yaml

SCIENTIFIC_METADATA_TYPE = "scientific_metadata"
HIGH_LEVEL_METADATA_TYPE = "high_level"
VALID_METADATA_TYPES = (SCIENTIFIC_METADATA_TYPE, HIGH_LEVEL_METADATA_TYPE)


def load_metadata_extractors(extractor_name: str) -> Callable:
    """Load metadata extractors from the entry points."""

    return entry_points(group="scicat_ingestor.metadata_extractor")[
        extractor_name
    ].load()


def _is_file_name_valid(file_name: str) -> bool:
    return (
        (
            'imsc.json' in file_name
        )  # Should be removed once we stop supporting JSON schema files
        or ('imsc.yml' in file_name)
        or ('imsc.yaml' in file_name)
    ) and not file_name.startswith('.')


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
        if _is_file_name_valid(file_name)
    ]


def _is_json_file(text: str) -> bool:
    """Check if the text is a valid JSON file."""
    try:
        json.loads(text)
        return True
    except json.JSONDecodeError:
        return False


def _load_schema_file(schema_file_name: pathlib.Path) -> dict:
    import warnings

    text = schema_file_name.read_text()
    if _is_json_file(text):
        # If it is a JSON file, load it as JSON.
        # We cannot use try-except here since `yaml.safe_load`
        # can also load JSON strings.
        warnings.warn(
            "The json schema file format is deprecated. Please use YAML format.\n"
            "You can dump the schema to YAML using `scicat-json-to-yaml` command."
            "This warning will be removed from future versions.",
            DeprecationWarning,
            stacklevel=3,
        )
        return json.loads(text)
    else:
        loaded_schema = yaml.safe_load(text)

    if not isinstance(loaded_schema, dict):
        raise ValueError(
            f"Invalid schema file {schema_file_name}. Schema must be a dictionary."
        )

    return loaded_schema


@dataclass(kw_only=True)
class MetadataVariableValueSpec:
    """Value and unit retrieved according to `MetadataVariableConfig`."""

    value: Any
    unit: str = ""


@dataclass(kw_only=True)
class MetadataVariableConfig:
    source: str
    value_type: str
    unit: str = ""
    """Hardcoded unit for metadata variable.

    We are not using ``None`` since scicat understands
    ``None`` and an empty string as same units.
    """


@dataclass(kw_only=True)
class VariableConfigNexusFile(MetadataVariableConfig):
    """Metadata variable that is extracted from the nexus file."""

    path: str


@dataclass(kw_only=True)
class VariableConfigScicat(MetadataVariableConfig):
    """Metadata variable that is extracted from the scicat backend."""

    url: str
    field: str


@dataclass(kw_only=True)
class VariableConfigValue(MetadataVariableConfig):
    """Metadata variable that is from the variable map."""

    operator: str = ""
    value: str
    field: str | None = None
    pattern: str | None = None
    replacement: str | None = None
    # We only allow one field(argument) for now


@dataclass(kw_only=True)
class MetadataItemConfig:
    machine_name: str
    field_type: str
    value: str
    type: str
    human_name: str | None = None
    unit: str | None = None


def build_metadata_variables(
    variables: dict[str, dict[str, str]],
) -> dict[str, MetadataVariableConfig]:
    """
    Return a dictionary of metadata variables from the ``variables`` dictionary.
    """

    def _build_metadata_variable(variable: dict[str, str]) -> MetadataVariableConfig:
        match variable["source"]:
            case "NXS":
                return VariableConfigNexusFile(**variable)
            case "SC":
                return VariableConfigScicat(**variable)
            case "VALUE":
                return VariableConfigValue(**variable)
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
    variables: dict[str, MetadataVariableConfig]
    schema: dict[str, MetadataItemConfig]

    @classmethod
    def from_dict(cls, schema: dict) -> "MetadataSchema":
        return cls(
            **{
                **schema,
                "variables": build_metadata_variables(schema["variables"]),
                "schema": {
                    item_name: MetadataItemConfig(
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
        return cls.from_dict(_load_schema_file(schema_file_name))


def _maybe_single_variable(val: str) -> bool:
    return val.startswith("<") and val.endswith(">")


def render_variable_value(
    var_value: str | dict | float,
    variable_registry: dict[str, MetadataVariableValueSpec],
) -> MetadataVariableValueSpec:
    # if input is not a string it converts it to string
    output_value = var_value if isinstance(var_value, str) else json.dumps(var_value)

    # If it is only one variable, then it is a simple replacement
    if (
        _maybe_single_variable(output_value)
        and (var_key := output_value.removesuffix(">").removeprefix("<"))
        in variable_registry
    ):
        return variable_registry[var_key]

    # If it is a complex variable, then it is a combination of variables
    # similar to f-string in python
    # In this case, we do not forward unit
    for reg_var_name, reg_var_value in variable_registry.items():
        output_value = output_value.replace(
            "<" + reg_var_name + ">", str(reg_var_value.value)
        )

    if "<" in output_value and ">" in output_value:
        raise Exception(f"Unresolved variable: {var_value}")

    output_value = (
        output_value if isinstance(var_value, str) else json.loads(output_value)
    )
    return MetadataVariableValueSpec(value=output_value)


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


def _parse_args() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser(
        description="Validate the metadata schema files."
    )
    arg_parser.add_argument(
        type=str,
        help="Schema file/directory path (imsc) to validate. If a directory is passed, "
        "all schema files in the directory will be validated.",
        dest="schema_file",
    )
    return arg_parser.parse_args()


def _collect_target_files(
    schema_file: pathlib.Path, logger: logging.Logger
) -> list[pathlib.Path]:
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file(location) {schema_file} does not exist.")
    elif schema_file.is_dir():
        logger.info("Collecting schema files from the directory `%s`...", schema_file)
        schema_files = list_schema_file_names(schema_file)
        if not schema_files:
            raise FileNotFoundError(
                f"No schema files found in the directory {schema_file}."
            )
        file_list = '\n - '.join([file_path.name for file_path in schema_files])
        logger.info("Found %d schema files.\n - %s", len(schema_files), file_list)
    else:
        schema_files = [schema_file]

    return schema_files


def _validate_file(schema_file: pathlib.Path, logger: logging.Logger) -> bool:
    # Check if it is a json file
    if _is_json_file(schema_file.read_text()):
        logger.warning(
            "Schema file [yellow]%s[/yellow] is in "
            "[bold yellow]JSON[/bold yellow] format. "
            "It is recommended to use [bold yellow]YAML[/bold yellow] format for new schema files.",
            schema_file,
        )
        return False
    # Try building the `MetadataSchema` from the file.
    try:
        schema = MetadataSchema.from_file(schema_file)
        msg = "Schema file [green]%s[/ green] is [bold green]VALID[/bold green]."
        logger.info(msg, schema_file)
    except Exception as e:
        msg = "Schema file [red]%s[/red] is [bold red]INVALID[/bold red]: %s"
        logger.error(msg, schema_file.name, e)
        return False

    # Validate schema
    # Manually check mandatory machine names
    # They are part of fields of `scicat_dataset.ScicatDataset` dataclass.
    # Some of the mandatory arguments are not filled by schema definition.
    MANDATORY_MACHINE_NAMES = {
        'datasetName',
        'principalInvestigator',
        'creationLocation',
        'owner',
        'ownerEmail',
        'sourceFolder',
        'contactEmail',
        'creationTime',
    }
    machine_names = {field.machine_name for field in schema.schema.values()}
    if not MANDATORY_MACHINE_NAMES.issubset(machine_names):
        missing = MANDATORY_MACHINE_NAMES - machine_names
        logger.error(
            "Schema file [red]%s[/red] is missing mandatory fields: [yellow]%s[/yellow]",
            schema_file.name,
            '[/yellow], [yellow]'.join(missing),
        )
        return False

    return True


def validate_schema() -> None:
    """Validate the schema file."""
    from pathlib import Path

    from scicat_logging import build_devtool_logger

    # Parse command line arguments
    args = _parse_args()
    # Build a logger
    logger = build_devtool_logger("validate-schema-file")
    logger.info("Scicat ingestor metadata schema files validation test.")
    logger.info("It only validates if the schema file has a valid structure.")

    # Collect schema files
    schema_files = _collect_target_files(Path(args.schema_file), logger)
    logger.info("Validating %d schema files...", len(schema_files))

    # Collect validities of all files first
    # to avoid stopping on the first invalid file.
    validities = [_validate_file(schema_file, logger) for schema_file in schema_files]
    if all(validities):
        logger.info("All schema files are valid.")
    else:
        error_msg = "One or more schema files are invalid. Please check the logs."
        logger.error(error_msg)
        raise ValueError(error_msg)
