# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 ScicatProject contributors (https://github.com/ScicatProject)
import argparse
import logging
import pathlib

from scicat_logging import build_devtool_logger
from scicat_metadata import (
    VALID_METADATA_TYPES,
    MetadataItemConfig,
    MetadataSchema,
    _is_json_file,
    list_schema_file_names,
)


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


def _validate_mandatory_machine_names(
    schema: MetadataSchema, file_name: str, logger: logging.Logger
) -> bool:
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
            file_name,
            '[/yellow], [yellow]'.join(missing),
        )
        return False

    logger.info("All mandatory [green]machine names[/green] found.")
    return True


def _validate_schema_selector(selector: str | dict, logger: logging.Logger) -> bool:
    if isinstance(selector, str):
        if len(selector.split(":")) != 3:
            logger.error(
                "Invalid selector format: '[yellow]%s[/yellow]' "
                "Selector should be <bold>field:fiter_type:value</bold>",
                selector,
            )
            return False
    elif isinstance(selector, dict):
        for conditions in selector.values():
            return all(_validate_schema_selector(item, logger) for item in conditions)

    logger.info("All [green]schema selector[/green] validated.")
    return True


def _validate_schema_item_config_type(
    metadata_item_configs: dict[str, MetadataItemConfig], logger: logging.Logger
) -> bool:
    invalid_types = [
        field.field_type
        for field in metadata_item_configs.values()
        if field.field_type not in VALID_METADATA_TYPES
    ]

    if any(invalid_types):
        logger.error(
            "Invalid metadata schema types found: %s .\nValid types are: %s. "
            "Metadata items with invalid types will be ignored.",
            VALID_METADATA_TYPES,
            invalid_types,
        )
        return False

    return True


def _validate_file(schema_file: pathlib.Path, logger: logging.Logger) -> bool:
    logger.info(
        "Checking schema file: [bold purple]%s [/bold purple]", schema_file.name
    )
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
        msg = "Schema file [green]%s[/ green] has [bold green]VALID[/bold green] structure."
        logger.info(msg, schema_file.name)
    except Exception as e:
        msg = "Schema file [red]%s[/red] is [bold red]INVALID[/bold red]: %s"
        logger.error(msg, schema_file.name, e)
        return False

    return (
        # Validate schema
        # Manually check mandatory machine names
        # They are part of fields of `scicat_dataset.ScicatDataset` dataclass.
        # Some of the mandatory arguments are not filled by schema definition.
        _validate_mandatory_machine_names(schema, schema_file.name, logger)
        and _validate_schema_selector(schema.selector, logger)
        # Validate item type config. It's a free field but should be one of valid types.
        and _validate_schema_item_config_type(schema.schema, logger)
    )


def validate_schema(
    *, schema_path: pathlib.Path | None = None, logger: logging.Logger | None = None
) -> None:
    """Validate the schema file."""
    if schema_path is None:
        # Parse command line arguments
        args = _parse_args()
        schema_path = pathlib.Path(args.schema_file)

    if logger is None:
        # Build a logger
        logger = build_devtool_logger("validate-schema-file")

    logger.info("Scicat ingestor metadata schema files validation test.")
    logger.info("It only validates if the schema file has a valid structure.")

    # Collect schema files
    schema_files = _collect_target_files(schema_path, logger)
    if not schema_files:
        raise ValueError("No schema file found to validate.")

    logger.info("Validating %d schema files...", len(schema_files))

    # Collect validities of all files first
    # to avoid stopping on the first invalid file.
    validities = {
        schema_file.name: _validate_file(schema_file, logger)
        for schema_file in schema_files
    }
    if all(validities.values()):
        logger.info(
            "All schema files,\n  - %s\nare [bold green]VALID[/bold green].",
            '\n  - '.join(validities.keys()),
        )
    else:
        invalid_files = {k for k, _ in filter(lambda kv: not kv[1], validities.items())}
        valid_files = set(validities.keys()) - invalid_files
        if valid_files:
            logger.info(
                "[bold green]Valid[/bold green] files: \n  - %s\n",
                '\n  - '.join(valid_files),
            )
        logger.error(
            "[bold red]Invalid[/bold red] files: \n  - %s\n",
            '\n  - '.join(invalid_files),
        )
        raise ValueError("One or more schema files are invalid: Please check the logs.")
