# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import json
import pathlib
from collections.abc import Callable
from importlib.metadata import entry_points


def load_metadata_extractors(extractor_name: str) -> Callable:
    """Load metadata extractors from the entry points."""

    return entry_points(group="scicat_ingestor.metadata_extractor")[
        extractor_name
    ].load()


def list_schema_file_names(schemas_directory: str | pathlib.Path) -> list[str]:
    """
    Return a list of the metadata schema file names found in ``schemas_directory``.

    Valid metadata schema configuration ends with imsc.json
    ``imsc`` stands for Ingestor Metadata Schema Configuration
    """
    import os

    return [
        file_name
        for file_name in os.listdir(schemas_directory)
        if file_name.endswith("imsc.json") and not file_name.startswith(".")
    ]


def _load_json_schema(schema_file_name: str) -> dict:
    with open(schema_file_name) as fh:
        return json.load(fh)


def collect_schemas(dir_path: str | pathlib.Path) -> dict:
    """
    Return a dictionary of the metadata schema configurations found in ``dir_path``.
    """
    return {
        (schema := _load_json_schema(schema_file_name))["id"]: schema
        for schema_file_name in list_schema_file_names(dir_path)
    }


def select_applicable_schema(nexus_file, nxs, schemas):
    """
    Evaluates which metadata schema configuration is applicable to ``nexus_file``.

    Order of the schemas matters and first schema that is suitable is selected.
    """
    for schema in schemas.values():
        if isinstance(schema['selector'], str):
            selector_list = schema['selector'].split(':')
            selector = {
                "operand_1": selector_list[0],
                "operation": selector_list[1],
                "operand_2": selector_list[2],
            }
        elif isinstance(schema['selector'], dict):
            selector = schema['selector']
        else:
            raise Exception("Invalid type for schema selector")

        if selector['operand_1'] in [
            "filename",
            "data_file",
            "nexus_file",
            "data_file_name",
        ]:
            selector['operand_1_value'] = nexus_file

        if selector['operation'] == "starts_with":
            if selector['operand_1_value'].startswith(selector['operand_2']):
                return schema

    raise Exception("No applicable metadata schema configuration found!!")
