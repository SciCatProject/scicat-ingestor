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
        return cls.from_dict(_load_json_schema(schema_file_name))


def render_variable_value(var_value: Any, variable_registry: dict) -> str:
    # if input is not a string it converts it to string
    output_value = var_value if isinstance(var_value, str) else json.dumps(var_value)

    # If it is only one variable, then it is a simple replacement
    if (
        var_key := output_value.removesuffix(">").removeprefix("<")        
    ) in variable_registry:
        reg_val = variable_registry[var_key]
        if isinstance(reg_val, dict):
            if "value" in reg_val:
                return reg_val["value"]
            # For complex nested dictionaries, return the string representation
            return str(reg_val)
        return reg_val

    for reg_var_name, reg_var_value in variable_registry.items():
        if isinstance(reg_var_value, dict):
            # For dictionaries with a "value" key, use that
            if "value" in reg_var_value:
                extracted_value = reg_var_value["value"]
            # For other dictionaries, convert to string representation
            else:
                extracted_value = str(reg_var_value)
        else:
            extracted_value = reg_var_value
            
        # Replace the variable placeholder in the template
        output_value = output_value.replace(
            "<" + reg_var_name + ">", str(extracted_value)
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
    """
    Evaluate if a schema applies to a file based on its selector.
    """
    stack = [(selector, True)]  # (selector, is_and_context)
    final_result = True
    or_group_result = False  # Track results within OR groups
    
    while stack:
        current_selector, is_and_context = stack.pop()
        
        if isinstance(current_selector, str):
            parts = current_selector.split(":")
            if len(parts) != 3:
                raise ValueError(f"Invalid selector format: {current_selector}")
                
            select_target_name, select_function_name, select_argument = parts
            
            # Get the appropriate target value based on the selector
            if select_target_name == "filename":
                # Extract just the filename without path
                select_target_value = filename.split('/')[-1]
            elif select_target_name == "fullpath":
                # Use the complete path
                select_target_value = filename
            elif select_target_name == "dirname":
                # Check against directory components
                path_parts = filename.split('/')
                # Default to False; will be updated if a match is found
                result = False
                for part in path_parts[:-1]:  # Exclude the filename
                    if select_function_name == "starts_with" and part.startswith(select_argument):
                        result = True
                        break
                    elif select_function_name == "contains" and select_argument in part:
                        result = True
                        break
                
                # Apply the result directly for dirname
                if is_and_context:
                    final_result = final_result and result
                    if not final_result:  # Short-circuit AND
                        break
                else:
                    or_group_result = or_group_result or result
                    final_result = or_group_result
                continue  # Skip the standard processing below
            else:
                raise ValueError(f"Invalid target name {select_target_name}")
            
            # Standard processing for filename and fullpath
            if select_function_name == "starts_with":
                result = select_target_value.startswith(select_argument)
            elif select_function_name == "contains":
                result = select_argument in select_target_value
            else:
                raise ValueError(f"Invalid function name {select_function_name}")
                
            # Apply the result based on current context
            if is_and_context:
                final_result = final_result and result
                if not final_result:  # Short-circuit AND
                    break
            else:
                or_group_result = or_group_result or result
                final_result = or_group_result
            
        elif isinstance(current_selector, dict):
            for key, conditions in current_selector.items():
                if key == "or":
                    # Create a sub-evaluation for the OR group
                    or_result = False
                    or_stack = [(item, False, False) for item in reversed(conditions)]
                    
                    # Process OR conditions
                    while or_stack:
                        or_item, _, _ = or_stack.pop()
                        # Recursively evaluate this condition
                        item_result = _select_applicable_schema(or_item, filename)
                        or_result = or_result or item_result
                        if or_result:  # Short-circuit OR
                            break
                    
                    # Apply OR result to AND context
                    if is_and_context:
                        final_result = final_result and or_result
                        if not final_result:  # Short-circuit AND
                            return False
                    else:
                        return or_result
                        
                elif key == "and":
                    # Add AND conditions to stack
                    for item in reversed(conditions):
                        stack.append((item, True, False))
                else:
                    raise NotImplementedError(f"Invalid operator: {key}")
        else:
            raise ValueError(f"Invalid selector type: {type(current_selector)}")
            
    return final_result


def select_applicable_schema(
    nexus_file: pathlib.Path,
    schemas: OrderedDict[str, MetadataSchema],
) -> MetadataSchema:
    """
    Evaluates which metadata schema configuration is applicable to ``nexus_file``.

    Order of the schemas matters and first schema that is suitable is selected in priority.
    if other schemas are also suitable, keep only what is not in conflict.
    """
    result_schema = None
    for schema in schemas.values():
        if _select_applicable_schema(schema.selector, str(nexus_file)):
            if result_schema is None:
                result_schema = schema
            else:
                # Merge the schema
                result_schema.id += '_' + schema.id
                result_schema.name += ' & ' + schema.name
                result_schema.order = min(result_schema.order, schema.order)
                result_schema.selector = {
                    "and": [result_schema.selector, schema.selector]
                }
                for key, value in schema.variables.items():
                    if key not in result_schema.variables:
                        result_schema.variables[key] = value
                for key, value in schema.schema.items():
                    if key not in result_schema.schema:
                        result_schema.schema[key] = value
    if result_schema:
        return result_schema
    raise Exception("No applicable metadata schema configuration found!!")
