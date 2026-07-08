# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)

import json
import logging
import pathlib
from collections import OrderedDict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from typing import Any

import h5py
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
class SampleAttachmentConfig:
    query_sample_name: bool = field(
        default=False,
        metadata={
            "description": "If true, automatically find sample dataset PID from Scicat "
            "by querying the matching sample name found in the nexus file. "
            "Sample name and proposal ID must be present in the nexus file to complete the query"
        },
    )
    sample_name_path: str = field(
        default="/entry/sample/name",
        metadata={
            "description": "Path to the name of the sample encoded in the nexus file."
        },
    )
    proposal_id_path: str = field(
        default="/entry/experiment_identifier",
        metadata={"description": "Path to the proposal id encoded in the nexus file."},
    )

    def read_sample_name(
        self, *, file: h5py.File, logger: logging.Logger | None = None
    ) -> str | None:
        try:
            return file[self.sample_name_path][...].item().decode("utf-8")
        except Exception as err:
            if logger:
                logger.warning("Could not find sample name with error: %.2000s", err)
        return None

    def read_proposal_id(
        self, *, file: h5py.File, logger: logging.Logger | None = None
    ) -> str | None:
        try:
            return file[self.proposal_id_path][...].item().decode("utf-8")
        except Exception as err:
            if logger:
                logger.warning("Could not find proposal ID with error: %.2000s", err)
        return None

    def query_sample(
        self, *, file: h5py.File, scicat_config, logger: logging.Logger | None = None
    ) -> str | None: ...


@dataclass(kw_only=True)
class JobItemConfig:
    type: str
    job_params: dict[str, Any]
    owner_user: str
    owner_group: str
    contact_email: str

    def __post_init__(self) -> None:
        # validate job_params
        if not isinstance(self.job_params, dict) or any(
            isinstance(val, dict) for val in self.job_params.values()
        ):
            raise TypeError("job_params must be a single depth dictionary")

    def payload(self, variable_registry: dict) -> dict:
        rendered_job_params = {
            name: render_variable_value(val, variable_registry)
            for name, val in self.job_params.items()
        }
        return {
            "type": self.type,
            "jobParams": rendered_job_params,
            "ownerUser": self.owner_user,
            "ownerGroup": self.owner_group,
            "contactEmail": self.contact_email,
        }


@dataclass(kw_only=True)
class MetadataSchema:
    """Dataclass representation of IMSC(*.imsc.yml files) recipes."""

    id: str
    name: str
    instrument: str
    selector: str | dict
    order: int
    sample_attachment: SampleAttachmentConfig = field(
        default_factory=SampleAttachmentConfig
    )
    variables: dict[str, MetadataVariableConfig] = field(default_factory=dict)
    schema: dict[str, MetadataItemConfig] = field(default_factory=dict)
    jobs: dict[str, JobItemConfig] = field(default_factory=dict)

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
    def from_file(cls, schema_file_path: pathlib.Path) -> "MetadataSchema":
        return cls.from_dict(_load_schema_file(schema_file_path))

    def save_file(
        self, schema_file_path: pathlib.Path, *, exist_ok: bool = False
    ) -> None:
        from dataclasses import asdict
        from itertools import chain

        schema_dict = {k: v for k, v in asdict(self).items() if v is not None}
        for _config in chain(
            schema_dict['variables'].values(), schema_dict['schema'].values()
        ):
            null_keys = [vc_k for vc_k, vc_v in _config.items() if vc_v is None]
            for null_key in null_keys:
                _config.pop(null_key)

        if schema_file_path.exists() and (not exist_ok):
            raise FileExistsError(
                schema_file_path,
                'File already exist at the path. Set exist_ok=True to overwrite.',
            )
        with schema_file_path.open(mode='w') as file:
            yaml.safe_dump(schema_dict, file, sort_keys=False)


def _maybe_single_variable(val: str) -> bool:
    return val.startswith("<") and val.endswith(">")


def render_variable_value(
    var_value: str | dict | float,
    variable_registry: dict[str, MetadataVariableValueSpec],
) -> MetadataVariableValueSpec:
    def _render_item(_value) -> MetadataVariableValueSpec:
        # If it is only one variable, then it is a simple replacement
        if (
            _maybe_single_variable(_value)
            and (var_key := _value.removesuffix(">").removeprefix("<"))
            in variable_registry
        ):
            return variable_registry[var_key]

        # If it is a complex variable, then it is a combination of variables
        # similar to f-string in python
        # In this case, we do not forward unit.
        for reg_var_name, reg_var_value in variable_registry.items():
            _value = _value.replace("<" + reg_var_name + ">", str(reg_var_value.value))

        if "<" in _value and ">" in _value:
            raise Exception(f"Unresolved variable: {var_value}")

        return MetadataVariableValueSpec(value=_value)

    if isinstance(var_value, str):
        return _render_item(var_value)
    elif isinstance(var_value, dict):
        values = {
            _render_item(key).value: _render_item(value)
            for key, value in var_value.items()
        }
        units = {val.unit for val in values.values()}
        unit = next(iter(units)) if len(units) == 1 else ''
        return MetadataVariableValueSpec(
            value={key: value.value for key, value in values.items()}, unit=unit
        )
    elif isinstance(var_value, Iterable):
        values = [_render_item(value) for value in var_value]
        units = {val.unit for val in values}
        unit = next(iter(units)) if len(units) == 1 else ''
        return MetadataVariableValueSpec(value=[val.value for val in values], unit=unit)
    else:
        return _render_item(json.dumps(var_value))


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


_AVAILABLE_SELECTION_TARGET_NAMES = ('filename',)
_AVAILBALE_SELECTION_FUNCTION_NAMES = ('starts_with', 'contains')
_AVAILABLE_OPERATOR_NAMES = ('and', 'or')


def _select_applicable_schema(
    selector: str | dict,
    filename: str | None = None,
    *,
    logger: logging.Logger,
) -> bool:
    if isinstance(selector, str):
        if selector == '*':
            # Global selector. Should be applicable to any ingestions.
            return True

        # Regular selectors. i.e. filename:starts_with:/ess/data/coda
        if len(selector_args := selector.split(":")) != 3:
            return False
        select_target_name, select_function_name, select_argument = selector_args
        if select_target_name in ["filename"]:
            select_target_value = filename
        else:
            logger.warning(
                "Invalid target name %s. Use one of (%s)",
                select_target_name,
                ', '.join(_AVAILABLE_SELECTION_TARGET_NAMES),
            )
            return False

        if select_target_value is None:
            return False

        if select_function_name == "starts_with":
            return select_target_value.startswith(select_argument)
        elif select_function_name == "contains":
            return select_argument in select_target_value
        else:
            logger.warning(
                "Invalid function name %s. Use one of (%s)",
                select_function_name,
                ', '.join(_AVAILBALE_SELECTION_FUNCTION_NAMES),
            )
            return False

    elif isinstance(selector, dict):
        output = True
        for key, conditions in selector.items():
            if key == "or":
                output = output and any(
                    _select_applicable_schema(item, filename, logger=logger)
                    for item in conditions
                )
            elif key == "and":
                output = output and all(
                    _select_applicable_schema(item, filename, logger=logger)
                    for item in conditions
                )
            else:
                logger.warning(
                    "Invalid condition operator name: %s. Use one of (%s)",
                    key,
                    ', '.join(_AVAILABLE_OPERATOR_NAMES),
                )
        return output
    else:
        logger.warning("Invalid type for schema selector %s.", type(selector))

    return False


def select_applicable_schema(
    nexus_file: pathlib.Path,
    schemas: OrderedDict[str, MetadataSchema],
    *,
    fall_back_schema: MetadataSchema | None = None,
    logger: logging.Logger,
) -> MetadataSchema:
    """
    Evaluates which metadata schema configuration is applicable to ``nexus_file``.

    Order of the schemas matters and first schema that is suitable is selected.
    """
    for schema in schemas.values():
        if _select_applicable_schema(schema.selector, str(nexus_file), logger=logger):
            return schema

    if fall_back_schema is None:
        raise ValueError(
            "No applicable metadata schema is found and "
            "no fallback schema is given. Cannot determine the schema..."
        )

    logger.warning(
        "No applicable metadata schema found based on the selectors. "
        "Fallback schema will be used..."
    )

    return fall_back_schema
