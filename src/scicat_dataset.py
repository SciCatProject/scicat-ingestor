# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import ast
import copy
import datetime
import logging
import os.path
import pathlib
import re
import urllib
import uuid
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from types import MappingProxyType
from typing import Any

import h5py
import numpy as np

from scicat_communication import render_full_url, retrieve_value_from_scicat
from scicat_configuration import (
    DatasetOptions,
    FileHandlingOptions,
    OfflineIngestorConfig,
)
from scicat_metadata import (
    HIGH_LEVEL_METADATA_TYPE,
    SCIENTIFIC_METADATA_TYPE,
    VALID_METADATA_TYPES,
    MetadataItemConfig,
    MetadataVariableConfig,
    MetadataVariableValueSpec,
    VariableConfigNexusFile,
    VariableConfigScicat,
    VariableConfigValue,
    render_variable_value,
)


def to_string(value: Any) -> str:
    return str(value)


def to_string_array(value: list[Any]) -> list[str]:
    return [
        str(v) for v in (ast.literal_eval(value) if isinstance(value, str) else value)
    ]


def to_integer(value: Any) -> int:
    return int(value)


def to_float(value: Any) -> float:
    return float(value)


def to_date(value: Any) -> str | None:
    if isinstance(value, str):
        return datetime.datetime.fromisoformat(value).isoformat()
    elif isinstance(value, int | float):
        return datetime.datetime.fromtimestamp(value, tz=datetime.UTC).isoformat()
    if isinstance(value, bytes):
        return datetime.datetime.fromisoformat(value.decode()).isoformat()
    return None


def to_dict(value: Any) -> dict:
    if isinstance(value, str):
        result = ast.literal_eval(value)
        if isinstance(result, dict):
            return result
        else:
            raise ValueError(
                "Invalid value. Must be able to convert to a dictionary. Got ", value
            )
    elif isinstance(value, dict):
        return value

    return dict(value)


def to_list(value: Any) -> list:
    if isinstance(value, str):
        result = ast.literal_eval(value)
        if isinstance(result, list):
            return result
        else:
            raise ValueError(
                "Invalid value. Must be able to convert to a dictionary. Got ", value
            )
    elif isinstance(value, list):
        return value
    else:
        raise TypeError()


_DtypeConvertingMap = MappingProxyType(
    {
        "string": to_string,
        "string[]": to_string_array,
        "integer": to_integer,
        "float": to_float,
        "date": to_date,
        "dict": to_dict,
        "list": to_list,
        "email": to_string,
        "link": to_string,
        # TODO: Add email converter
    }
)


def convert_to_type(input_value: Any, dtype_desc: str) -> Any:
    if (converter := _DtypeConvertingMap.get(dtype_desc)) is None:
        raise ValueError(
            "Invalid dtype description. Must be one of: ",
            "string, string[], integer, float, date.",
            f"Got: {dtype_desc}",
        )
    return converter(input_value)


def _do_nothing(
    value: MetadataVariableValueSpec, recipe: MetadataVariableConfig
) -> MetadataVariableValueSpec:
    """Do nothing operator."""
    _ = recipe  # Unused in this operator
    return value


def _join_with_space(
    value: MetadataVariableValueSpec, recipe: MetadataVariableConfig
) -> MetadataVariableValueSpec:
    """Join with space operator."""
    _ = recipe  # Unused in this operator
    _orig = value.value
    _value = ", ".join(ast.literal_eval(_orig) if isinstance(_orig, str) else _orig)
    return MetadataVariableValueSpec(value=_value)


def _filename(
    value: MetadataVariableValueSpec, recipe: MetadataVariableConfig
) -> MetadataVariableValueSpec:
    """Filename operator."""
    _ = recipe  # Unused in this operator
    return MetadataVariableValueSpec(value=os.path.basename(value.value))


def _dirname(
    value: MetadataVariableValueSpec, recipe: MetadataVariableConfig
) -> MetadataVariableValueSpec:
    """Dirname operator."""
    _ = recipe  # Unused in this operator
    return MetadataVariableValueSpec(value=os.path.dirname(value.value))


def _grandparents_dirname(
    value: MetadataVariableValueSpec, recipe: MetadataVariableConfig
) -> MetadataVariableValueSpec:
    """Grandparents dirname operator."""
    _ = recipe  # Unused in this operator
    return MetadataVariableValueSpec(
        value=os.path.dirname(os.path.dirname(value.value))
    )


def _getitem_from_variable_value(
    value: MetadataVariableValueSpec, recipe: VariableConfigValue
) -> MetadataVariableValueSpec:
    """Getitem operator."""
    _ = recipe  # Unused in this operator
    return MetadataVariableValueSpec(value=value.value[recipe.field])


def _str_replace(
    value: MetadataVariableValueSpec, recipe: VariableConfigValue
) -> MetadataVariableValueSpec:
    """String replace operator."""
    _ = recipe  # Unused in this operator
    if recipe.pattern is None or recipe.replacement is None:
        return MetadataVariableValueSpec(value=value.value)
    else:
        return MetadataVariableValueSpec(
            value=str(value.value).replace(recipe.pattern, recipe.replacement)
        )


def _url_safe(
    value: MetadataVariableValueSpec, recipe: VariableConfigValue
) -> MetadataVariableValueSpec:
    """URL safe operator."""
    _ = recipe  # Unused in this operator
    return MetadataVariableValueSpec(value=urllib.parse.quote_plus(value.value))


def _to_lower(
    value: MetadataVariableValueSpec, recipe: VariableConfigValue
) -> MetadataVariableValueSpec:
    """Convert to lower case operator."""
    _ = recipe  # Unused in this operator
    return MetadataVariableValueSpec(value=str(value.value).lower())


def _to_upper(
    value: MetadataVariableValueSpec, recipe: VariableConfigValue
) -> MetadataVariableValueSpec:
    """Convert to upper case operator."""
    _ = recipe  # Unused in this operator
    return MetadataVariableValueSpec(value=str(value.value).upper())


_OPERATOR_REGISTRY = MappingProxyType(
    {
        "DO_NOTHING": _do_nothing,
        "join_with_space": _join_with_space,
        # "evaluate": lambda value: ast.literal_eval(value),
        # We are not adding the evaluate function here since
        # ``evaluate`` function should be avoided if possible.
        # It might seem easy to use, but it is very easy to break
        # when the input is not as expected.
        # It is better to use the specific converters for the types.
        # However, if it is the only way to go, you can add it here.
        # Please add a comment to explain why it is needed.
        "filename": _filename,
        "dirname": _dirname,
        "dirname-2": _grandparents_dirname,
        "getitem": _getitem_from_variable_value,
        "str-replace": _str_replace,
        "urlsafe": _url_safe,
        "to-lower": _to_lower,
        "to-upper": _to_upper,
    }
)
"""Operator should accept ``MetadataVariableValueSpec`` and ``VariableConfigValue`` as arguments.

It is for propagating unit information.
For example, if we add a new operator that might affect unit,
it should also take care of the unit conversion.

"""


def _get_operator(
    operator: str | None,
) -> Callable[
    [MetadataVariableValueSpec, VariableConfigValue], MetadataVariableValueSpec
]:
    return _OPERATOR_REGISTRY.get(operator or "DO_NOTHING", _do_nothing)


def _retrieve_as_string(
    h5file: h5py.File, path: str, *, encoding: str = "utf-8"
) -> str:
    return h5file[path][...].item().decode(encoding)


def _retrieve_unit(h5file: h5py.File, path: str) -> str:
    return h5file[path].attrs.get("units", None)


def _retrieve_values_from_file(
    variable_recipe: VariableConfigNexusFile, h5file: h5py.File
) -> MetadataVariableValueSpec:
    _vt = variable_recipe.value_type
    if _vt == "string[]" and ("*" in variable_recipe.path):  # Selectors are used
        path = variable_recipe.path.split("/")[1:]
        paths = extract_paths_from_h5_file(h5file, path)
        value = [_retrieve_as_string(h5file, p) for p in paths]
        unit = None  # No unit retrieval for a value from a selector
    else:
        if _vt == "string":
            value = _retrieve_as_string(h5file, variable_recipe.path)
        else:
            value = h5file[variable_recipe.path][...]

        unit = _retrieve_unit(h5file, variable_recipe.path)

    if unit is None:
        # Overwrite hardcoded unit with variable configuration unit
        unit = variable_recipe.unit

    if (
        "[]" not in variable_recipe.value_type
    ):
        if (
            isinstance(value, np.ndarray)
            and value.ndim == 0
            and value.size == 1
        ):
            value = value.item()
        elif (
            isinstance(value, list | np.ndarray | tuple)
            and len(value) == 1
        ):  # Supposed to be scalar value
            value = value[0]

    return MetadataVariableValueSpec(value=value, unit=unit)


def _build_default_variable_map(
    *,
    nexus_file_path: pathlib.Path,
    ingestor_file_dir: str,
    schema_id: str,
) -> dict[str, MetadataVariableValueSpec]:
    from functools import partial

    value_spec_no_unit = partial(MetadataVariableValueSpec, unit="")
    return {
        "ingestor_run_id": value_spec_no_unit(value=str(uuid.uuid4())),
        "data_file_path": value_spec_no_unit(value=nexus_file_path.as_posix()),
        "data_file_name": value_spec_no_unit(value=str(nexus_file_path.name)),
        "now": value_spec_no_unit(
            value=datetime.datetime.now(tz=datetime.UTC).isoformat()
        ),
        "ingestor_files_directory": value_spec_no_unit(value=ingestor_file_dir),
        "ingestor_metadata_schema_id": value_spec_no_unit(value=schema_id),
    }


def extract_variables_values(
    variables: dict[str, MetadataVariableConfig],
    h5file: h5py.File,
    config: OfflineIngestorConfig,
    schema_id: str,
) -> dict[str, MetadataVariableValueSpec]:
    variable_map: dict[str, MetadataVariableValueSpec] = _build_default_variable_map(
        nexus_file_path=pathlib.Path(config.nexus_file),
        ingestor_file_dir=config.ingestion.file_handling.ingestor_files_directory,
        schema_id=schema_id,
    )
    for variable_name, variable_recipe in variables.items():
        if isinstance(variable_recipe, VariableConfigNexusFile):
            value = _retrieve_values_from_file(variable_recipe, h5file)
        elif isinstance(variable_recipe, VariableConfigScicat):
            url_template = render_variable_value(
                variable_recipe.url, variable_map
            ).value
            if not isinstance(url_template, str):
                raise ValueError(f"Invalid URL template: {url_template}")

            full_endpoint_url = render_full_url(url_template, config.scicat)
            _value = retrieve_value_from_scicat(
                config=config.scicat,
                scicat_endpoint_url=full_endpoint_url,
                field_name=variable_recipe.field,
            )
            # Unit is not retrieved from scicat
            value = MetadataVariableValueSpec(value=_value)
        elif isinstance(variable_recipe, VariableConfigValue):
            value = render_variable_value(variable_recipe.value, variable_map)
            _operator = _get_operator(variable_recipe.operator)
            value = _operator(value, variable_recipe)

        else:
            raise Exception("Invalid variable source: ", variable_recipe.source)

        # Convert dtype to match the target value_type.
        variable_map[variable_name] = MetadataVariableValueSpec(
            value=convert_to_type(value.value, variable_recipe.value_type),
            unit=value.unit,
        )

    return variable_map


def extract_paths_from_h5_file(
    _h5_object: h5py.Group | h5py.File,
    _path: list[str],
) -> list[str]:
    master_key = _path.pop(0)
    output_paths = []
    if "*" in master_key:
        temp_keys = [
            k2
            for k2 in _h5_object.keys()
            if master_key == "*" or re.search(master_key, k2)
        ]
        for key in temp_keys:
            output_paths += [
                key + "/" + subkey
                for subkey in extract_paths_from_h5_file(
                    _h5_object[key], copy.deepcopy(_path)
                )
            ]
    else:
        if _path:
            output_paths = [
                master_key + "/" + subkey
                for subkey in extract_paths_from_h5_file(_h5_object[master_key], _path)
            ]
        else:
            output_paths = [master_key]

    return output_paths


@dataclass(kw_only=True)
class TechniqueDesc:
    pid: str
    "Technique PID"
    name: str
    "Technique Name"


@dataclass(kw_only=True)
class ScicatDataset:
    pid: str | None
    size: int
    numberOfFiles: int
    isPublished: bool = False
    datasetName: str
    description: str | None = None
    principalInvestigator: str
    creationLocation: str
    scientificMetadata: dict
    owner: str
    ownerEmail: str
    sourceFolder: str
    contactEmail: str
    creationTime: str
    type: str = "raw"
    sampleId: str | None = None
    techniques: list[TechniqueDesc] = field(default_factory=list)
    instrumentId: str | None = None
    proposalId: str | None = None
    ownerGroup: str | None = None
    accessGroups: list[str] | None = None
    startTime: str | None = None
    endTime: str | None = None
    runNumber: str | None = None
    keywords: list[str] | None = None


@dataclass(kw_only=True)
class DataFileListItem:
    path: str
    "Relative path of the file to the source folder."
    size: int | None = None
    "Size of the single file in bytes."
    time: str
    chk: str | None = None
    uid: str | None = None
    gid: str | None = None
    perm: str | None = None


@dataclass(kw_only=True)
class OrigDataBlockInstance:
    datasetId: str
    size: int
    chkAlg: str
    dataFileList: list[DataFileListItem]
    ownerGroup: str | None = None
    accessGroups: list[str] | None = None


def _calculate_checksum(file_path: pathlib.Path, algorithm_name: str) -> str | None:
    """Calculate the checksum of a file."""
    import hashlib

    if not file_path.exists():
        return None

    if algorithm_name != "blake2b":
        raise ValueError(
            "Only b2blake hash algorithm is supported for now. Got: ",
            f"{algorithm_name}",
        )

    chk = hashlib.new(algorithm_name, usedforsecurity=False)
    buffer = memoryview(bytearray(128 * 1024))
    with open(file_path, "rb", buffering=0) as file:
        for n in iter(lambda: file.readinto(buffer), 0):
            chk.update(buffer[:n])

    return chk.hexdigest()


def _create_single_data_file_list_item(
    *,
    file_path: pathlib.Path,
    compute_file_hash: bool,
    compute_file_stats: bool,
    file_hash_algorithm: str = "",
) -> DataFileListItem:
    """``DataFileListItem`` constructing helper."""

    file_info: dict[str, Any] = {
        "path": file_path.absolute().as_posix(),
        "time": datetime.datetime.now(tz=datetime.UTC).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        ),
    }
    if file_path.exists():
        if compute_file_stats:
            file_stats = file_path.stat()
            timestamp_str = datetime.datetime.fromtimestamp(
                file_stats.st_ctime, tz=datetime.UTC
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            file_info = {
                **file_info,
                **{
                    "size": file_stats.st_size,
                    "time": timestamp_str,
                    "uid": str(file_stats.st_uid),
                    "gid": str(file_stats.st_gid),
                    "perm": oct(file_stats.st_mode),
                },
            }

        if compute_file_hash:
            file_info["chk"] = _calculate_checksum(file_path, file_hash_algorithm)

    return DataFileListItem(**file_info)


def _build_hash_path(
    *,
    original_file_instance: DataFileListItem,
    dir_path: pathlib.Path,
    hash_file_extension: str,
) -> pathlib.Path:
    "Compose path to the hash file."
    file_stem = pathlib.Path(original_file_instance.path).stem
    return dir_path / pathlib.Path(".".join([file_stem, hash_file_extension]))


def _save_hash_file(
    *,
    original_file_instance: DataFileListItem,
    hash_path: pathlib.Path,
) -> None:
    """Save the hash of the ``original_file_instance``."""
    if original_file_instance.chk is None:
        raise ValueError("Checksum is not provided.")

    hash_path.write_text(original_file_instance.chk)


def create_data_file_list(
    *,
    nexus_file: pathlib.Path,
    done_writing_message_file: pathlib.Path | None = None,
    nexus_structure_file: pathlib.Path | None = None,
    ingestor_directory: pathlib.Path,
    config: FileHandlingOptions,
    source_folder: pathlib.Path | str | None = None,
    logger: logging.Logger,
) -> list[DataFileListItem]:
    """
    Create a list of ``DataFileListItem`` instances for the files provided.

    Params
    ------
    nexus_file:
        Path to the NeXus file.
    done_writing_message_file:
        Path to the "done writing" message file.
    nexus_structure_file:
        Path to the NeXus structure file.
    ingestor_directory:
        Path to the directory where the files will be saved.
    config:
        Configuration related to the file handling.
    logger:
        Logger instance.

    """
    from functools import partial

    single_file_constructor = partial(
        _create_single_data_file_list_item,
        file_hash_algorithm=config.file_hash_algorithm,
        compute_file_stats=config.compute_file_stats,
        compute_file_hash=config.compute_file_hash,
    )

    # Collect the files that will be ingested
    file_list = [nexus_file]
    if done_writing_message_file is not None:
        file_list.append(done_writing_message_file)
    if nexus_structure_file is not None:
        file_list.append(nexus_structure_file)

    # Create the list of the files
    data_file_list = []
    for minimum_file_path in file_list:
        logger.info("Adding file %s to the datafiles list", minimum_file_path)
        new_file_item = single_file_constructor(
            file_path=minimum_file_path,
        )
        data_file_list.append(new_file_item)
        if config.save_file_hash:
            logger.info(
                "Computing hash of the file(%s) from disk...", minimum_file_path
            )
            hash_file_path = _build_hash_path(
                original_file_instance=new_file_item,
                dir_path=ingestor_directory,
                hash_file_extension=config.hash_file_extension,
            )
            logger.info("Saving hash into a file ... %s", hash_file_path)
            _save_hash_file(
                original_file_instance=new_file_item, hash_path=hash_file_path
            )
            data_file_list.append(
                single_file_constructor(
                    file_path=hash_file_path, compute_file_hash=False
                )
            )
        if source_folder and config.file_path_type == "relative":
            for data_file in data_file_list:
                data_file.path = str(
                    pathlib.Path(data_file.path).relative_to(source_folder)
                )

    return data_file_list


def _filter_by_field_type(
    schemas: Iterable[MetadataItemConfig], field_type: str
) -> list[MetadataItemConfig]:
    return [field for field in schemas if field.field_type == field_type]


def _render_variable_as_type(
    value: Any, variable_map: dict[str, MetadataVariableValueSpec], dtype: str
) -> Any:
    return convert_to_type(render_variable_value(value, variable_map).value, dtype)


@dataclass(kw_only=True)
class MetadataItemValueSpec:
    value: Any
    type: str
    human_name: str
    unit: str

    @classmethod
    def from_metadata_item_config(
        cls,
        item_config: MetadataItemConfig,
        variable_map: dict[str, MetadataVariableValueSpec],
    ) -> "MetadataItemValueSpec":
        value = render_variable_value(item_config.value, variable_map)
        return cls(
            value=convert_to_type(value.value, item_config.type),
            unit=item_config.unit or value.unit,
            human_name=item_config.human_name or item_config.machine_name,
            type=item_config.type,
        )


def _create_scientific_metadata(
    *,
    sm_schemas: list[MetadataItemConfig],
    variable_map: dict[str, MetadataVariableValueSpec],
) -> dict:
    """Create scientific metadata from the metadata schema configuration.

    Params
    ------
    metadata_schema_id:
        The ID of the metadata schema configuration.
    sm_schemas:
        The scientific metadata schema configuration.
    variable_map:
        The variable map to render the scientific metadata values.

    """
    metadatas = {
        item_config.machine_name: MetadataItemValueSpec.from_metadata_item_config(
            item_config, variable_map
        )
        for item_config in sm_schemas
    }
    return {name: asdict(meta) for name, meta in metadatas.items()}


def _validate_metadata_schemas(
    metadata_schema: dict[str, MetadataItemConfig],
) -> None:
    invalid_types = [
        field.field_type
        for field in metadata_schema.values()
        if field.field_type not in VALID_METADATA_TYPES
    ]

    if any(invalid_types):
        raise ValueError(
            "Invalid metadata schema types found. Valid types are: ",
            VALID_METADATA_TYPES,
            "Got: ",
            invalid_types,
        )


def create_scicat_dataset_instance(
    *,
    metadata_schema: dict[str, MetadataItemConfig],  # metadata-schema["schema"]
    variable_map: dict[str, MetadataVariableValueSpec],
    data_file_list: list[DataFileListItem],
    config: DatasetOptions,
    logger: logging.Logger,
) -> ScicatDataset:
    """
    Prepare the ``ScicatDataset`` instance.

    Params
    ------
    metadata_schema:
        Metadata schema.
    variables_values:
        Variables values.
    data_file_list:
        List of the data files.
    config:
        Configuration related to scicat dataset instance.
    logger:
        Logger instance.

    """
    _validate_metadata_schemas(metadata_schema)
    # Create the dataset instance
    scicat_dataset = ScicatDataset(
        size=sum([file.size for file in data_file_list if file.size is not None]),
        numberOfFiles=len(data_file_list),
        isPublished=False,
        scientificMetadata=_create_scientific_metadata(
            sm_schemas=_filter_by_field_type(
                metadata_schema.values(), SCIENTIFIC_METADATA_TYPE
            ),  # Scientific metadata schemas
            variable_map=variable_map,
        ),
        **{
            field.machine_name: _render_variable_as_type(
                field.value, variable_map, field.type
            )
            for field in _filter_by_field_type(
                metadata_schema.values(), HIGH_LEVEL_METADATA_TYPE
            )
            # High level schemas
        },
    )

    # Auto generate or assign default values if needed
    if not config.allow_dataset_pid and scicat_dataset.pid:
        logger.info("PID is not allowed in the dataset by configuration.")
        scicat_dataset.pid = None
    elif config.generate_dataset_pid:
        logger.info("Auto generating PID for the dataset based on the configuration.")
        scicat_dataset.pid = uuid.uuid4().hex
    if scicat_dataset.instrumentId is None:
        scicat_dataset.instrumentId = config.default_instrument_id
        logger.info(
            "Instrument ID is not provided. Setting to default value. %s",
            scicat_dataset.instrumentId,
        )
    if scicat_dataset.proposalId is None:
        scicat_dataset.proposalId = config.default_proposal_id
        logger.info(
            "Proposal ID is not provided. Setting to default value. %s",
            scicat_dataset.proposalId,
        )
    if scicat_dataset.ownerGroup is None:
        scicat_dataset.ownerGroup = config.default_owner_group
        logger.info(
            "Owner group is not provided. Setting to default value. %s",
            scicat_dataset.ownerGroup,
        )
    if scicat_dataset.accessGroups is None:
        scicat_dataset.accessGroups = config.default_access_groups
        logger.info(
            "Access group is not provided. Setting to default value. %s",
            scicat_dataset.accessGroups,
        )

    logger.info("Dataset instance is created successfully. %s", scicat_dataset)
    return scicat_dataset


def scicat_dataset_to_dict(dataset: ScicatDataset) -> dict:
    """
    Convert the ``dataset`` to a dictionary.

    It removes the ``None`` values from the dictionary.
    You can add more handlings for specific fields here if needed.

    Params
    ------
    dataset:
        Scicat dataset instance.

    """
    return {k: v for k, v in asdict(dataset).items() if v is not None}


def _define_dataset_source_folder(
    datafilelist: list[DataFileListItem],
    data_file_path: pathlib.Path,
    source_folder_config: str = "common_path",
) -> pathlib.Path | None:
    """
    Return the dataset source folder, which is the common path
    between all the data files associated with the dataset
    """
    import os

    if source_folder_config == "data_file":
        return pathlib.Path(os.path.dirname(data_file_path))
    elif source_folder_config == "common_path":
        return pathlib.Path(os.path.commonpath([item.path for item in datafilelist]))
    else:
        return None


def _path_to_relative(
    datafilelist_item: DataFileListItem,
    dataset_source_folder: pathlib.Path,
    file_path_type: str = "relative",
) -> DataFileListItem:
    """
    Copy the datafiles item and transform the path to the relative path
    to the dataset source folder
    """
    from copy import copy

    origdatablock_datafilelist_item = copy(datafilelist_item)
    if file_path_type == "relative":
        origdatablock_datafilelist_item.path = (
            pathlib.Path(datafilelist_item.path)
            .relative_to(dataset_source_folder)
            .as_posix()
        )
    return origdatablock_datafilelist_item


def _prepare_origdatablock_datafilelist(
    datafiles_list: list[DataFileListItem],
    dataset_source_folder: pathlib.Path,
    file_path_type: str = "relative",
) -> list[DataFileListItem]:
    """
    Prepare the datafiles list for the origdatablock entry in scicat
    That means that the file paths needs to be relative to the dataset source folder
    """
    return [
        _path_to_relative(item, dataset_source_folder, file_path_type)
        for item in datafiles_list
    ]


def create_origdatablock_instance(
    data_file_list: list[DataFileListItem],
    scicat_dataset: dict,
    config: FileHandlingOptions,
) -> OrigDataBlockInstance:
    origdatablock_datafiles_list = _prepare_origdatablock_datafilelist(
        data_file_list, scicat_dataset["sourceFolder"], config.file_path_type
    )
    return OrigDataBlockInstance(
        datasetId=scicat_dataset["pid"],
        size=sum([item.size for item in data_file_list if item.size is not None]),
        chkAlg=config.file_hash_algorithm,
        dataFileList=origdatablock_datafiles_list,
        ownerGroup=scicat_dataset["ownerGroup"],
        accessGroups=scicat_dataset["accessGroups"],
    )


def origdatablock_to_dict(origdatablock: OrigDataBlockInstance) -> dict:
    """
    Convert the ``origdatablock`` to a dictionary.

    It removes the ``None`` values from the dictionary.
    You can add more handlings for specific fields here if needed.

    Params
    ------
    origdatablock:
        Origdatablock instance to be sent to scicat backend.

    """
    return {k: v for k, v in asdict(origdatablock).items() if v is not None}
