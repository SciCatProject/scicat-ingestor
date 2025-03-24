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
import logging

import h5py
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
    MetadataItem,
    MetadataSchemaVariable,
    NexusFileMetadataVariable,
    ScicatMetadataVariable,
    ValueMetadataVariable,
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
        try:
            # Try ISO format first
            return datetime.datetime.fromisoformat(value).isoformat()
        except ValueError:
            try:
                # Try custom format "dd-MMM-yy HH:mm:ss"
                return datetime.datetime.strptime(value, "%d-%b-%y %H:%M:%S").replace(tzinfo=datetime.UTC).isoformat()
            except ValueError:
                return None
    elif isinstance(value, int | float):
        return datetime.datetime.fromtimestamp(value, tz=datetime.UTC).isoformat()
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


_OPERATOR_REGISTRY = MappingProxyType(
    {
        "DO_NOTHING": lambda value, recipe: value,
        "join_with_space": lambda value, recipe: ", ".join(
            ast.literal_eval(value) if isinstance(value, str) else value
        ),
        # "evaluate": lambda value: ast.literal_eval(value),
        # We are not adding the evaluate function here since
        # ``evaluate`` function should be avoided if possible.
        # It might seem easy to use, but it is very easy to break
        # when the input is not as expected.
        # It is better to use the specific converters for the types.
        # However, if it is the only way to go, you can add it here.
        # Please add a comment to explain why it is needed.
        "filename": lambda value, recipe: os.path.basename(value),
        "dirname": lambda value, recipe: os.path.dirname(value),
        "dirname-2": lambda value, recipe: os.path.dirname(os.path.dirname(value)),
        "getitem": lambda value, recipe: value[
            recipe.field
        ],  # The only operator that takes an argument
        "str-replace": lambda value, recipe: str(value).replace(
            recipe.pattern, recipe.replacement
        ),
        "urlsafe": lambda value, recipe: urllib.parse.quote_plus(value),
        "to-lower": lambda value, recipe: str(value).lower(),
        "to-upper": lambda value, recipe: str(value).upper(),
    }
)


def _get_operator(operator: str | None) -> Callable:
    return _OPERATOR_REGISTRY.get(operator or "DO_NOTHING", lambda _: _)


def _retrieve_value_and_unit(
    h5file: h5py.File, path: str, *, encoding: str = "utf-8"
) -> tuple[Any, str | None]:
    """Retrieve both value and unit (if available) from an HDF5 dataset."""
    dataset = h5file[path]
    value = dataset[...].item()
    
    # Convert value to string if it's bytes
    value_str = value.decode(encoding) if isinstance(value, bytes) else value
    
    # Check for unit attributes (common patterns in scientific HDF5 files)
    unit = None
    attr_name = "units"
    if attr_name in dataset.attrs:
        unit_val = dataset.attrs[attr_name]
        # Handle bytes unit values
        unit = unit_val.decode(encoding) if isinstance(unit_val, bytes) else str(unit_val)
            
    return value_str, unit

def _retrieve_values_from_file(
    variable_recipe: NexusFileMetadataVariable, h5file: h5py.File
) -> Any:
    """Retrieve values from file, with unit support and multi possible paths support."""
    if isinstance(variable_recipe.path, list):
        value = None
        unit = None
        for path in variable_recipe.path:
            try:
                # Create a temporary variable recipe with a single path
                temp_recipe = copy.copy(variable_recipe)
                temp_recipe.path = path
                
                # Try to retrieve with this path
                value, unit = _retrieve_values_from_file(temp_recipe, h5file)
                
                # If we got a value, use it and stop trying other paths
                if value is not None:
                    return value, unit
            except (KeyError, Exception) as e:
                logging.debug("Path %s not found or error: %s", path, str(e))
                continue
                
        # If we get here, none of the paths worked
        logging.warning(
            "None of the specified paths found in file: %s",
            variable_recipe.path
        )
        return None, None

    unit = None
    if "*" in variable_recipe.path:
        path = variable_recipe.path.split("/")[1:]
        path[0] += "/"
        paths = extract_paths_from_h5_file(h5file, path[:])
        if variable_recipe.value_type == "dict":
            result = {}
            for p in paths:
                parts = p.split("/")[len(path):]
                
                current_result = result
                for i, part in enumerate(parts[:-1]):
                    if part not in current_result:
                        current_result[part] = {}
                    current_result = current_result[part]
                    
                if parts:
                    value, unit = _retrieve_value_and_unit(h5file, p)
                    current_result[parts[-1]] = {
                        "value": value,
                    }
                    if unit:
                        current_result[parts[-1]]["unit"] = unit
            value = result
        else:
            values_and_units = [_retrieve_value_and_unit(h5file, p) for p in paths]
            value = [v for v, _ in values_and_units]
            unit = [u for _, u in values_and_units if u]
    else:
        try:
            value, unit = _retrieve_value_and_unit(h5file, variable_recipe.path)
        except KeyError:
            value = None
            logging.warning(
                "Key %s not found in the file: %s",
                variable_recipe.path,
                h5file.filename
            )
    return value.strip() if isinstance(value, str) else value, unit


def extract_variables_values(
    nexus_file: pathlib.Path,
    variables: dict[str, MetadataSchemaVariable],
    h5file: h5py.File,
    config: OfflineIngestorConfig,
    schema_id: str,
) -> dict:
    variable_map = {
        "ingestor_run_id": str(uuid.uuid4()),
        "data_file_path": str(nexus_file),
        "data_file_name": str(nexus_file.name),
        "now": datetime.datetime.now(tz=datetime.UTC).isoformat(),
        "ingestor_files_directory": config.ingestion.file_handling.ingestor_files_directory,
        "ingestor_metadata_schema_id": schema_id,
    }
    for variable_name, variable_recipe in variables.items():
        source = variable_recipe.source
        unit = None
        if isinstance(variable_recipe, NexusFileMetadataVariable):
            value, unit = _retrieve_values_from_file(variable_recipe, h5file)
        elif isinstance(variable_recipe, ScicatMetadataVariable):
            full_endpoint_url = render_full_url(
                render_variable_value(variable_recipe.url, variable_map),
                config.scicat,
            )
            value = retrieve_value_from_scicat(
                config=config.scicat,
                scicat_endpoint_url=full_endpoint_url,
                field_name=variable_recipe.field,
            )
        elif isinstance(variable_recipe, ValueMetadataVariable):
            value = variable_recipe.value
            value = render_variable_value(value, variable_map)
            _operator = _get_operator(variable_recipe.operator)
            value = _operator(value, variable_recipe)

        else:
            raise Exception("Invalid variable source: ", source)
        variable_map[variable_name] = {
            "value": convert_to_type(value, variable_recipe.value_type)
        }
        if unit is not None:
            variable_map[variable_name] = {
                "value": convert_to_type(value, variable_recipe.value_type),
                "unit": unit,
            }
        else:
            variable_map[variable_name] = convert_to_type(value, variable_recipe.value_type)

    return variable_map


def extract_paths_from_h5_file(
    _h5_object: h5py.Group | h5py.File,
    _path: list[str],
) -> list[str]:
    master_key = _path.pop(0)
    output_paths = []

    if "*" in master_key:
        regex_pattern = master_key.replace("*", ".*")
        temp_keys = [k2 for k2 in _h5_object.keys() if re.search(regex_pattern, k2)]
        for key in temp_keys:
            if _h5_object[key].__class__ == h5py.Group:
                output_paths += [
                    key + "/" + subkey
                    for subkey in extract_paths_from_h5_file(
                        _h5_object[key], copy.deepcopy(_path + [master_key])
                    )
                ]
            elif _path:
                output_paths += [
                    key + "/" + subkey
                    for subkey in extract_paths_from_h5_file(
                        _h5_object[key], copy.deepcopy(_path)
                    )
                ]
            else:
                output_paths.append(key)
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
        logger.debug("Adding file %s to the datafiles list", minimum_file_path)
        new_file_item = single_file_constructor(
            file_path=minimum_file_path,
        )
        data_file_list.append(new_file_item)
        if config.save_file_hash:
            logger.debug(
                "Computing hash of the file(%s) from disk...", minimum_file_path
            )
            hash_file_path = _build_hash_path(
                original_file_instance=new_file_item,
                dir_path=ingestor_directory,
                hash_file_extension=config.hash_file_extension,
            )
            logger.debug("Saving hash into a file ... %s", hash_file_path)
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
    schemas: Iterable[MetadataItem], field_type: str
) -> list[MetadataItem]:
    return [field for field in schemas if field.field_type == field_type]


def _render_variable_as_type(value: Any, variable_map: dict, dtype: str) -> Any:
    return convert_to_type(render_variable_value(value, variable_map), dtype)


def _create_scientific_metadata(
    *,
    sm_schemas: list[MetadataItem],
    variable_map: dict,
) -> dict:
    """Create scientific metadata from the metadata schema configuration."""
    result = {}
    
    for field in sm_schemas:
        machine_name = field.machine_name
        rendered_value = _render_variable_as_type(field.value, variable_map, field.type)
        
        # Safe way to get unit that works for both dict and primitive values
        unit = ""
        if isinstance(rendered_value, dict) and not machine_name in variable_map:
            result[machine_name] = {
                "value": rendered_value,
                "human_name": getattr(field, "human_name", field.machine_name),
                "type": field.type,
            }
            for subfield in rendered_value.keys():
                var_value = variable_map.get(rendered_value[subfield]['machine_name'], {})
                if isinstance(var_value, dict) and "unit" in var_value:
                    rendered_value[subfield]["unit"] = var_value.get("unit", "")
        else:
            var_value = variable_map.get(field.machine_name, {})
            if isinstance(var_value, dict) and "unit" in var_value:
                unit = var_value.get("unit", "")
            
            result[machine_name] = {
                "value": rendered_value,
                "unit": unit,
                "human_name": getattr(field, "human_name", field.machine_name),
                "type": field.type,
            }
    
    return result


def _validate_metadata_schemas(
    metadata_schema: dict[str, MetadataItem],
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
    metadata_schema: dict[str, MetadataItem],  # metadata-schema["schema"]
    variable_map: dict,
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
        logger.debug("PID is not allowed in the dataset by configuration.")
        scicat_dataset.pid = None
    elif config.generate_dataset_pid:
        logger.debug("Auto generating PID for the dataset based on the configuration.")
        scicat_dataset.pid = uuid.uuid4().hex
    if scicat_dataset.instrumentId is None:
        scicat_dataset.instrumentId = config.default_instrument_id
        logger.debug(
            "Instrument ID is not provided. Setting to default value. %s",
            scicat_dataset.instrumentId,
        )
    if scicat_dataset.proposalId is None:
        scicat_dataset.proposalId = config.default_proposal_id
        logger.debug(
            "Proposal ID is not provided. Setting to default value. %s",
            scicat_dataset.proposalId,
        )
    if scicat_dataset.ownerGroup is None:
        scicat_dataset.ownerGroup = config.default_owner_group
        logger.debug(
            "Owner group is not provided. Setting to default value. %s",
            scicat_dataset.ownerGroup,
        )
    if scicat_dataset.accessGroups is None:
        scicat_dataset.accessGroups = config.default_access_groups
        logger.debug(
            "Access group is not provided. Setting to default value. %s",
            scicat_dataset.accessGroups,
        )

    logger.debug("Dataset instance is created successfully. %s", scicat_dataset)
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
