# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import datetime
import logging
import pathlib
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from scicat_configuration import FileHandlingOptions


def to_string(value: Any) -> str:
    return str(value)


def to_string_array(value: list[Any]) -> list[str]:
    return [str(v) for v in value]


def to_integer(value: Any) -> int:
    return int(value)


def to_float(value: Any) -> float:
    return float(value)


def to_date(value: Any) -> str | None:
    if isinstance(value, str):
        return datetime.datetime.fromisoformat(value).isoformat()
    elif isinstance(value, int | float):
        return datetime.datetime.fromtimestamp(value, tz=datetime.UTC).isoformat()
    return None


_DtypeConvertingMap = MappingProxyType(
    {
        "string": to_string,
        "string[]": to_string_array,
        "integer": to_integer,
        "float": to_float,
        "date": to_date,
    }
)


def convert_to_type(input_value: Any, dtype_desc: str) -> Any:
    if (converter := _DtypeConvertingMap.get(dtype_desc)) is None:
        raise ValueError(
            "Invalid dtype description. Must be one of: ",
            "string, string[], integer, float, date.\nGot: {dtype_desc}",
        )
    return converter(input_value)


@dataclass(kw_only=True)
class TechniqueDesc:
    pid: str
    "Technique PID"
    names: str
    "Technique Name"


@dataclass(kw_only=True)
class ScicatDataset:
    pid: str
    datasetName: str
    description: str
    principalInvestigator: str
    creationLocation: str
    scientificMetadata: dict
    owner: str
    ownerEmail: str
    sourceFolder: str
    contactEmail: str
    creationTime: str
    type: str = "raw"
    techniques: list[TechniqueDesc] | None = None
    instrumentId: str
    sampleId: str
    proposalId: str
    ownerGroup: str
    accessGroup: list[str]


@dataclass(kw_only=True)
class DataFileListItem:
    path: str
    "Absolute path to the file."
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


def _calculate_checksum(file_path: pathlib.Path, algorithm_name: str) -> str | None:
    """Calculate the checksum of a file."""
    import hashlib

    if not file_path.exists():
        return None

    if algorithm_name != "b2blake":
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
    calculate_checksum: bool,
    compute_file_stats: bool,
    file_hash_algorithm: str = "",
) -> DataFileListItem:
    """``DataFileListItem`` constructing helper."""

    if file_path.exists() and compute_file_stats:
        return DataFileListItem(
            path=file_path.absolute().as_posix(),
            size=(file_stats := file_path.stat()).st_size,
            time=datetime.datetime.fromtimestamp(
                file_stats.st_ctime, tz=datetime.UTC
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            chk=_calculate_checksum(file_path, file_hash_algorithm)
            if calculate_checksum
            else None,
            uid=str(file_stats.st_uid),
            gid=str(file_stats.st_gid),
            perm=oct(file_stats.st_mode),
        )
    else:
        return DataFileListItem(
            path=file_path.absolute().as_posix(),
            time=datetime.datetime.now(tz=datetime.UTC).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            ),
        )


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
            calculate_checksum=config.compute_file_hash,
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
            if new_file_item.chk is not None:
                _save_hash_file(
                    original_file_instance=new_file_item, hash_path=hash_file_path
                )
                data_file_list.append(
                    single_file_constructor(
                        file_path=hash_file_path, calculate_checksum=False
                    )
                )
            else:
                logger.warning(
                    "File(%s) instance does not have checksum. "
                    "Probably the file does not exist. "
                    "Skip saving...",
                    minimum_file_path,
                )

    return data_file_list
