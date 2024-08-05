# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import datetime
import json
import pathlib
from types import MappingProxyType
from typing import Any

from scicat_configuration import FileHandlingOptions
from scicat_schemas import (
    load_datafilelist_item_schema_template,
    load_dataset_schema_template,
    load_origdatablock_schema_template,
)


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


def build_dataset_instance(
    *,
    dataset_pid_prefix: str,
    nxs_dataset_pid: str,
    dataset_name: str,
    dataset_description: str,
    principal_investigator: str,
    facility: str,
    environment: str,
    scientific_metadata: str,
    owner: str,
    owner_email: str,
    source_folder: str,
    contact_email: str,
    iso_creation_time: str,
    technique_pid: str,
    technique_name: str,
    instrument_id: str,
    sample_id: str,
    proposal_id: str,
    owner_group: str,
    access_groups: list[str],
) -> str:
    return load_dataset_schema_template().render(
        dataset_pid_prefix=dataset_pid_prefix,
        nxs_dataset_pid=nxs_dataset_pid,
        dataset_name=dataset_name,
        dataset_description=dataset_description,
        principal_investigator=principal_investigator,
        facility=facility,
        environment=environment,
        scientific_metadata=scientific_metadata,
        owner=owner,
        owner_email=owner_email,
        source_folder=source_folder,
        contact_email=contact_email,
        iso_creation_time=iso_creation_time,
        technique_pid=technique_pid,
        technique_name=technique_name,
        instrument_id=instrument_id,
        sample_id=sample_id,
        proposal_id=proposal_id,
        owner_group=owner_group,
        access_groups=access_groups,
    )


def build_single_datafile_instance(
    *,
    file_absolute_path: str,
    file_size: int,
    datetime_isoformat: str,
    uid: str,
    gid: str,
    perm: str,
    checksum: str = "",
) -> str:
    return load_datafilelist_item_schema_template().render(
        file_absolute_path=file_absolute_path,
        file_size=file_size,
        datetime_isoformat=datetime_isoformat,
        checksum=checksum,
        uid=uid,
        gid=gid,
        perm=perm,
    )


def build_origdatablock_instance(
    *,
    dataset_pid_prefix: str,
    nxs_dataset_pid: str,
    dataset_size: int,
    check_algorithm: str,
    data_file_desc_list: list[str],
) -> str:
    return load_origdatablock_schema_template().render(
        dataset_pid_prefix=dataset_pid_prefix,
        nxs_dataset_pid=nxs_dataset_pid,
        dataset_size=dataset_size,
        check_algorithm=check_algorithm,
        data_file_desc_list=data_file_desc_list,
    )


def _calculate_checksum(file_path: pathlib.Path, algorithm_name: str) -> str:
    """Calculate the checksum of a file."""
    import hashlib

    if not algorithm_name == "b2blake":
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


def build_single_data_file_desc(
    file_path: pathlib.Path, config: FileHandlingOptions
) -> dict[str, Any]:
    """
    Build the description of a single data file.
    """
    single_file_template = load_datafilelist_item_schema_template()

    return json.loads(
        single_file_template.render(
            file_absolute_path=file_path.absolute(),
            file_size=(file_stats := file_path.stat()).st_size,
            datetime_isoformat=datetime.datetime.fromtimestamp(
                file_stats.st_ctime, tz=datetime.UTC
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            chk=_calculate_checksum(file_path, config.file_hash_algorithm),
            uid=str(file_stats.st_uid),
            gid=str(file_stats.st_gid),
            perm=oct(file_stats.st_mode),
        )
    )


def _build_hash_file_path(
    *,
    original_file_path: str,
    ingestor_files_directory: str,
    hash_file_extension: str,
) -> pathlib.Path:
    """Build the path for the hash file."""
    original_path = pathlib.Path(original_file_path)
    dir_path = pathlib.Path(ingestor_files_directory)
    file_name = ".".join([original_path.name, hash_file_extension])
    return dir_path / pathlib.Path(file_name)


def save_and_build_single_hash_file_desc(
    original_file_desciption: dict, config: FileHandlingOptions
) -> dict:
    """Save the hash of the file and build the description."""
    import datetime
    import json

    from scicat_schemas import load_single_datafile_template

    single_file_template = load_single_datafile_template()
    file_hash: str = original_file_desciption["chk"]
    hash_path = _build_hash_file_path(
        original_file_path=original_file_desciption["path"],
        ingestor_files_directory=config.ingestor_files_directory,
        hash_file_extension=config.hash_file_extension,
    )
    hash_path.write_text(file_hash)

    return json.loads(
        single_file_template.render(
            file_absolute_path=hash_path.absolute(),
            file_size=(file_stats := hash_path.stat()).st_size,
            datetime_isoformat=datetime.datetime.fromtimestamp(
                file_stats.st_ctime, tz=datetime.UTC
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            uid=str(file_stats.st_uid),
            gid=str(file_stats.st_gid),
            perm=oct(file_stats.st_mode),
        )
    )
