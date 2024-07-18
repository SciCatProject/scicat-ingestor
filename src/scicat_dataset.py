# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import datetime
from types import MappingProxyType
from typing import Any

from scicat_schemas import (
    load_dataset_schema_template,
    load_origdatablock_schema_template,
    load_single_datafile_template,
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


def build_dataset_description(
    *,
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


def build_single_datafile_description(
    *,
    file_absolute_path: str,
    file_size: int,
    datetime_isoformat: str,
    checksum: str,
    uid: str,
    gid: str,
    perm: str,
) -> str:
    return load_single_datafile_template().render(
        file_absolute_path=file_absolute_path,
        file_size=file_size,
        datetime_isoformat=datetime_isoformat,
        checksum=checksum,
        uid=uid,
        gid=gid,
        perm=perm,
    )


def build_orig_datablock_description(
    *,
    nxs_dataset_pid: str,
    dataset_size: int,
    check_algorithm: str,
    data_file_desc_list: list[str],
) -> str:
    return load_origdatablock_schema_template().render(
        nxs_dataset_pid=nxs_dataset_pid,
        dataset_size=dataset_size,
        check_algorithm=check_algorithm,
        data_file_desc_list=data_file_desc_list,
    )
