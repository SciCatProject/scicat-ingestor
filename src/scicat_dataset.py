# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
from jinja2 import Template
from scicat_path_helpers import get_dataset_schema_template_path


def build_dataset_schema(
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
    return Template(get_dataset_schema_template_path().read_text()).render(
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
