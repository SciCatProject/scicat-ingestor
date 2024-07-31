# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)


def test_single_datafile_template_loading() -> None:
    from scicat_schemas.load_template import load_single_datafile_template

    assert load_single_datafile_template() is not None


def test_dataset_schema_template_loading() -> None:
    from scicat_schemas.load_template import load_dataset_schema_template

    assert load_dataset_schema_template() is not None


def test_origdatablock_schema_template_loading() -> None:
    from scicat_schemas.load_template import load_origdatablock_schema_template

    assert load_origdatablock_schema_template() is not None


_example_scientific_metadata = """"run_number": {
      "value": 18856,
      "unit": "",
      "human_name": "Run Number",
      "type": "integer"
    },
    "sample_temperature": {
      "value": 20.4,
      "unit": "C",
      "human_name": "Sample Temperature",
      "type": "quantity"
    },
    "start_time" : {
      "value" : "2024-07-16T09:30:12.987Z",
      "unit" : "",
      "human_name" : "Start Time",
      "type" : "date"
    }"""

_example_dataset_schema = (
    """
{
  "pid": "12.234.34567/e3690b21-ee8c-40d6-9409-6b6fdca776d2",
  "datasetName": "this is a dataset",
  "description": "this is the description of the dataset",
  "principalInvestigator": "Somebodys Name",
  "creationLocation": "ESS:CODA",
  "scientificMetadata": {
    """
    + _example_scientific_metadata
    + """
  },
  "owner": "Somebodys Name",
  "ownerEmail": "someones_@_email",
  "sourceFolder": "/ess/data/coda/2024/616254",
  "contactEmail": "someones_@_email",
  "creationTime": "2024-07-16T10:00:00.000Z",
  "type": "raw",
  "techniques": [
    {
      "pid": "someprotocol://someones/url/and/id",
      "names": "absorption and phase contrast nanotomography"
    }
  ],
  "instrumentId": "12.234.34567/765b3dc3-f658-410e-b371-04dd1adcd520",
  "sampleId": "bd31725a-dbfd-4c32-87db-1c1ebe61e5ca",
  "proposalId": "616254",
  "ownerGroup": "ess_proposal_616254",
  "accessGroups": [
    "scientific information management systems group",
    "scicat group"
  ]
}

"""
)


def test_dataset_schema_rendering() -> None:
    import json

    from scicat_dataset import build_dataset_instance

    dataset_schema = build_dataset_instance(
        dataset_pid_prefix="12.234.34567",
        nxs_dataset_pid="e3690b21-ee8c-40d6-9409-6b6fdca776d2",
        dataset_name="this is a dataset",
        dataset_description="this is the description of the dataset",
        principal_investigator="Somebodys Name",
        facility="ESS",
        environment="CODA",
        scientific_metadata=_example_scientific_metadata,
        owner="Somebodys Name",
        owner_email="someones_@_email",
        source_folder="/ess/data/coda/2024/616254",
        contact_email="someones_@_email",
        iso_creation_time="2024-07-16T10:00:00.000Z",
        technique_pid="someprotocol://someones/url/and/id",
        technique_name="absorption and phase contrast nanotomography",
        instrument_id="12.234.34567/765b3dc3-f658-410e-b371-04dd1adcd520",
        sample_id="bd31725a-dbfd-4c32-87db-1c1ebe61e5ca",
        proposal_id="616254",
        owner_group="ess_proposal_616254",
        access_groups=[
            "scientific information management systems group",
            "scicat group",
        ],
    )

    assert json.loads(dataset_schema) == json.loads(_example_dataset_schema)


_example_file_description_1 = """
{
  "path": "/ess/data/coda/2024/616254/0001.nxs",
  "size": 1231231,
  "time": "2024-07-16T10:00:00.000Z",
  "chk": "1234567890abcdef",
  "uid": "1004",
  "gid": "1005",
  "perm": "33188"
}
"""


def test_single_file_description_rendering() -> None:
    import json

    from scicat_dataset import build_single_datafile_instance

    file_description = build_single_datafile_instance(
        file_absolute_path="/ess/data/coda/2024/616254/0001.nxs",
        file_size=1231231,
        datetime_isoformat="2024-07-16T10:00:00.000Z",
        checksum="1234567890abcdef",
        uid="1004",
        gid="1005",
        perm="33188",
    )

    assert json.loads(file_description) == json.loads(_example_file_description_1)


_example_file_description_2 = """
{
  "path": "/ess/data/coda/2024/616254/0002.nxs",
  "size": 1231231,
  "time": "2024-07-16T10:00:00.000Z",
  "uid": "1004",
  "gid": "1005",
  "perm": "33188"
}
"""


def test_single_file_description_rendering_no_checksum() -> None:
    import json

    from scicat_dataset import build_single_datafile_instance

    file_description = build_single_datafile_instance(
        file_absolute_path="/ess/data/coda/2024/616254/0002.nxs",
        file_size=1231231,
        datetime_isoformat="2024-07-16T10:00:00.000Z",
        uid="1004",
        gid="1005",
        perm="33188",
    )

    assert json.loads(file_description) == json.loads(_example_file_description_2)


_example_file_description_3 = """
{
  "path": "/ess/data/coda/2024/616254/0003.nxs",
  "size": 1231231,
  "time": "2024-07-16T10:00:00.000Z",
  "chk": "1234567890abcdef",
  "uid": "1004",
  "gid": "1005",
  "perm": "33188"
}
"""

_example_orig_datablock = (
    """
{
  "datasetId": "20.500.12269/53fd2786-3729-11ef-83e5-fa163e9aae0a",
  "size": 446630741,
  "chkAlg": "blake2b",
  "dataFileList": [
    """
    + _example_file_description_1
    + """,
    """
    + _example_file_description_2
    + """,
    """
    + _example_file_description_3
    + """
  ]
}
"""
)


def test_orig_datablock_rendering() -> None:
    import json

    from scicat_dataset import build_orig_datablock_instance

    orig_datablock = build_orig_datablock_instance(
        dataset_pid="20.500.12269/53fd2786-3729-11ef-83e5-fa163e9aae0a",
        dataset_size=446630741,
        check_algorithm="blake2b",
        data_file_desc_list=[
            _example_file_description_1,
            _example_file_description_2,
            _example_file_description_3,
        ],
    )

    assert json.loads(orig_datablock) == json.loads(_example_orig_datablock)
