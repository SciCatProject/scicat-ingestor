# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
# import scippnexus as snx
import copy
import datetime
import json
import logging
import os
import pathlib
import uuid
from urllib.parse import urljoin

import h5py
import requests
from scicat_configuration import (
    OfflineIngestorConfig,
    build_offline_ingestor_arg_parser,
    build_scicat_offline_ingestor_config,
)
from scicat_dataset import (
    convert_to_type,
    create_data_file_list,
)
from scicat_logging import build_logger
from scicat_metadata import collect_schemas, select_applicable_schema
from scicat_path_helpers import compose_ingestor_directory
from system_helpers import exit, offline_ingestor_exit_at_exceptions


def replace_variables_values(url: str, values: dict) -> str:
    for key, value in values.items():
        url = url.replace("{" + key + "}", str(value))
    return url


def extract_variables_values(
    variables: dict, h5file, config: OfflineIngestorConfig
) -> dict:
    values = {}

    # loop on all the variables defined
    for variable in variables.keys():
        source = variables[variable]["source"]
        value = ""
        if source == "NXS":
            # extract value from nexus file
            # we need to address path entry/user_*/name
            value = h5file[variables[variable]["path"]][...]
        elif source == "SC":
            # build url
            url = replace_variables_values(
                config[""]["scicat_url"] + variables[variable]["url"], values
            )
            # retrieve value from SciCat
            response = requests.get(
                url,
                headers={"token": config[""]["token"]},
                timeout=10,  # TODO: decide timeout. Maybe from configuration?
            )
            # extract value
            value = response.json()[variables[variable]["field"]]
        elif source == "VALUE":
            # the value is the one indicated
            # there might be some substitution needed
            value = replace_variables_values(variables[variable]["value"], values)
            if (
                "operator" in variables[variable].keys()
                and variables[variable]["operator"]
            ):
                operator = variables[variable]["operator"]
                if operator == "join_with_space":
                    value = ", ".join(value)
        else:
            raise Exception("Invalid variable source configuration")

        values[variable] = convert_to_type(value, variables[variable]["value_type"])

    return values


def _create_datafilelist_item(file_full_path: pathlib.Path, config, logger):
    """
    Create the matching entry in the datafiles list for the file provided
    :param file_full_path:
    :param config:
    :param logger:
    :return:
    """
    datafilelist_item = {
        "path": file_full_path,
        "size": 0,
        "time": datetime.datetime.now(tz=datetime.UTC).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        ),
    }

    if config.ingestion.compute_files_stats and file_full_path.exists():
        logger.info("create_datafilelist_item: reading file stats from disk")
        stats = file_full_path.stat()
        datafilelist_item = {
            **datafilelist_item,
            **{
                "size": stats.st_size,
                "time": datetime.datetime.fromtimestamp(
                    stats.st_ctime, tz=datetime.UTC
                ).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "uid": stats.st_uid,
                "gid": stats.st_gid,
                "perm": stats.st_mode,
            },
        }

    return datafilelist_item


def _prepare_scicat_dataset(
    metadata_schema: dict, values: dict, datafilelist: list[dict], config, logger
):
    """
    Prepare scicat dataset as dictionary ready to be ``POST``ed.
    """
    logger.info("_prepare_scicat_dataset: Preparing scicat dataset structure")
    schema: dict = metadata_schema["schema"]
    dataset: dict = {}

    scientific_metadata = {
        'ingestor_metadata_schema_id': {
            "value": metadata_schema["id"],
            "unit": "",
            "human_name": "Ingestor Metadata Schema Id",
            "type": "string",
        }
    }
    for field in schema.values():
        machine_name = field["machine_name"]
        field_type = field["type"]
        if field["field_type"] == "high_level":
            dataset[machine_name] = convert_to_type(
                replace_variables_values(field["value"], values), field_type
            )
        elif field["field_type"] == "scientific_metadata":
            scientific_metadata[machine_name] = {
                "value": convert_to_type(
                    replace_variables_values(field["value"], values), field_type
                ),
                "unit": "",
                "human_name": field["human_name"]
                if field.get("human_name", None)
                else machine_name,
                "type": field_type,
            }
        else:
            raise Exception("Metadata schema field type invalid")

    dataset["scientific_metadata"] = scientific_metadata

    # now check that the configuration setting shave been respected
    if not config.dataset.allow_dataset_pid and "pid" in dataset.keys():
        logger.info("_prepare_scicat_dataset: Pid not allowed by configuration")
        del dataset["pid"]
    if config.dataset.generate_dataset_pid:
        logger.info("_prepare_scicat_dataset: Auto generating pid by configuration")
        dataset["pid"] = str(uuid.uuid4())

    if "instrumentId" not in dataset.keys() or not dataset["instrumentId"]:
        logger.info(
            "_prepare_scicat_dataset: Assigning default instrument id: %s",
            config.dataset.default_instrument_id,
        )
        dataset["instrumentId"] = config.dataset.default_instrument_id

    if "proposalId" not in dataset.keys() or not dataset["proposalId"]:
        logger.info(
            "_prepare_scicat_dataset: Assigning default proposal id: %s",
            config.dataset.default_proposal_id,
        )
        dataset["proposalId"] = config.dataset.default_proposal_id

    if "ownerGroup" not in dataset.keys() or not dataset["ownerGroup"]:
        logger.info(
            "_prepare_scicat_dataset: Assigning default ownerGroup: %s",
            config.dataset.default_owner_group,
        )
        dataset["ownerGroup"] = config.dataset.default_owner_group

    if "accessGroups" not in dataset.keys() or not dataset["accessGroups"]:
        logger.info(
            "_prepare_scicat_dataset: Assigning default accessGroups: %s",
            json.dumps(config.dataset.default_access_groups),
        )
        dataset["accessGroups"] = config.dataset.default_access_groups

    dataset["size"] = len(datafilelist)
    dataset["numberOfFiles"] = sum([item["size"] for item in datafilelist])
    dataset["isPublished"] = False

    logger.info("_prepare_scicat_dataset: Scicat dataset: %s", json.dumps(dataset))
    return dataset


def _create_scicat_dataset(dataset: dict, config, logger: logging.Logger) -> dict:
    """
    Execute a POST request to scicat to create a dataset
    """
    logger.info("_create_scicat_dataset: Sending POST request to create new dataset")
    response = requests.request(
        method="POST",
        url=urljoin(config.scicat.host, "datasets"),
        json=dataset,
        headers=config.scicat.headers,
        timeout=config.scicat.timeout,
        stream=False,
        verify=True,
    )

    result = response.json()
    if not response.ok:
        err = result.get("error", {})
        logger.error(
            "_create_scicat_dataset: Failed to create new dataset. Error %s", err
        )
        raise Exception(f"Error creating new dataset: {err}")

    logger.info(
        "_create_scicat_dataset: Dataset created successfully. Dataset pid: %s",
        result['pid'],
    )
    return result


def _prepare_scicat_origdatablock(scicat_dataset, datafilelist, config, logger):
    """
    Create local copy of the orig datablock to send to scicat
    """
    logger.info(
        "_prepare_scicat_origdatablock: Preparing scicat origdatablock structure"
    )
    origdatablock = {
        "ownerGroup": scicat_dataset["ownerGroup"],
        "accessGroups": scicat_dataset["accessGroups"],
        "size": sum([item["size"] for item in datafilelist]),
        "chkAlg": config.ingestion.file_hash_algorithm,
        "dataFileList": datafilelist,
        "datasetId": scicat_dataset["pid"],
    }

    logger.info(
        "_prepare_scicat_origdatablock: Scicat origdatablock: %s",
        json.dumps(origdatablock),
    )
    return origdatablock


def _create_scicat_origdatablock(
    origdatablock: dict, config, logger: logging.Logger
) -> dict:
    """
    Execute a POST request to scicat to create a new origdatablock
    """
    logger.info(
        "_create_scicat_origdatablock: Sending POST request to create new origdatablock"
    )
    response = requests.request(
        method="POST",
        url=urljoin(config.scicat.host, "origdatablocks"),
        json=origdatablock,
        headers=config.scicat.headers,
        timeout=config.scicat.timeout,
        stream=False,
        verify=True,
    )

    result = response.json()
    if not response.ok:
        err = result.get("error", {})
        logger.error(
            "_create_scicat_origdatablock: Failed to create new origdatablock."
            "Error %s",
            err,
        )
        raise Exception(f"Error creating new origdatablock: {err}")

    logger.info(
        "_create_scicat_origdatablock: Origdatablock created successfully. "
        "Origdatablock pid: %s",
        result['_id'],
    )
    return result


def _define_dataset_source_folder(datafilelist) -> pathlib.Path:
    """
    Return the dataset source folder, which is the common path
    between all the data files associated with the dataset
    """
    return pathlib.Path(os.path.commonpath([item["path"] for item in datafilelist]))


def _path_to_relative(
    datafilelist_item: dict, dataset_source_folder: pathlib.Path
) -> dict:
    """
    Copy the datafiles item and transform the path to the relative path
    to the dataset source folder
    """
    origdatablock_datafilelist_item = copy.deepcopy(datafilelist_item)
    origdatablock_datafilelist_item["path"] = str(
        datafilelist_item["path"].to_relative(dataset_source_folder)
    )
    return origdatablock_datafilelist_item


def _prepare_origdatablock_datafilelist(
    datafiles_list: list, dataset_source_folder: pathlib.Path
) -> list:
    """
    Prepare the datafiles list for the origdatablock entry in scicat
    That means that the file paths needs to be relative to the dataset source folder
    """
    return [_path_to_relative(item, dataset_source_folder) for item in datafiles_list]


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_offline_ingestor_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_offline_ingestor_config(arg_namespace)
    ingestion_options = config.ingestion
    file_handling_options = ingestion_options.file_handling
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info(
        'Starting the Scicat background Ingestor with the following configuration: %s',
        config.to_dict(),
    )

    # Collect all metadata schema configurations
    schemas = collect_schemas(ingestion_options.schemas_directory)

    with offline_ingestor_exit_at_exceptions(logger):
        nexus_file_path = pathlib.Path(config.offline_run.nexus_file)
        logger.info(
            "Nexus file to be ingested : %s",
            nexus_file_path,
        )
        data_file_paths = [nexus_file_path]
        done_writing_message_file_path = pathlib.Path()
        if config.ingestion.file_handling.message_to_file:
            done_writing_message_file_path = pathlib.Path(
                config.offline_run.done_writing_message_file
            )
            logger.info(
                "Done writing message file linked to nexus file : %s",
                done_writing_message_file_path,
            )

            # log done writing message input file
            logger.info(json.load(done_writing_message_file_path.open()))
            data_file_paths.append(done_writing_message_file_path)

        # define which is the directory where the ingestor should save
        # the files it creates, if any is created
        ingestor_directory = compose_ingestor_directory(
            config.ingestion.file_handling, nexus_file_path
        )

        # open nexus file with h5py
        with h5py.File(nexus_file_path) as h5file:
            # load instrument metadata configuration
            metadata_schema = select_applicable_schema(nexus_file_path, h5file, schemas)

            # define variables values
            variables_values = extract_variables_values(
                metadata_schema['variables'], h5file, config
            )

        # Collect data-file descriptions
        data_file_list = create_data_file_list(
            data_file_paths, ingestor_directory, file_handling_options, logger
        )

        dataset_source_folder = _define_dataset_source_folder(data_file_list)

        origdatablock_datafiles_list = _prepare_origdatablock_datafilelist(
            data_file_list, dataset_source_folder
        )

        # create and populate scicat dataset entry
        local_dataset = _prepare_scicat_dataset(
            metadata_schema, variables_values, data_file_list, config, logger
        )

        # create dataset in scicat
        scicat_dataset = _create_scicat_dataset(local_dataset, config, logger)

        # create and populate scicat origdatablock entry
        # with files and hashes previously computed
        local_origdatablock = _prepare_scicat_origdatablock(
            scicat_dataset, origdatablock_datafiles_list, config, logger
        )

        # create origdatablock in scicat
        scicat_origdatablock = _create_scicat_origdatablock(
            local_origdatablock, config, logger
        )

        # check one more time if we successfully created the entries in scicat
        exit(logger, unexpected=(bool(scicat_dataset) and bool(scicat_origdatablock)))
