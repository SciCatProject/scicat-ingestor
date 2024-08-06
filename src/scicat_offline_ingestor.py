# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
# import scippnexus as snx
import copy
import json
import logging
import os
import pathlib
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
    create_scicat_dataset_instance,
    scicat_dataset_to_dict,
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
    fh_options = ingestion_options.file_handling
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

        # define which is the directory where the ingestor should save
        # the files it creates, if any is created
        ingestor_directory = compose_ingestor_directory(fh_options, nexus_file_path)

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
            nexus_file=nexus_file_path,
            ingestor_directory=ingestor_directory,
            config=fh_options,
            logger=logger,
            # TODO: add done_writing_message_file and nexus_structure_file
        )

        # Create scicat dataset instance(entry)
        local_dataset = scicat_dataset_to_dict(
            create_scicat_dataset_instance(
                metadata_schema_id=metadata_schema["id"],
                metadata_schemas=metadata_schema["schemas"],
                variable_map=variables_values,
                data_file_list=data_file_list,
                config=config.dataset,
                logger=logger,
            )
        )
        # create dataset in scicat
        scicat_dataset = _create_scicat_dataset(local_dataset, config, logger)

        dataset_source_folder = _define_dataset_source_folder(data_file_list)

        origdatablock_datafiles_list = _prepare_origdatablock_datafilelist(
            data_file_list, dataset_source_folder
        )
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
