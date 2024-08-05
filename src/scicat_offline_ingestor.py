# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
<<<<<<< HEAD

from pathlib import Path
=======
# import scippnexus as snx
import copy
import datetime
import hashlib
import json
import logging
import pathlib
import uuid
from urllib.parse import urljoin, quote
import os
>>>>>>> a83d857 (added checks for existing datasets and fixed few other bugs)

import h5py
from scicat_communication import create_scicat_dataset, create_scicat_origdatablock
from scicat_configuration import (
    OfflineIngestorConfig,
    build_arg_parser,
    build_dataclass,
    merge_config_and_input_args,
)
from scicat_dataset import (
    create_data_file_list,
    create_origdatablock_instance,
    create_scicat_dataset_instance,
    extract_variables_values,
    origdatablock_to_dict,
    scicat_dataset_to_dict,
)
from scicat_logging import build_logger
from scicat_metadata import collect_schemas, select_applicable_schema
from scicat_path_helpers import compose_ingestor_directory
from system_helpers import handle_exceptions


def build_offline_config() -> OfflineIngestorConfig:
    arg_parser = build_arg_parser(
        OfflineIngestorConfig, mandatory_args=('--config-file',)
    )
    arg_namespace = arg_parser.parse_args()
    merged_configuration = merge_config_and_input_args(
        Path(arg_namespace.config_file), arg_namespace
    )
    # Remove unused fields
    # It is because ``OfflineIngestorConfig`` shares the template config file
    # with ``OnlineIngestorConfig``.
    del merged_configuration["kafka"]

    return build_dataclass(OfflineIngestorConfig, merged_configuration)


def _check_if_dataset_exists_by_pid(
        local_dataset,
        config,
        logger
) -> bool :
    """
    Check if a dataset with the same pid exists already in SciCat.
    """
    dataset_exists = False
    if config.ingestion.check_if_dataset_exists_by_pid:
        if "pid" in local_dataset.keys() and local_dataset["pid"]:
            logger.info("_check_if_dataset_exists: Checking if dataset with pid {} already exists.".format(local_dataset["pid"]))

            response = requests.request(
                method="GET",
                url=urljoin(config.scicat.host, "datasets/{}".format(quote(local_dataset["pid"]))),
                headers=config.scicat.headers,
                timeout=config.scicat.timeout,
                stream=config.scicat.stream,
                verify=config.scicatverify,
            )

            if not response.ok:
                logger.info("Dataset by job id error. status : {} {}".format(response.status_code, response.reason))
            else:
                result = response.json()
                if result:
                    logger.info("Retrieved Dataset with pid {} from SciCat".format(result["pid"]))
                    dataset_exists = True
        else:
            logger.info("_check_if_dataset_exists: Dataset has no pid associated. Assuming new dataset")

    return dataset_exists


def _check_if_dataset_exists_by_metadata(local_dataset, config, logger):
    """
    Check if a dataset already exists in SciCat where
    the metadata key specified has the same value as the dataset that we want to create
    """
    dataset_exists = False
    if config.ingestion.check_if_dataset_exists_by_metadata:
        metadata_key = config.ingestion.check_if_dataset_exists_by_metadata_key

        if metadata_key in local_dataset["scientificMetadata"].keys() and local_dataset["scientificMetadata"][metadata_key]["value"]:
            metadata_value = local_dataset["scientificMetadata"][metadata_key]["value"]
            logger.info("_check_if_dataset_exists_by_metadata: Checking if dataset with scientific metadata key {} "
                        "set to value {} already exists.".format(metadata_key, metadata_value))

            url = "{}?filter={{\"where\":{}}}".format(
                urljoin(config.scicat.host, "datasets"),
                json.dumps({"scientificMetadata.{}.value".format(metadata_key): metadata_value})
            )
            logger.info("_check_if_dataset_exists_by_metadata: Url : {}".format(url))

            response = requests.request(
                method="GET",
                url=url,
                headers=config.scicat.headers,
                timeout=config.scicat.timeout,
                stream=config.scicat.stream,
                verify=config.scicatverify,
            )

            if not response.ok:
                logger.info("_check_if_dataset_exists_by_metadata: Error. status : {} {}".format(response.status_code, response.reason))
            else:
                results = response.json()
                if results:
                    logger.info("_check_if_dataset_exists_by_metadata: Retrieved {} Dataset from SciCat".format(len(results)))
                    dataset_exists = True
        else:
            logger.info("_check_if_dataset_exists: Dataset has no pid associated. Assuming new dataset")

    return dataset_exists


def main() -> None:
    """Main entry point of the app."""
    config = build_offline_config()
    fh_options = config.ingestion.file_handling
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info(
        'Starting the Scicat background Ingestor with the following configuration: %s',
        config.to_dict(),
    )

    # Collect all metadata schema configurations
    schemas = collect_schemas(config.ingestion.schemas_directory)

    with handle_exceptions(logger):
        nexus_file_path = Path(config.nexus_file)
        logger.info("Nexus file to be ingested: %s", nexus_file_path)

        # Path to the directory where the ingestor saves the files it creates
        ingestor_directory = compose_ingestor_directory(fh_options, nexus_file_path)

        # open nexus file with h5py
        with h5py.File(nexus_file_path) as h5file:
            # load instrument metadata configuration
            metadata_schema = select_applicable_schema(nexus_file_path, h5file, schemas)

            # define variables values
            variable_map = extract_variables_values(
                metadata_schema['variables'], h5file, config.scicat
            )

        # Collect data-file descriptions
        data_file_list = create_data_file_list(
            nexus_file=nexus_file_path,
            ingestor_directory=ingestor_directory,
            config=fh_options,
            logger=logger,
            # TODO: add done_writing_message_file and nexus_structure_file
        )

        # Prepare scicat dataset instance(entry)
        logger.info("Preparing scicat dataset instance ...")
        local_dataset = scicat_dataset_to_dict(
            create_scicat_dataset_instance(
                metadata_schema_id=metadata_schema["id"],
                metadata_schemas=metadata_schema["schemas"],
                variable_map=variable_map,
                data_file_list=data_file_list,
                config=config.dataset,
                logger=logger,
            )
        )
        logger.debug("Scicat dataset: %s", local_dataset)
        # Create dataset in scicat
        scicat_dataset = create_scicat_dataset(
            dataset=local_dataset, config=config.scicat, logger=logger
        )

        # Prepare origdatablock
        logger.info("Preparing scicat origdatablock instance ...")
        local_origdatablock = origdatablock_to_dict(
            create_origdatablock_instance(
                data_file_list=data_file_list,
                scicat_dataset=local_dataset,
                config=fh_options,
            )
        )
<<<<<<< HEAD
        logger.debug("Scicat origdatablock: %s", local_origdatablock)
        # create origdatablock in scicat
        scicat_origdatablock = create_scicat_origdatablock(
            origdatablock=local_origdatablock, config=config.scicat, logger=logger
        )

        # check one more time if we successfully created the entries in scicat
        if not ((len(scicat_dataset) > 0) and (len(scicat_origdatablock) > 0)):
            logger.error(
                "Failed to create dataset or origdatablock in scicat.\n"
                "SciCat dataset: %s\nSciCat origdatablock: %s",
                scicat_dataset,
                scicat_origdatablock,
            )
            raise RuntimeError("Failed to create dataset or origdatablock.")
=======

        # create and populate scicat dataset entry
        local_dataset = _prepare_scicat_dataset(
            metadata_schema,
            variables_values,
            datafilelist,
            config,
            logger
        )

        dataset_already_present = (
            _check_if_dataset_exists_by_pid(local_dataset, config, logger) or
            _check_if_dataset_exists_by_metadata(local_dataset, config, logger)
        )

        if (dataset_already_present):
            logger.info("Dataset with pid {} already present in SciCat. Skipping it!!!".format(local_dataset["pid"]))
        else:
            if (not config.ingestion.dry_run):
                # create dataset in scicat
                scicat_dataset = _create_scicat_dataset(
                    local_dataset,
                    config,
                    logger
                )
            else:
                logger.info("This is a dry run. No request is sent to SciCat and no dataset is created in SciCat")

            # create and populate scicat origdatablock entry
            # with files and hashes previously computed
            local_origdatablock = _prepare_scicat_origdatablock(
                scicat_dataset,
                origdatablock_datafiles_list,
                config,
                logger
            )

            if (not config.ingestion.dry_run):
                # create origdatablock in scicat
                scicat_origdatablock = _create_scicat_origdatablock(
                    local_origdatablock,
                    config,
                    logger
                )
            else:
                logger.info("This is a dry run. No request is sent to SciCat and no origdatablock is created in SciCat")


        # check one more time if we successfully created the entries in scicat
        exit(logger, unexpected=not(bool(scicat_dataset) and bool(scicat_origdatablock)))
>>>>>>> a83d857 (added checks for existing datasets and fixed few other bugs)
