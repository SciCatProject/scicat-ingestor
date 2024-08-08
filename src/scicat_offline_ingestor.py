# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
# import scippnexus as snx
import copy
import json
import os
import pathlib

import h5py
from scicat_communication import create_scicat_dataset, create_scicat_origdatablock
from scicat_configuration import (
    build_offline_ingestor_arg_parser,
    build_scicat_offline_ingestor_config,
)
from scicat_dataset import (
    create_data_file_list,
    create_scicat_dataset_instance,
    extract_variables_values,
    scicat_dataset_to_dict,
)
from scicat_logging import build_logger
from scicat_metadata import collect_schemas, select_applicable_schema
from scicat_path_helpers import compose_ingestor_directory
from system_helpers import handle_exceptions


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

    with handle_exceptions(logger):
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

        # Create scicat dataset instance(entry)
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
        # create dataset in scicat
        scicat_dataset = create_scicat_dataset(
            dataset=local_dataset, config=config.scicat, logger=logger
        )

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
