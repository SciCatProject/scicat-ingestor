# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
# import scippnexus as snx
import copy
import datetime
import hashlib
import json
import logging
import pathlib
from urllib.parse import urljoin
import os

import h5py
import pytz
import requests
from scicat_configuration import (
    BackgroundIngestorConfig,
    build_background_ingestor_arg_parser,
    build_scicat_background_ingester_config,
)
from scicat_dataset import (
    build_single_data_file_desc,
    convert_to_type,
    save_and_build_single_hash_file_desc,
)
from scicat_logging import build_logger
from scicat_metadata import collect_schemas, select_applicable_schema
from src.scicat_path_helpers import compose_ingestor_directory, compose_ingestor_output_file_path
from system_helpers import exit_at_exceptions


def replace_variables_values(url: str, values: dict) -> str:
    for key, value in values.items():
        url = url.replace("{" + key + "}", str(value))
    return url


def extract_variables_values(
    variables: dict, h5file, config: BackgroundIngestorConfig
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

def _new_hash(algorithm: str) -> Any:
    try:
        return hashlib.new(algorithm, usedforsecurity=False)
    except TypeError:
        # Fallback for Python < 3.9
        return hashlib.new(algorithm)


def _compute_file_checksum(file_full_path: pathlib.Path, algorithm: str) -> str:
    """
    Compute the checksum of a file using specified algorithm.
    :param file_full_path:
    :param algorithm:
    :return:
    """
    chk = _new_hash(algorithm)
    buffer = memoryview(bytearray(128 * 1024))
    with file_full_path.open("rb", buffering=0) as file:
        for n in iter(lambda: file.readinto(buffer), 0):
            chk.update(buffer[:n])
    return chk.hexdigest()  # type: ignore[no-any-return]


def _create_datafiles_entry(
        file_full_path: pathlib.Path,
        config,
        logger
):
    """
    Create the matching entry in the datafiles list for the file proovided
    :param file_full_path:
    :param config:
    :param logger:
    :return:
    """
    logger.info("create_datafiles_entry: adding file {}".format(file_full_path))

    datafiles_item = {
        "path": file_full_path,
        "size": 0,
        "time": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }

    if config.ingestion_options.compute_files_stats and file_full_path.exists():
        logger.info("create_datafiles_entry: reading file stats from disk")
        stats = file_full_path.stat()
        datafiles_item = {
            **datafiles_item,
            **{
                "size": stats.st_size,
                "time": datetime.datetime.fromtimestamp(stats.st_ctime, tz=pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "uid": stats.st_uid,
                "gid": stats.st_gid,
                "perm": stats.st_mode,
            }
        }

    return datafiles_item

def _compute_file_checksum_if_needed(
        file_full_path: pathlib.Path,
        ingestor_directory: pathlib.Path,
        config,
        logger
):
    checksum = ""
    datafiles_item = {}

    if config.ingestion_options.compute_files_hash and os.path.exists(file_full_path):
        logger.info("create_datafiles_entry: computing hash of the file from disk")
        checksum = _compute_file_checksum(file_full_path, config.ingestion_options.file_hash_algorithm)

        if config.ingstion_options.save_hash_in_file:

            # file path for hash file
            hash_file_full_path = compose_ingestor_output_file_path(
                ingestor_directory,
                file_full_path.stem,
                config.ingestion_options.hash_file_extension)
            logger.info("create_datafiles_entry: saving hash in file {}".format(hash_file_full_path))

            # save hash in file
            with hash_file_full_path.open('w') as fh:
                fh.write(datafiles_item['chk'])

            datafiles_item = _create_datafiles_entry(hash_file_full_path,config,logger)

    return checksum, datafiles_item


def _create_datafiles_list(
        nexus_file_path: pathlib.Path,
        done_writing_message_file_path: pathlib.Path,
        ingestor_directory: pathlib.Path,
        config,
        logger
) -> list:
    """
    Update the file size and creation time according to the configuration
    :param nexus_file_path:
    :param done_writing_message_file_path,
    :param config,
    :param logger
    :return:
    """

    logger.info("create_datafiles_list: adding nexus file {}".format(nexus_file_path))
    datafiles_list = [
        _create_datafiles_entry(nexus_file_path, config, logger)
    ]
    checksum, datafiles_hash_item = _compute_file_checksum_if_needed(
        nexus_file_path,
        ingestor_directory,
        config,
        logger)
    if checksum:
        datafiles_list[0]['chk'] = checksum
    if datafiles_hash_item:
        datafiles_list.append(datafiles_hash_item)

    if config.kafka_options.message_saving_options.message_to_file:
        logger.info("create_datafiles_list: adding done writing message file {}".format(done_writing_message_file_path))
        datafiles_list.append(
            _create_datafiles_entry(done_writing_message_file_path, config, logger)
        )
        checksum, datafiles_hash_item = _compute_file_checksum_if_needed(
            nexus_file_path,
            ingestor_directory,
            config,
            logger)
        if checksum:
            datafiles_list[-1]['chk'] = checksum
        if datafiles_hash_item:
            datafiles_list.append(datafiles_hash_item)

    return datafiles_list

def prepare_scicat_dataset(metadata_schema, values):
    """Prepare scicat dataset as dictionary ready to be ``POST``ed."""
    schema: dict = metadata_schema["schema"]
    dataset = {}
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

    return dataset


def create_scicat_dataset(dataset: str, config: dict, logger: logging.Logger) -> dict:
    """
    Execute a POST request to scicat to create a dataset
    """
    response = requests.request(
        method="POST",
        url=urljoin(config["scicat_url"], "datasets"),
        json=dataset,
        headers=config["scicat_headers"],
        timeout=config["timeout_seconds"],
        stream=False,
        verify=True,
    )

    result = response.json()
    if response.ok:
        ...
    else:
        err = result.get("error", {})
        raise Exception(f"Error creating new dataset: {err}")

    logger.info("Dataset create successfully. Dataset pid: %s", result['pid'])
    return result


def create_scicat_origdatablock(
    scicat_dataset_pid, nexus_file=None, done_writing_message_file=None
): ...


def _define_dataset_source_folder(
    datafiles_list
) -> pathlib.Path:
    """
    Return the dataset source folder, which is the common path between all the data files associated with the dataset
    """
    return pathlib.Path( os.path.commonpath( [item["path"] for item in datafiles_list]))


def _path_to_relative(
        datafiles_item: dict,
        dataset_source_folder: pathlib.Path
) -> dict:
    """
    Copy the datafiles item and transform the path to the relative path to the dataset source folder
    """
    origdatablock_datafile_item = copy.deepcopy(datafiles_item)
    origdatablock_datafile_item["path"] = str(datafiles_item["path"].to_relative(dataset_source_folder))
    return origdatablock_datafile_item


def _prepare_origdatablock_datafiles_list(
        datafiles_list: list,
        dataset_source_folder: pathlib.Path
) -> list:
    """
    Prepare the datafiles list for the origdatablock entry in scicat
    That means that the file paths needs to be relative to the dataset source folder
    """
    return [_path_to_relative(item,dataset_source_folder) for item in datafiles_list]


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_background_ingestor_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_background_ingester_config(arg_namespace)
    ingestion_options = config.ingestion_options
    file_handling_options = ingestion_options.file_handling_options
    logger = build_logger(config)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info(
        'Starting the Scicat background Ingestor with the following configuration:'
    )
    logger.info(config.to_dict())

    # Collect all metadata schema configurations
    schemas = collect_schemas(ingestion_options.schema_directory)

    with exit_at_exceptions(logger, daemon=False):
        nexus_file_path = pathlib.Path(config.single_run_options.nexus_file)
        logger.info(
            "Nexus file to be ingested : %s",
            nexus_file_path,
        )
        done_writing_message_file_path = pathlib.Path()
        if config.kafka_options.message_saving_options.message_to_file:
            done_writing_message_file_path = pathlib.Path(
                config.single_run_options.done_writing_message_file)
            logger.info(
                "Done writing message file linked to nexus file : %s",
                done_writing_message_file_path
            )

            # log done writing message input file
            logger.info(json.load(done_writing_message_file_path.open()))

        # define which is the directory where the ingestor should save the files it creates, if any is created
        ingestor_directory = compose_ingestor_directory(
            config.ingestion_options.file_handling_options,
            nexus_file_path
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
        data_file_list = [
            build_single_data_file_desc(nexus_file_path, file_handling_options),
            build_single_data_file_desc(
                done_writing_message_file, file_handling_options
            ),
            # TODO: Add nexus structure file
        ]
        # Create hash of all the files if needed
        if file_handling_options.save_file_hash:
            data_file_list += [
                save_and_build_single_hash_file_desc(
                    data_file_dict, file_handling_options
                )
                for data_file_dict in data_file_list
            ]
        # Collect all data-files and hash-files descriptions
        _ = [json.dumps(file_dict, indent=2) for file_dict in data_file_list]

        # create datafiles list
        datafiles_list = _create_datafiles_list(
            nexus_file_path,
            done_writing_message_file_path,
            ingestor_directory,
            config,
            logger
        )

        dataset_source_folder = _define_dataset_source_folder(
            datafiles_list
        )

        origdatablock_datafiles_list = _prepare_origdatablock_datafiles_list(
            datafiles_list,
            dataset_source_folder
        )

        # create and populate scicat dataset entry
        scicat_dataset = prepare_scicat_dataset(metadata_schema, variables_values)

        # create dataset in scicat
        scicat_dataset = create_scicat_dataset(scicat_dataset, config)
        scicat_dataset_pid = scicat_dataset["pid"]

        # create and populate scicat origdatablock entry
        # with files and hashes previously computed
        scicat_origdatablock = create_scicat_origdatablock(
            scicat_dataset_pid,
            origdatablock_datafiles_list
        )

        # create origdatablock in scicat
        scicat_origdatablock_id = create_scicat_origdatablock(scicat_origdatablock)

        # return successful code
        return scicat_origdatablock_id
