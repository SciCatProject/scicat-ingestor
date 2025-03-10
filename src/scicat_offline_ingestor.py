# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)

import logging
from pathlib import Path
import re
from typing import OrderedDict

import h5py
from scicat_communication import (
    login,
    logout,
    check_dataset_by_metadata,
    check_dataset_by_pid,
    check_datafiles,
    check_origdatablock_by_datasetId,
    get_instrument_by_name,
    get_instrument_nomad_id_by_name,
    get_proposal_by_id,
    get_sample_by_id,
    get_dataset_by_pid,
    create_scicat_dataset,
    patch_scicat_dataset,
    patch_scicat_dataset_numfiles,
    patch_scicat_origdatablock,
    create_scicat_origdatablock,
    create_instrument,
    create_proposal,
    create_sample,
)
from scicat_configuration import (
    IngestionOptions,
    OfflineIngestorConfig,
    SciCatOptions,
    build_arg_parser,
    build_dataclass,
    merge_config_and_input_args,
)
from scicat_dataset import (
    ScicatDataset,
    create_data_file_list,
    create_origdatablock_instance,
    create_scicat_dataset_instance,
    extract_variables_values,
    origdatablock_to_dict,
    scicat_dataset_to_dict,
)
from scicat_logging import build_logger
from scicat_metadata import MetadataSchema, collect_schemas, select_applicable_schema
from scicat_path_helpers import compose_ingestor_directory
from system_helpers import exit, handle_exceptions


def build_offline_config(logger: logging.Logger | None = None) -> OfflineIngestorConfig:
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

    config = build_dataclass(
        tp=OfflineIngestorConfig, data=merged_configuration, logger=logger, strict=False
    )

    return config


def _check_if_dataset_exists_by_pid(
    local_dataset: ScicatDataset,
    ingest_config: IngestionOptions,
    scicat_config: SciCatOptions,
    logger: logging.Logger,
) -> bool:
    """
    Check if a dataset with the same pid exists already in SciCat.
    """
    if ingest_config.check_if_dataset_exists_by_pid and (local_dataset.pid is not None):
        logger.debug(
            "Checking if dataset with pid %s already exists.", local_dataset.pid
        )
        return check_dataset_by_pid(
            pid=local_dataset.pid, config=scicat_config, logger=logger
        )

    # Other cases, assuming dataset does not exist
    return False


def _check_if_dataset_exists_by_metadata(
    local_dataset: ScicatDataset,
    ingest_config: IngestionOptions,
    scicat_config: SciCatOptions,
    logger: logging.Logger,
):
    """
    Check if a dataset already exists in SciCat where
    the metadata key specified has the same value as the dataset that we want to create
    """
    if ingest_config.check_if_dataset_exists_by_metadata:
        metadata_key = ingest_config.check_if_dataset_exists_by_metadata_key
        target_metadata: dict = local_dataset.scientificMetadata.get(metadata_key, {})
        metadata_value = target_metadata.get("value")

        if metadata_value is not None:
            logger.debug(
                "Checking if dataset with scientific metadata key %s "
                "set to value %s already exists.",
                metadata_key,
                metadata_value,
            )
            return check_dataset_by_metadata(
                metadata_key=metadata_key,
                metadata_value=metadata_value,
                config=scicat_config,
                logger=logger,
            )
        else:
            logger.debug(
                "No value found for metadata key %s specified for checking dataset.",
                metadata_key,
            )
    else:
        logger.debug("No metadata key specified for checking dataset existence.")

    # Other cases, assuming dataset does not exist
    return False

def _check_if_datafile_exists(
    local_dataset: ScicatDataset,
    nexus_file_path: Path,
    scicat_config: SciCatOptions,
    logger: logging.Logger,
) -> bool:
    """
    Check if a datafile exists already in SciCat.
    """
    return check_datafiles(
        datafiles=[nexus_file_path.name],
        proposalId=local_dataset.proposalId,
        config=scicat_config,
        logger=logger
    )

def _increment_dataset_number(pid: str) -> str:
    """Increment the dataset number in the PID.
    """
    base, number = pid.rsplit('DS', 1)
    return f"{base}DS{int(number) + 1}"

def is_valid_email(email: str) -> bool:
    """
    Validates if a string is in a valid email format.
    """
    if not email:
        return False
        
    # Standard email validation pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def _generate_or_get_dataset_pid(
    local_dataset: ScicatDataset,
    scicat_config: SciCatOptions,
    logger: logging.Logger,
) -> str:
    """Generate a dataset PID or get it from SciCat
    if a dataset for same proposal and sampl already exists.
    """
    pid = f"{local_dataset.proposalId}-DS0"
    existing_dataset = get_dataset_by_pid(
        pid=pid, config=scicat_config, logger=logger
    )
    while existing_dataset:
        if local_dataset.sampleId == existing_dataset.get("sampleId"):
            return pid
        pid = _increment_dataset_number(pid)
        existing_dataset = get_dataset_by_pid(
            pid=pid, config=scicat_config, logger=logger
        )
    return pid

def _process_ill_dataset(
    local_dataset_instance: ScicatDataset,
    nexus_file_path: Path,
    variable_map: dict,
    config: OfflineIngestorConfig,
    logger: logging.Logger,
) -> ScicatDataset:
    if local_dataset_instance.datasetName == "Internal use":
        raise RuntimeError("Dataset name is set to 'Internal use'.")

    if not is_valid_email(local_dataset_instance.contactEmail):
        local_dataset_instance.contactEmail += "@ill.fr"
        logger.debug(
            "Contact email is not a valid email address. Appending '@ill.fr' to it."
        )

    if "instrument_name" in variable_map:
        instrument_data_list = get_instrument_by_name(
            variable_map["instrument_name"], config.scicat, logger
        )
        if not instrument_data_list:
            unique_name = get_instrument_nomad_id_by_name(
                variable_map["instrument_name"], config.scicat, logger
            )
            instrument_data = create_instrument(
                instrument_name=variable_map["instrument_name"],
                unique_name=unique_name if unique_name else variable_map["instrument_name"],
                config=config.scicat,
                logger=logger,
            )
        else:
            instrument_data = instrument_data_list[0]
        local_dataset_instance.instrumentId = instrument_data["pid"]
    else:
        raise RuntimeError("Instrument name is not set in the variables.")
    
    if _check_if_datafile_exists(
        local_dataset_instance, nexus_file_path, config.scicat, logger
    ):
        logger.warning(
            "Datafile %s of proposal %s already present in SciCat. Skipping it!!!",
            nexus_file_path.name, local_dataset_instance.proposalId,
        )
        raise RuntimeError("Datafile already present in SciCat.")

    if "duration" in variable_map and "duration" in local_dataset_instance.scientificMetadata:
        try:
            # Create a clean relative path for use as the key
            source_folder = local_dataset_instance.sourceFolder
            rel_path = nexus_file_path.relative_to(source_folder) if nexus_file_path.is_relative_to(source_folder) else nexus_file_path.name
            
            # Store duration as a mapping from file path to duration value
            duration_value = local_dataset_instance.scientificMetadata["duration"]["value"]
            local_dataset_instance.scientificMetadata["duration"]["value"] = {str(rel_path): duration_value}
        except (ValueError, TypeError) as e:
            logger.warning("Failed to process duration metadata: %s", str(e))

    if "proposal_id" in variable_map:
        proposal_data = get_proposal_by_id(
            local_dataset_instance.proposalId, config.scicat, logger
        )
        if not proposal_data:
            proposal_data = create_proposal(
                local_dataset=local_dataset_instance,
                config=config.scicat,
                logger=logger,
            )
            if not proposal_data:
                raise RuntimeError("Failed to create proposal with ID: %s" % local_dataset_instance.proposalId)
        local_dataset_instance.proposalId = proposal_data.get("proposalId")
    else:
        raise RuntimeError("Proposal ID is not set in the variables.")

    if local_dataset_instance.sampleId is not None:
        if local_dataset_instance.sampleId.strip() == "":
            local_dataset_instance.sampleId =  f"UNKNOWN_{local_dataset_instance.proposalId}"
            logger.debug("Sample ID is empty. Setting it to default: %s", local_dataset_instance.sampleId)
        sample_data = get_sample_by_id(
            local_dataset_instance.sampleId, config.scicat, logger
        )
        if not sample_data:
            sample_data = create_sample(
                local_dataset=local_dataset_instance,
                config=config.scicat,
                logger=logger,
            )
    else:
        raise RuntimeError("Sample ID is not set in the variables.")

    local_dataset_instance.pid = _generate_or_get_dataset_pid(
        local_dataset_instance, config.scicat, logger
    )
    return local_dataset_instance


def _process_single_file(nexus_file_path: Path, schemas: OrderedDict[str, MetadataSchema], config: OfflineIngestorConfig, logger: logging.Logger) -> bool:
    try:
        fh_options = config.ingestion.file_handling
        nexus_file_path = Path(config.nexus_file)
        logger.debug("Nexus file to be ingested: %s", nexus_file_path)

        # Path to the directory where the ingestor saves the files it creates
        ingestor_directory = compose_ingestor_directory(fh_options, nexus_file_path)
        logger.debug("Ingestor directory: %s", ingestor_directory)

        # open nexus file with h5py
        with h5py.File(nexus_file_path) as h5file:
            # load instrument metadata configuration
            metadata_schema = select_applicable_schema(nexus_file_path, schemas)
            logger.debug(
                "Metadata Schema selected : %s (Id: %s)",
                metadata_schema.name,
                metadata_schema.id,
            )

            # define variables values
            variable_map = extract_variables_values(
                metadata_schema.variables, h5file, config, metadata_schema.id
            )

        data_file_list = create_data_file_list(
            nexus_file=nexus_file_path,
            ingestor_directory=ingestor_directory,
            config=fh_options,
            source_folder=variable_map["source_folder"]["value"] if "source_folder" in variable_map else None,
            logger=logger,
            # TODO: add done_writing_message_file and nexus_structure_file
        )

        # Prepare scicat dataset instance(entry)
        logger.debug("Preparing scicat dataset instance ...")
        local_dataset_instance = create_scicat_dataset_instance(
            metadata_schema=metadata_schema.schema,
            variable_map=variable_map,
            data_file_list=data_file_list,
            config=config.dataset,
            logger=logger,
        )

        config.scicat.token = login(config.scicat, logger)

        if local_dataset_instance.creationLocation == "ILL":
            local_dataset_instance = _process_ill_dataset(
                local_dataset_instance, nexus_file_path, variable_map, config, logger
            )
        else:
            # Check if dataset already exists in SciCat
            if _check_if_dataset_exists_by_pid(
                local_dataset_instance, config.ingestion, config.scicat, logger
            ) or _check_if_dataset_exists_by_metadata(
                local_dataset_instance, config.ingestion, config.scicat, logger
            ):
                raise RuntimeError(
                    "Dataset with pid %s already present in SciCat. Skipping it!!!",
                    local_dataset_instance.pid,
                )

        # If dataset does not exist, continue with the creation of the dataset
        local_dataset = scicat_dataset_to_dict(local_dataset_instance)
        logger.debug("Scicat dataset: %s", local_dataset)

        # Prepare origdatablock
        logger.debug("Preparing scicat origdatablock instance ...")
        local_origdatablock = origdatablock_to_dict(
            create_origdatablock_instance(
                data_file_list=data_file_list,
                scicat_dataset=local_dataset,
                config=fh_options,
            )
        )
        logger.debug("Scicat origdatablock: %s", local_origdatablock)

        # Create dataset in scicat
        if config.ingestion.dry_run:
            logger.info(
                "Dry run mode. Skipping Scicat API calls for creating dataset ..."
            )
            raise RuntimeError("Dry run mode. Skipping Scicat API calls for creating dataset.")
        else:
            if _check_if_dataset_exists_by_pid(
                local_dataset_instance, config.ingestion, config.scicat, logger
            ):
                scicat_dataset = patch_scicat_dataset(
                    dataset=local_dataset, config=config.scicat, logger=logger
                )
            else:
                scicat_dataset = create_scicat_dataset(
                    dataset=local_dataset, config=config.scicat, logger=logger
                )

            if check_origdatablock_by_datasetId(
                datasetId=local_origdatablock.get("datasetId", None), config=config.scicat, logger=logger
            ):
                scicat_origdatablock = patch_scicat_origdatablock(
                    origdatablock=local_origdatablock, config=config.scicat, logger=logger
                )
            else:
                scicat_origdatablock = create_scicat_origdatablock(
                    origdatablock=local_origdatablock, config=config.scicat, logger=logger
                )

            patch_scicat_dataset_numfiles(
                datasetId=scicat_dataset.get("pid", None),
                numfiles=len(scicat_origdatablock.get("dataFileList", [])),
                config=config.scicat,
                logger=logger,
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

            # check one more time if we successfully created the entries in scicat
            if not (bool(scicat_dataset) and bool(scicat_origdatablock)):
                raise RuntimeError("Failed to create dataset or origdatablock.")
            
        logout(config.scicat, logger)
        return True        
    except Exception as e:
        logger.error("Failed to process file %s: %s", nexus_file_path, str(e))
        logout(config.scicat, logger)
        return False

def main() -> None:
    """Main entry point of the app."""
    tmp_config = build_offline_config()
    logger = build_logger(tmp_config)
    config = build_offline_config(logger=logger)

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.debug(
        'Starting the Scicat background Ingestor with the following configuration: %s',
        config.to_dict(),
    )

    # Collect all metadata schema configurations
    schemas = collect_schemas(config.ingestion.schemas_directory)

    # Determine files to process
    path = Path(config.nexus_file)
    if not path.exists():
        logger.error("Path %s does not exist.", path)
        exit(logger, unexpected=True)
        
    files_to_process = []
    
    if path.is_dir():
        # Process directory recursively
        logger.info("Processing directory: %s", path)
        # Find all .nxs files recursively
        for nexus_file in path.glob('**/*.nxs'):
            files_to_process.append(nexus_file)
        logger.info("Found %d Nexus files to process", len(files_to_process))
    else:
        # Process single file
        files_to_process.append(path)
        
    if not files_to_process:
        logger.warning("No files found to process")
        exit(logger, unexpected=False)
        
    # Track overall success
    success_count = 0
    total_files = len(files_to_process)
    
    # Process each file
    for index, nexus_file_path in enumerate(files_to_process):
        logger.info(
            "Processing file %d of %d: %s", 
            index + 1, 
            total_files, 
            nexus_file_path
        )
        
        # Create a modified config with the current file path
        file_config = config
        file_config.nexus_file = str(nexus_file_path)
        
        try:
            # Process the file
            result = _process_single_file(nexus_file_path, schemas, file_config, logger)
            if result:
                success_count += 1
                logger.info("Successfully processed file: %s", nexus_file_path)
        except Exception as e:
            logger.error(
                "Error processing file %s: %s", 
                nexus_file_path, 
                str(e), 
                exc_info=True
            )
    
    # Report results
    logger.info(
        "Processing complete. Successfully processed %d of %d files.", 
        success_count, 
        total_files
    )
    
    # Exit with status based on overall success
    exit(logger, unexpected=(success_count == 0 and total_files > 0))

if __name__ == "__main__":
    main()
