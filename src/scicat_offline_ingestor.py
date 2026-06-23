# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)

import logging
from pathlib import Path

import h5py

from fallback_metadata_schema import get_fallback_schema
from scicat_communication import (
    check_dataset_by_metadata,
    check_dataset_by_pid,
    create_scicat_dataset,
    create_scicat_origdatablock,
    query_sample,
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
from scicat_metadata import (
    MetadataVariableValueSpec,
    SampleAttachmentConfig,
    collect_schemas,
    select_applicable_schema,
)
from scicat_nexus_helper import open_h5file
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
    del merged_configuration["health_check"]

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
        logger.info(
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
            logger.info(
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
            logger.info(
                "No value found for metadata key %s specified for checking dataset.",
                metadata_key,
            )
    else:
        logger.info("No metadata key specified for checking dataset existence.")

    # Other cases, assuming dataset does not exist
    return False


def _retrieve_source_folder(
    variable_map: dict[str, MetadataVariableValueSpec],
) -> str | None:
    """Retrieve the source folder from the variable map."""
    value = variable_map.get("source_folder", None)
    return value.value if isinstance(value, MetadataVariableValueSpec) else None


def read_sample_name(
    *,
    sample_attch_config: SampleAttachmentConfig,
    file: h5py.File,
    logger: logging.Logger | None = None,
) -> str | None:
    try:
        return file[sample_attch_config.sample_name_path][...].item().decode("utf-8")
    except Exception as err:
        if logger:
            logger.warning("Could not find sample name with error: %.2000s", err)
    return None


def read_proposal_id(
    *,
    sample_attch_config: SampleAttachmentConfig,
    file: h5py.File,
    logger: logging.Logger | None = None,
) -> str | None:
    try:
        return file[sample_attch_config.proposal_id_path][...].item().decode("utf-8")
    except Exception as err:
        if logger:
            logger.warning("Could not find proposal ID with error: %.2000s", err)
    return None


def maybe_sample_dataset_pid(
    *,
    sample_attch_config: SampleAttachmentConfig,
    file: h5py.File,
    scicat_config: SciCatOptions,
    logger: logging.Logger,
) -> list[str]:
    sample_dataset_pid = []
    if sample_attch_config.query_sample_name:
        sample_name = read_sample_name(
            sample_attch_config=sample_attch_config, file=file, logger=logger
        )
        proposal_id = read_proposal_id(
            sample_attch_config=sample_attch_config, file=file, logger=logger
        )
        if sample_name is None or proposal_id is None:
            if logger:
                logger.warning(
                    "Sample name or/and proposal ID not found. Cannot query the sample dataset PID."
                )
        else:
            sample_dataset_pid = query_sample(
                config=scicat_config,
                sample_name=sample_name,
                proposal_id=proposal_id,
                logger=logger,
            )
    return sample_dataset_pid


def main() -> None:
    """Main entry point of the app."""
    tmp_config = build_offline_config()
    logger = build_logger(tmp_config)
    config = build_offline_config(logger=logger)
    fh_options = config.ingestion.file_handling

    logger.info('Starting the Scicat background Ingestor.')

    # Collect all metadata schema configurations
    schemas = collect_schemas(config.ingestion.schemas_directory)
    fallback_schema = get_fallback_schema(config.ingestion.fallback_schema_file_path)

    logger.info("Found %s schemas", len(schemas))
    if fallback_schema:
        logger.debug("Found fallback schema: %s.", fallback_schema.name)

    with handle_exceptions(logger):
        nexus_file_path = Path(config.nexus_file)
        logger.info("Nexus file to be ingested: %s", nexus_file_path)

        # Path to the directory where the ingestor saves the files it creates
        ingestor_directory = compose_ingestor_directory(fh_options, nexus_file_path)

        # open nexus file with h5py
        with open_h5file(
            nexus_file_path,
            file_handling_config=config.ingestion.file_handling,
            logger=logger,
        ) as h5file:
            # load instrument metadata configuration
            metadata_schema = select_applicable_schema(
                nexus_file_path, schemas, logger=logger
            )
            logger.info(
                "Metadata Schema selected : %s (Id: %s)",
                metadata_schema.name,
                metadata_schema.id,
            )
            sample_dataset_pid_list = maybe_sample_dataset_pid(
                sample_attch_config=metadata_schema.sample_attachment,
                file=h5file,
                scicat_config=config.scicat,
                logger=logger,
            )

            # define variables values
            variable_map = extract_variables_values(
                variables=metadata_schema.variables,
                h5file=h5file,
                config=config,
                schema_id=metadata_schema.id,
                logger=logger,
            )

        data_file_list = create_data_file_list(
            nexus_file=nexus_file_path,
            ingestor_directory=ingestor_directory,
            config=fh_options,
            source_folder=_retrieve_source_folder(variable_map),
            logger=logger,
            # TODO: add done_writing_message_file and nexus_structure_file
        )

        # Prepare scicat dataset instance(entry)
        logger.info("Preparing scicat dataset instance.")
        local_dataset_instance = create_scicat_dataset_instance(
            metadata_schema=metadata_schema.schema,
            variable_map=variable_map,
            data_file_list=data_file_list,
            config=config.dataset,
            sample_dataset_pid_list=sample_dataset_pid_list,
            logger=logger,
        )

        # Check if dataset already exists in SciCat
        if _check_if_dataset_exists_by_pid(
            local_dataset_instance, config.ingestion, config.scicat, logger
        ) or _check_if_dataset_exists_by_metadata(
            local_dataset_instance, config.ingestion, config.scicat, logger
        ):
            logger.warning(
                "Dataset with pid %s already present in SciCat. Skipping it!!!",
                local_dataset_instance.pid,
            )
            exit(logger, unexpected=False)

        # If dataset does not exist, continue with the creation of the dataset
        local_dataset = scicat_dataset_to_dict(local_dataset_instance)
        logger.debug("Scicat dataset: %s", local_dataset)

        # Prepare origdatablock
        logger.info("Preparing scicat origdatablock instance ...")
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
            exit(logger, unexpected=False)
        else:
            scicat_dataset = create_scicat_dataset(
                dataset=local_dataset,
                config=config.scicat,
                logger=logger,
                data_file_path=nexus_file_path,
            )

            # create origdatablock in scicat
            scicat_origdatablock = create_scicat_origdatablock(
                origdatablock=local_origdatablock,
                config=config.scicat,
                logger=logger,
                data_file_path=nexus_file_path,
            )

            # check one more time if we successfully created the entries in scicat
            if not ((len(scicat_dataset) > 0) and (len(scicat_origdatablock) > 0)):
                logger.error(
                    "Failed to create dataset or origdatablock in scicat for file %s. "
                    "SciCat dataset: %s, SciCat origdatablock: %s",
                    nexus_file_path,
                    scicat_dataset,
                    scicat_origdatablock,
                )
                raise RuntimeError(
                    f"Failed to create dataset or origdatablock for file {nexus_file_path}"
                )

            # if we get here, both dataset and origdatablock have been created successfully
            logger.info(
                "Dataset ingestion successful. "
                "Data file: %s, "
                "Scicat dataset pid: %s, "
                "SciCat origdatablock id: %s",
                nexus_file_path,
                scicat_dataset.get('pid'),
                scicat_origdatablock.get('_id'),
            )

            exit(
                logger,
                unexpected=not (bool(scicat_dataset) and bool(scicat_origdatablock)),
            )
            raise RuntimeError(
                f"Error on existing offline ingestor for file {nexus_file_path}."
            )


if __name__ == "__main__":
    main()
