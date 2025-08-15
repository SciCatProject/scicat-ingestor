# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)

import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import h5py

from scicat_communication import (
    check_dataset_by_metadata,
    check_dataset_by_pid,
    create_scicat_dataset,
    create_scicat_origdatablock,
)
from scicat_configuration import (
    FileHandlingOptions,
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
    collect_schemas,
    select_applicable_schema,
)
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


@contextmanager
def _open_h5file(
    *,
    file_path: Path,
    retry_delays: tuple[int, ...],
    logger: logging.Logger,
    _retried: int = 0,
) -> Generator[h5py.File, None, None]:
    import time

    try:
        # Instead of using context manager,
        # we are opening the file and close them manually.
        # It is because entering the context manager
        # as opening the file conflicts with the try-except block
        # that can result non-stopped generator error.
        # It happens when it doesn't fail opening the file but fails after.
        opened_file = h5py.File(file_path, 'r')
    except BlockingIOError as e:
        if len(retry_delays) > 0:
            cur_retry, next_retries = retry_delays[0], retry_delays[1:]
            logger.error(
                "Error opening HDF5 file %s at attempt #[%d]. Retrying in %d seconds",
                file_path,
                _retried + 1,
                cur_retry,
            )
            # Just time.sleep because one offline ingestor process
            # does not affect others.
            time.sleep(cur_retry)

            with _open_h5file(
                file_path=file_path,
                retry_delays=next_retries,
                logger=logger,
                _retried=_retried + 1,
            ) as h5file:
                yield h5file
        else:
            logger.error(
                "Failed to open HDF5 file after %d attempts: %s", _retried + 1, e
            )
            raise e
    else:
        yield opened_file
        opened_file.close()


@contextmanager
def open_h5file(
    file_path: Path,
    *,
    file_handling_config: FileHandlingOptions,
    logger: logging.Logger,
) -> Generator[h5py.File, None, None]:
    _MAX_RETRY_DELAYS = 15
    _MIN_RETRY_DELAYS = 1
    _DEFAULT_DELAY = 3

    def _wrap_retry_delay(delay: int) -> int:
        return max(_MIN_RETRY_DELAYS, min(_MAX_RETRY_DELAYS, delay))

    max_retries = file_handling_config.data_file_open_max_tries
    retry_delays = file_handling_config.data_file_open_retry_delay
    retry_delays = tuple(_wrap_retry_delay(delay) for delay in retry_delays)

    if len(retry_delays) == 0:
        retry_delays = (_DEFAULT_DELAY,) * max_retries
    elif len(retry_delays) < max_retries:
        # If the list of retry delays is shorter than the number of retries,
        # extend it with the last value.
        missing_length = max_retries - len(retry_delays)
        retry_delays = retry_delays + (retry_delays[-1],) * missing_length

    with _open_h5file(
        file_path=file_path, retry_delays=retry_delays, logger=logger
    ) as h5file:
        yield h5file


def main() -> None:
    """Main entry point of the app."""
    tmp_config = build_offline_config()
    logger = build_logger(tmp_config)
    config = build_offline_config(logger=logger)
    fh_options = config.ingestion.file_handling

    # Log the configuration as dictionary so that it is easier to read from the logs
    logger.info(
        'Starting the Scicat background Ingestor with the following configuration: %s',
        config.to_dict(),
    )

    # Collect all metadata schema configurations
    schemas = collect_schemas(config.ingestion.schemas_directory)
    logger.info("Found %s schemas", len(schemas))

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
            metadata_schema = select_applicable_schema(nexus_file_path, schemas)
            logger.info(
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
            source_folder=_retrieve_source_folder(variable_map),
            logger=logger,
            # TODO: add done_writing_message_file and nexus_structure_file
        )

        # Prepare scicat dataset instance(entry)
        logger.info("Preparing scicat dataset instance ...")
        local_dataset_instance = create_scicat_dataset_instance(
            metadata_schema=metadata_schema.schema,
            variable_map=variable_map,
            data_file_list=data_file_list,
            config=config.dataset,
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
                dataset=local_dataset, config=config.scicat, logger=logger
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

            # check one more time if we successfully created the entries in scicat
            exit(
                logger,
                unexpected=not (bool(scicat_dataset) and bool(scicat_origdatablock)),
            )
            raise RuntimeError("Failed to create dataset or origdatablock.")


if __name__ == "__main__":
    main()
