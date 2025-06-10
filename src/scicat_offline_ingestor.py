# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)

import logging
from pathlib import Path
import os
import copy
from scicat_configuration import (
    OfflineIngestorConfig,
    build_arg_parser,
    build_dataclass,
    merge_config_and_input_args,
)
from scicat_logging import build_logger
from scicat_metadata import collect_schemas, select_applicable_schema
from system_helpers import exit
from scicat_ingestor_utils import process_single_file


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

def main() -> None:
    """Main entry point of the app."""
    tmp_config = build_offline_config()
    logger = build_logger(tmp_config)
    config = build_offline_config(logger=logger)

    logger.debug(
        'Starting the Scicat background Ingestor with the following configuration: %s',
        config.to_dict(),
    )
    schemas = collect_schemas(config.ingestion.schemas_directory)
    
    # Track overall statistics    
    total_files = 0
    skipped_files = 0
    success_count = 0
    
    # Track directories with no applicable schemas
    directories_without_schemas = set()
    
    paths = config.nexus_file if isinstance(config.nexus_file, list) else [config.nexus_file]
    logger.info(f"Processing {len(paths)} specified paths")

    for path_str in paths:
        path = Path(path_str).resolve()
        try:
            path_exists = path.exists()
        except PermissionError:
            logger.error(f"Permission denied for path {path_str}, skipping.")
            continue
        if not path_exists:
            logger.error(f"Path {path} does not exist, skipping.")
            continue
            
        logger.info(f"Processing path: {path}")
        
        if path.is_dir():
            try:
                for root, _, files in os.walk(path, followlinks=True):
                    root_path = Path(root).resolve()

                    logger.info(f"Processing directory: {root}")

                    if root_path in directories_without_schemas:
                        nxs_count = sum(1 for f in files if f.endswith('.nxs'))
                        if nxs_count > 0:
                            total_files += nxs_count
                            skipped_files += nxs_count
                            logger.debug(f"Skipping {nxs_count} files in {root} - no applicable schemas")
                        continue
                    
                    # Find first .nxs file for quick schema applicability check
                    first_nexus = next((f for f in files if f.endswith('.nxs')), None)
                    if not first_nexus:
                        continue
                        
                    nexus_file_count = sum(1 for f in files if f.endswith('.nxs'))
                    total_files += nexus_file_count
                    
                    if total_files % 1000 == 0:
                        logger.info(f"Discovered {total_files} files so far...")
                        
                    # Check schema applicability using the first file
                    sample_file_path = os.path.join(root, first_nexus)
                    applicable_schema = None
                    
                    try:
                        applicable_schema = select_applicable_schema(sample_file_path, schemas)
                    except Exception as e:
                        logger.debug(f"Error checking schema applicability for {sample_file_path}: {str(e)}")
                    
                    if applicable_schema is None or "internalUse" in root:
                        logger.debug(f"No schema applies to directory: {root}, skipping all files")
                        directories_without_schemas.add(root_path)
                        skipped_files += nexus_file_count
                        continue
                    
                    # Process files in batches to avoid memory issues with large directories
                    # This avoids creating a huge list in memory
                    batch_size = 1000
                    processed = 0
                    
                    for nexus_file in (f for f in files if f.endswith('.nxs')):
                        nexus_file_path = Path(os.path.join(root, nexus_file))
                        file_config = copy.deepcopy(config)
                        file_config.nexus_file = nexus_file_path
                        
                        try:
                            result = process_single_file(nexus_file_path, applicable_schema, file_config, logger)
                            if result:
                                success_count += 1
                                if success_count % 100 == 0:
                                    logger.info(f"Successfully processed {success_count} files so far...")
                        except ConnectionError as e:
                            # Connection error occurred - exit processing completely
                            logger.error("Connection to SciCat server failed. Stopping processing.")
                            logger.error("Please check server status and try again later.")
                            exit(logger, unexpected=True)
                            
                        processed += 1
                        if processed % batch_size == 0:
                            logger.debug(f"Processed {processed}/{nexus_file_count} files in directory {root}")
            except ConnectionError as e:
                # Catch any connection errors that bubble up
                logger.error("Connection to SciCat server failed. Stopping processing.")
                logger.error("Please check server status and try again later.")
                exit(logger, unexpected=True)
        else:
            total_files = 1
            nexus_file_path = path
            
            applicable_schema = None
            try:
                applicable_schema = select_applicable_schema(nexus_file_path, schemas)
                if applicable_schema is None:
                    logger.warning(f"No schema applies to file: {nexus_file_path}")
                    skipped_files = 1
                else:
                    logger.debug(f"Using schema: {applicable_schema.name} (Id: {applicable_schema.id})")
                    file_config = config
                    result = process_single_file(nexus_file_path, applicable_schema, file_config, logger)
                    if result:
                        success_count = 1
            except ConnectionError as e:
                # Connection error occurred - exit processing completely
                logger.error("Connection to SciCat server failed. Stopping processing.")
                logger.error("Please check server status and try again later.")
                exit(logger, unexpected=True)
            except Exception as e:
                logger.error(f"Error processing file {nexus_file_path}: {str(e)}", exc_info=True)
    logger.info(
        "Processing complete. Processed %d files: %d successful, %d skipped due to no applicable schema.", 
        total_files, 
        success_count, 
        skipped_files
    )
    
    exit(logger, unexpected=(success_count == 0 and total_files > skipped_files > 0))

if __name__ == "__main__":
    main()
