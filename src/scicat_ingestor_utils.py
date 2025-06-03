import logging
from pathlib import Path
import re
from typing import OrderedDict
import os
import copy
import numpy as np
import requests
import urllib3

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
    update_scicat_origdatablock_filesList,
    create_scicat_origdatablock,
    create_instrument,
    create_proposal,
    create_sample,
)
from scicat_configuration import (
    IngestionOptions,
    OfflineIngestorConfig,
    SciCatOptions
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
from scicat_metadata import MetadataSchema
from scicat_path_helpers import compose_ingestor_directory
from typing import List, Union
import ast

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

def is_connection_error(exception: Exception) -> bool:
    """
    Determine if an exception is related to connection issues.
    
    Args:
        exception: The exception to check
        
    Returns:
        bool: True if it's a connection-related exception
    """
    # Check for common connection-related exceptions
    connection_errors = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        urllib3.exceptions.ProtocolError,
        ConnectionResetError,
        ConnectionRefusedError,
        ConnectionAbortedError,
    )
    
    # Direct type check
    if isinstance(exception, connection_errors):
        return True
    
    # Check for wrapped exceptions
    if hasattr(exception, '__context__') and exception.__context__ is not None:
        if isinstance(exception.__context__, connection_errors):
            return True
    
    # Check for common connection error messages
    error_str = str(exception).lower()
    connection_messages = [
        'connection reset by peer',
        'connection refused',
        'connection aborted',
        'connection timed out',
        'network is unreachable',
        'failed to establish a connection',
        'server disconnected',
    ]
    
    return any(msg in error_str for msg in connection_messages)

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

def _extract_instrument_properties(nexus_file_path: Path, h5file, metadata_schema: MetadataSchema, logger: logging.Logger) -> tuple[str, MetadataSchema]:
    """Extract instrument properties when `/entry0/instrument` is not present.
        
    Returns:
        tuple: (instrument_name, updated_metadata_schema)
    """
    instrument_name = None
    
    # First try to get instrument name from "/entry0/instrument_name"
    if "/entry0/instrument_name" in h5file:
        try:
            value = h5file["/entry0/instrument_name"][...]
            if isinstance(value, (list, np.ndarray)):
                # Use first item if it's an array
                instrument_name = value[0]
                if isinstance(instrument_name, bytes):
                    instrument_name = instrument_name.decode('utf-8')
            else:
                instrument_name = value
                if isinstance(instrument_name, bytes):
                    instrument_name = instrument_name.decode('utf-8')
            logger.debug(f"Found instrument name: {instrument_name}")
        except Exception as e:
            logger.warning(f"Error reading instrument_name: {str(e)}")
    
    # If no instrument name found, try to find it from path components
    if not instrument_name:
        try:
            path_components = [part.lower() for part in str(nexus_file_path).split('/')]
            
            if '/entry0' in h5file:
                entry_group = h5file['/entry0']
                for group_name in entry_group:
                    if not isinstance(entry_group[group_name], h5py.Group):
                        continue
                        
                    # Check if this group name matches any path component (case insensitive)
                    if group_name.lower() in path_components:
                        # Found a potential instrument group
                        instrument_group = f"/entry0/{group_name}"
                        logger.debug(f"Found potential instrument group: {instrument_group}")
                        
                        instrument_name = group_name
                        logger.debug(f"Using group name as instrument name: {instrument_name}")
                        break
                
                if not instrument_name:
                    raise RuntimeError("No instrument name found in file")
        except Exception as e:
            raise RuntimeError(f"Error searching for instrument groups: {str(e)}")
    
    # If instrument name found, update schema variables
    if instrument_name:
        modified_variables = copy.deepcopy(metadata_schema.variables)
        
        # Update paths in all variables
        for var_name, var_config in modified_variables.items():
            if hasattr(var_config, 'path'):
                # Replace XXX with instrument name
                if "XXX" in var_config.path:
                    var_config.path = var_config.path.replace("XXX", instrument_name)
                elif "/entry0/instrument" in var_config.path:
                    var_config.path = var_config.path.replace("/entry0/instrument", f"/entry0/{instrument_name}")
        
        # Use the modified schema for variable extraction
        metadata_schema_copy = copy.deepcopy(metadata_schema)
        metadata_schema_copy.variables = modified_variables
        return instrument_name, metadata_schema_copy
    else:
        raise RuntimeError("No instrument name found in file")

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
    
def _generate_or_get_dataset_pid(
    local_dataset: ScicatDataset,
    scicat_config: SciCatOptions,
    logger: logging.Logger,
) -> str:
    """Generate a dataset PID or get it from SciCat
    if a dataset for same proposal and sampl already exists.
    """
    pid = f"{local_dataset.proposalId}-DS1"
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

def _is_valid_email(email: str) -> bool:
    """
    Validates if a string is in a valid email format.
    """
    if not email:
        return False
        
    # Standard email validation pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def parse_string_array(value: str) -> Union[List[str], str]:
    """
    Converts a string representation of an array back to an actual array.
    """
    if not isinstance(value, str):
        return value
        
    # Check if the string represents a list/array
    if (value.startswith('[') and value.endswith(']')):
        try:
            # Try using ast.literal_eval which is safer than eval()
            result = ast.literal_eval(value)
            if isinstance(result, list):
                return result
        except (SyntaxError, ValueError):
            # If that fails, try manual parsing
            try:
                # Remove brackets
                content = value[1:-1]
                # Split by commas, respecting quotes
                parts = re.findall(r'\'([^\']*?)\'|\"([^\"]*?)\"', content)
                # Extract the matches (either first or second group will contain the value)
                return [p[0] if p[0] else p[1] for p in parts if p[0] or p[1]]
            except Exception:
                # If all parsing fails, return original
                return value
    
    # Not an array representation
    return value

def _process_ill_dataset(
    local_dataset_instance: ScicatDataset,
    nexus_file_path: Path,
    variable_map: dict,
    config: OfflineIngestorConfig,
    logger: logging.Logger,
) -> ScicatDataset:
    if local_dataset_instance.datasetName == "Internal use":
        raise RuntimeError("Dataset name is set to 'Internal use'.")

    local_dataset_instance.accessGroups.append(local_dataset_instance.proposalId)
    
    #TEMP FIX
    if local_dataset_instance.ownerEmail[0] == '[':
        local_dataset_instance.ownerEmail = local_dataset_instance.ownerEmail.split("'")[1]

    if not _is_valid_email(local_dataset_instance.contactEmail):
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

def process_single_file(nexus_file_path: Path, metadata_schema: MetadataSchema, config: OfflineIngestorConfig, logger: logging.Logger, variable_map: dict = None) -> bool:
    try:
        fh_options = config.ingestion.file_handling
        logger.debug("Nexus file to be ingested: %s", nexus_file_path)

        # Path to the directory where the ingestor saves the files it creates
        ingestor_directory = compose_ingestor_directory(fh_options, nexus_file_path)
        logger.debug("Ingestor directory: %s", ingestor_directory)

        if variable_map is None:
            # open nexus file with h5py
            with h5py.File(nexus_file_path) as h5file:
                # load instrument metadata configuration
                logger.debug(
                    "Metadata Schema selected : %s (Id: %s)",
                    metadata_schema.name,
                    metadata_schema.id,
                )

                # If at ILL and no standard instrument path, extract instrument properties
                if metadata_schema.variables["creationLocation"].value == "ILL" and "/entry0/instrument" not in h5file:
                    instrument_name, metadata_schema = _extract_instrument_properties(
                        nexus_file_path, h5file, metadata_schema, logger
                    )

                # define variables values
                variable_map = extract_variables_values(
                    nexus_file_path, metadata_schema.variables, h5file, config, metadata_schema.id
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

        try:
            config.scicat.token = login(config.scicat, logger)
        except Exception as e:
            if is_connection_error(e):
                logger.error("Connection error when contacting SciCat server: %s", str(e))
                logger.error("Stopping processing as subsequent files will likely fail too")
                # Propagate a special connection error exception to terminate processing
                raise ConnectionError("Cannot connect to SciCat server") from e
            # Re-raise other exceptions
            raise

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

        # Clean dataset sampleProperties
        if "sampleProperties" in local_dataset:
            local_dataset["sampleProperties"] = {
                k: v for k, v in local_dataset["sampleProperties"].items() if v is not None and v != "" and v != "None"
            }
            if not local_dataset["sampleProperties"]:
                del local_dataset["sampleProperties"]

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
                scicat_origdatablock = update_scicat_origdatablock_filesList(
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
    except ConnectionError as e:
        # Don't call logout - connection is already broken
        # Re-raise to signal caller that we need to stop processing
        raise
    except Exception as e:
        # Check if this is a wrapped connection error
        if is_connection_error(e):
            logger.error("Connection error detected when processing file %s: %s", nexus_file_path, str(e))
            # Don't call logout - connection is already broken
            # Raise a ConnectionError to signal caller to stop processing
            raise ConnectionError("Connection error detected") from e
            
        logger.error("Failed to process file %s: %s", nexus_file_path, str(e))
        try:
            logout(config.scicat, logger)
        except Exception:
            # Ignore logout errors
            pass
        return False

