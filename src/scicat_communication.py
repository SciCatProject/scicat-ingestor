# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import json
import logging
from dataclasses import asdict
from typing import Any
from urllib.parse import quote_plus, urljoin
import dateutil.parser
import copy

import requests
from scicat_configuration import SciCatOptions

def login(config: SciCatOptions, logger: logging.Logger) -> str:
    response = requests.post(
        config.urls.login, json=asdict(config.auth), headers=config.headers, timeout=config.timeout
    )
    if not response.ok:
        logger.error(
            "Failed to login to SciCat. \nError message from scicat backend: \n%s",
            response.json(),
        )
        raise ScicatDatasetAPIError("Error logging in to SciCat")
    return response.json()["access_token"]

def logout(config: SciCatOptions, logger: logging.Logger) -> str:
    response = requests.post(
        config.urls.logout, json=asdict(config.auth), headers=config.headers, timeout=config.timeout
    )
    if not response.ok:
        logger.error(
            "Failed to logout from SciCat. \nError message from scicat backend: \n%s",
            response.json(),
        )
        raise ScicatDatasetAPIError("Error logging out from SciCat")

def retrieve_value_from_scicat(
    *,
    config: SciCatOptions,
    scicat_endpoint_url: str,  # It should be already rendered
    # from variable_recipe["url"]
    field_name: str,  # variable_recipe["field"]
) -> str:
    response: dict = requests.get(
        scicat_endpoint_url, headers=config.headers, timeout=config.timeout
    ).json()
    return response[field_name] if field_name else response


class ScicatDatasetAPIError(Exception):
    pass


def _get_from_scicat(
    *, url: str, headers: dict, timeout: int, stream: bool, verify: bool
) -> requests.Response:
    return requests.get(url, headers=headers, timeout=timeout)


def _post_to_scicat(*, url: str, posting_obj: dict, headers: dict, timeout: int):
    return requests.request(
        method="POST",
        url=url,
        json=posting_obj,
        headers=headers,
        timeout=timeout,
        stream=False,
        verify=True,
    )


def create_scicat_dataset(
    *, dataset: dict, config: SciCatOptions, logger: logging.Logger
) -> dict:
    """
    Execute a POST request to scicat to create a dataset
    """
    logger.debug("Sending POST request to create new dataset")
    response = _post_to_scicat(
        url=config.urls.datasets,
        posting_obj=dataset,
        headers=config.headers,
        timeout=config.timeout,
    )
    result: dict = response.json()
    if not response.ok:
        logger.error(
            "Failed to create new dataset. \nError message from scicat backend: \n%s",
            result.get("error", {}),
        )
        raise ScicatDatasetAPIError(f"Error creating new dataset: \n{dataset}")

    logger.debug(
        "Dataset created successfully. Dataset pid: %s",
        result.get("pid"),
    )
    return result

def patch_scicat_dataset(
    *, dataset: dict, config: SciCatOptions, logger: logging.Logger
) -> dict:
    """
    Execute a PATCH request to scicat to update a dataset
    """
    logger.debug("Sending PATCH request to update dataset")
    current_dataset = get_dataset_by_pid(dataset['pid'], config, logger)
    if not current_dataset:
        logger.error("Dataset with pid %s does not exist", dataset['pid'])
        raise ScicatDatasetAPIError(f"Error updating dataset: \n{dataset}")

    # Format the dataset object to be sent to the backend
    patch_dataset = copy.deepcopy(dataset)
    if 'creation_time' in patch_dataset:
        patch_dataset['creation_time'] = min(
            dateutil.parser.parse(patch_dataset['creation_time']),
            dateutil.parser.parse(current_dataset['creation_time'])
        ).isoformat()
    if 'scientificMetadata' in patch_dataset:
        if 'start_time' in patch_dataset['scientificMetadata']:
            patch_dataset['scientificMetadata']['start_time']['value'] = min(
                dateutil.parser.parse(patch_dataset['scientificMetadata']['start_time']['value']),
                dateutil.parser.parse(current_dataset['scientificMetadata']['start_time']['value'])
            ).isoformat()
        if 'end_time' in patch_dataset['scientificMetadata']:
            patch_dataset['scientificMetadata']['end_time']['value'] = max(
                dateutil.parser.parse(patch_dataset['scientificMetadata']['end_time']['value']),
                dateutil.parser.parse(current_dataset['scientificMetadata']['end_time']['value'])
            ).isoformat()
        if 'duration' in patch_dataset['scientificMetadata']:
            current_value = current_dataset.get('scientificMetadata', {}).get('duration', {}).get('value', {})
            patch_dataset['scientificMetadata']['duration']['value'] = {
                **current_value,
                **patch_dataset['scientificMetadata']['duration']['value']
            }
        if 'sampleProperties' in patch_dataset['scientificMetadata']:
            if 'additional_environment' in patch_dataset['scientificMetadata']['sampleProperties']['value']:
                current_value = current_dataset.get('scientificMetadata', {}).get('sampleProperties', {}).get('value', {})
                patch_dataset['scientificMetadata']['sampleProperties']['value'] = {
                    **current_value,
                    **patch_dataset['scientificMetadata']['sampleProperties']['value']
                }
    patch_dataset.pop("pid", None)
    patch_dataset.pop("type", None)
    patch_dataset['numberOfFiles'] = current_dataset['numberOfFiles']
    patch_dataset['size'] = current_dataset['size']

    response = requests.patch(
        urljoin(config.host_address, f"{config.api_endpoints.datasets}/{quote_plus(dataset['pid'])}"),
        json=patch_dataset,
        headers=config.headers,
        timeout=config.timeout,
    )
    result: dict = response.json()
    if not response.ok:
        logger.error(
            "Failed to update dataset. \nError message from scicat backend: \n%s",
            result.get("error", {}),
        )
        raise ScicatDatasetAPIError(f"Error updating dataset: \n{dataset}")

    logger.debug(
        "Dataset updated successfully. Dataset pid: %s",
        result.get("pid"),
    )
    return result

def patch_scicat_dataset_numfiles(
    *, datasetId:str, numfiles:int, config: SciCatOptions, logger: logging.Logger
):
    patch_dataset = {
        "numberOfFiles": numfiles,
    }
    response = requests.patch(
        urljoin(config.host_address, f"{config.api_endpoints.datasets}/{quote_plus(datasetId)}"),
        json=patch_dataset,
        headers=config.headers,
        timeout=config.timeout,
    )
    result: dict = response.json()
    if not response.ok:
        logger.error(
            "Failed to update dataset. \nError message from scicat backend: \n%s",
            result.get("error", {}),
        )
        raise ScicatDatasetAPIError(f"Error updating dataset numberOfFiles: \n{datasetId}")

    logger.debug(
        "Dataset numberOfFiles updated successfully. Dataset pid: %s",
        result.get("pid"),
    )
    return result

def patch_scicat_origdatablock(*, origdatablock: dict, config: SciCatOptions, logger: logging.Logger) -> dict:
    current_origdatablock = get_origdatablock_by_datasetId(origdatablock['datasetId'], config, logger)[0]
    if not current_origdatablock:
        logger.error("Origdatablock with datasetId %s does not exist", origdatablock['datasetId'])
        raise ScicatOrigDatablockAPIError(f"Error updating origdatablock: \n{origdatablock}")
    request_body = {
        "dataFilesToAppend": origdatablock["dataFileList"]
    }
    response = requests.patch(
        urljoin(
            config.host_address,
            f"{config.api_endpoints.origdatablocks}/{quote_plus(current_origdatablock['_id'])}/appendFiles"
        ),
        json=request_body,
        headers=config.headers,
        timeout=config.timeout,
    )
    result = response.json()
    if not response.ok:
        logger.error(
            "Failed to append new files to existing dataFileList. \nError from scicat backend: \n%s",
            result.get("error", {}),
        )
        raise ScicatOrigDatablockAPIError(
            f"Error appending new files to existing dataFileList: \n{origdatablock}"
        )
    logger.debug(
        "Append request successful. The updated origdatablock pid: %s",
        result.get("_id"),
    )
    return result

class ScicatOrigDatablockAPIError(Exception):
    pass


def create_scicat_origdatablock(
    *, origdatablock: dict, config: SciCatOptions, logger: logging.Logger
) -> dict:
    """
    Execute a POST request to scicat to create a new origdatablock
    """
    logger.debug("Sending POST request to create new origdatablock")
    response = _post_to_scicat(
        url=config.urls.origdatablocks,
        posting_obj=origdatablock,
        headers=config.headers,
        timeout=config.timeout,
    )
    result: dict = response.json()
    if not response.ok:
        logger.error(
            "Failed to create new origdatablock. "
            "Error message from scicat backend: \n%s",
            result.get("error", {}),
        )
        raise ScicatOrigDatablockAPIError(
            f"Error creating new origdatablock: \n{origdatablock}"
        )

    logger.debug(
        "Origdatablock created successfully. Origdatablock pid: %s",
        result['_id'],
    )
    return result

def create_instrument(
    *, instrument_name: str, unique_name: str, config: SciCatOptions, logger: logging.Logger
) -> dict:
    """
    Execute a POST request to scicat to create a new instrument
    """
    logger.debug("Sending POST request to create new instrument")
    instrument_obj = {
        "name": instrument_name,
        "uniqueName": unique_name,
    }
    
    response = _post_to_scicat(
        url=config.urls.instruments,
        posting_obj=instrument_obj,  # Send formatted object instead of string
        headers=config.headers,
        timeout=config.timeout,
    )
    result: dict = response.json()
    if not response.ok:
        logger.error(
            "Failed to create new instrument. "
            "Error message from scicat backend: \n%s",
            result.get("error", {}),
        )
        raise ScicatOrigDatablockAPIError(
            f"Error creating new instrument: \n{instrument_name}"
        )

    logger.debug(
        "Instrument created successfully. Instrument pid: %s",
        result['_id'],
    )
    return result

def create_proposal(
    *, local_dataset: dict, config: SciCatOptions, logger: logging.Logger
) -> dict:
    """
    Execute a POST request to scicat to create a new proposal
    """
    logger.debug("Sending POST request to create new proposal")

    proposal_obj = {
        "proposalId": local_dataset.proposalId,
        "email": local_dataset.ownerEmail,
        "lastname": local_dataset.owner,
        "title": local_dataset.datasetName,
        "ownerGroup": local_dataset.ownerGroup,
        "MeasurementPeriodList": [
            {
                "instrument": local_dataset.instrumentId,
                "start": local_dataset.scientificMetadata.get('start_time').get('value'),
                "end": local_dataset.scientificMetadata.get('end_time').get('value'),
                "comment": local_dataset.datasetName,
            }
        ],
    }
    
    response = _post_to_scicat(
        url=config.urls.proposals,
        posting_obj=proposal_obj,
        headers=config.headers,
        timeout=config.timeout,
    )
    result: dict = response.json()
    if not response.ok:
        logger.error(
            "Failed to create new proposal. "
            "Error message from scicat backend: \n%s",
            result.get("error", {}),
        )
        raise ScicatOrigDatablockAPIError(
            f"Error creating new proposal: \n{proposal_obj['proposalId']}"
        )

    logger.debug(
        "Proposal created successfully. Proposal pid: %s",
        result['_id'],
    )
    return result

def create_sample(
    *, local_dataset, config: SciCatOptions, logger: logging.Logger
) -> dict:
    """
    Execute a POST request to scicat to create a new sample
    """
    logger.debug("Sending POST request to create new sample")

    sample_obj = {
        "sampleId": local_dataset.sampleId,
        "ownerGroup": local_dataset.ownerGroup,
        "description": "Sample for dataset " + local_dataset.datasetName,
    }

    response = _post_to_scicat(
        url=config.urls.samples,
        posting_obj=sample_obj,
        headers=config.headers,
        timeout=config.timeout,
    )
    result = response.json()
    if not response.ok:
        if result is dict:
            error_message = result.get("error", {})
        else:
            error_message = result
        logger.error(
            "Failed to create new sample. "
            "Error message from scicat backend: \n%s",
            error_message,
        )
        raise ScicatOrigDatablockAPIError(
            f"Error creating new sample: \n{sample_obj}"
        )
    logger.debug(
        "Sample created successfully. Sample pid: %s",
        result['_id'],
    )
    return result

def render_full_url(url: str, config: SciCatOptions) -> str:
    urls = asdict(config.urls)
    if not url.startswith("http://") and not url.startswith("https://"):
        for endpoint in urls.keys():
            if url.startswith(endpoint):
                return url.replace(endpoint, urls[endpoint])
    return url


def get_dataset_by_pid(
    pid: str, config: SciCatOptions, logger: logging.Logger
) -> dict | None:
    response = _get_from_scicat(
        url=urljoin(config.host_address, f"{config.api_endpoints.datasets}/{quote_plus(pid)}"),
        headers=config.headers,
        timeout=config.timeout,
        stream=config.stream,
        verify=config.verify,
    )
    
    if response.ok:
        logger.debug("Dataset with pid %s retrieved successfully", pid)
        return response.json()
    elif response.status_code == 403:
        logger.debug("Dataset with pid %s does not exist", pid)
        return None
    else:
        logger.warning(
            "Failed to retrieve dataset by pid %s\n"
            "with status code: %s\n"
            "Error message from scicat backend: %s",
            pid,
            response.status_code,
            response.reason,
        )
        return None

def check_dataset_by_pid(
    pid: str, config: SciCatOptions, logger: logging.Logger
) -> bool:
    dataset = get_dataset_by_pid(pid, config, logger)
    return dataset is not None


def check_dataset_by_metadata(
    metadata_key: str,
    metadata_value: Any,
    config: SciCatOptions,
    logger: logging.Logger,
) -> bool:
    metadata_dict = {f"scientificMetadata.{metadata_key}.value": metadata_value}
    filter_string = '?filter={"where":' + json.dumps(metadata_dict) + "}"
    url = urljoin(config.host_address, "datasets") + filter_string
    logger.debug("Checking if dataset exists by metadata key: %s", metadata_key)
    response = _get_from_scicat(
        url=url,
        headers=config.headers,
        timeout=config.timeout,
        stream=config.stream,
        verify=config.verify,
    )

    # Log the response
    dataset_exists = response.ok
    if response.ok:
        logger.debug("Retrieved %s dataset(s) from SciCat", len(response.json()))
        logger.debug("Dataset with metadata %s exists.", metadata_dict)
    # Filter 403 error code.
    # Scicat returns 403 error code when the file does not exist.
    # This function is trying to check the existence of the dataset,
    # therefore 403 error code should not be considered as an error.
    elif response.status_code == 403:
        logger.debug("Dataset with metadata %s does not exist.", metadata_dict)
    else:
        logger.error(
            "Failed to check dataset existence by metadata key %s \n"
            "with status code: %s \n"
            "Error message from scicat backend: \n%s\n"
            "Assuming the dataset does not exist.",
            metadata_key,
            response.status_code,
            response.reason,
        )
    return dataset_exists

def check_datafiles(
    datafiles: list[str], proposalId:str, config: SciCatOptions, logger: logging.Logger
) -> bool:
    # Create MongoDB-style query for dataFileList paths
    if not datafiles:
        logger.warning("No datafiles to check")
        return False
    if not proposalId:
        logger.error("ProposalId is missing")
        return False
    query = {
        "dataFileList.path": {"$in": datafiles}
    }
    
    fields_string = f'/fullquery?fields={json.dumps(query)}'
    url = urljoin(config.host_address, config.api_endpoints.origdatablocks) + fields_string
    
    logger.debug("Checking if datafiles exist by paths: %s", datafiles)
    response = _get_from_scicat(
        url=url,
        headers=config.headers,
        timeout=config.timeout,
        stream=config.stream,
        verify=config.verify,
    )

    if response.ok:
        results = response.json()
        found_files = []
        for block in results:
            if 'dataFileList' in block:
                if not block.get('datasetId').startswith(proposalId):
                    continue
                for file_info in block['dataFileList']:
                    if file_info.get('path') in datafiles:
                        found_files.append(file_info['path'])
        
        all_files_found = set(found_files) == set(datafiles)
        if all_files_found:
            logger.debug("All datafiles found in SciCat")
        else:
            missing = set(datafiles) - set(found_files)
            logger.debug("Missing datafiles: %s", list(missing))
        return all_files_found

    elif response.status_code == 403:
        logger.debug("Datafile(s) with paths %s do not exist.", datafiles)
        return False
    else:
        logger.error(
            "Failed to check datafile existence by paths %s\n"
            "with status code: %s\n"
            "Error message from scicat backend: %s\n"
            "Assuming the datafiles do not exist.",
            datafiles,
            response.status_code,
            response.reason,
        )
        return False
    
def check_origdatablock_by_datasetId(
    datasetId: str, config: SciCatOptions, logger: logging.Logger
)-> bool:
    origdatablock = get_origdatablock_by_datasetId(datasetId, config, logger)
    return origdatablock is not None and len(origdatablock) > 0

def get_instrument_by_name(
    instrument_name: str, config: SciCatOptions, logger: logging.Logger
):
    metadata_dict = {"name": instrument_name}
    filter_string = '?filter={"where":' + json.dumps(metadata_dict) + "}"
    url = urljoin(config.host_address, config.api_endpoints.instruments) + filter_string
    logger.debug("Checking if instrument exists by name: %s", instrument_name)
    response = _get_from_scicat(
        url=url,
        headers=config.headers,
        timeout=config.timeout,
        stream=config.stream,
        verify=config.verify,
    )

    # Log the response
    if response.ok:
        logger.debug("Retrieved %s instrument(s) from SciCat", len(response.json()))
        if len(response.json()) > 0:
            logger.debug("Instrument with name %s exists.", instrument_name)
        else:
            logger.debug("Instrument with name %s does not exist.", instrument_name)
        return response.json()
    # Filter 403 error code.
    # Scicat returns 403 error code when the file does not exist.
    # This function is trying to check the existence of the dataset,
    # therefore 403 error code should not be considered as an error.
    elif response.status_code == 403:
        logger.debug("Instrument with name %s does not exist.", instrument_name)
    else:
        logger.error(
            "Failed to check instrument existence by name %s \n"
            "with status code: %s \n"
            "Error message from scicat backend: \n%s\n"
            "Assuming the instrument does not exist.",
            instrument_name,
            response.status_code,
            response.reason,
        )
    return None

def get_instrument_nomad_id_by_name(
    instrument_name: str, config:SciCatOptions, logger: logging.Logger
):
    url = "https://scidata.ill.fr/api/instruments"
    logger.debug("Getting instrument nomad id by name: %s", instrument_name)
    response = requests.get(url, timeout=config.timeout)
    if response.ok:
        response_json = response.json()['data']
        instrument_nomad_id = [str(instrument['id']) for instrument in response_json 
                               if instrument['name'].lower().strip() == instrument_name.lower().strip()
                               ]
        if len(instrument_nomad_id) > 0:
            return instrument_nomad_id[0]
    else:
        logger.error(
            "Failed to get instrument nomad id by name %s \n"
            "with status code: %s \n"
            "Error message from scicat backend: \n%s",
            instrument_name,
            response.status_code,
            response.reason,
            )
    return None

def get_proposal_by_id(
    proposal_id: str, config: SciCatOptions, logger: logging.Logger
):
    if not proposal_id:
        raise ValueError("ProposalId is missing")
    url = urljoin(config.host_address, config.api_endpoints.proposals) + f"/{quote_plus(proposal_id)}"
    logger.debug("Checking if proposal exists by id: %s", proposal_id)
    response = _get_from_scicat(
        url=url,
        headers=config.headers,
        timeout=config.timeout,
        stream=config.stream,
        verify=config.verify,
    )

    # Log the response
    if response.ok:
        logger.debug("Retrieved %s proposal(s) from SciCat", len(response.json()))
        logger.debug("Proposal with id %s exists.", proposal_id)
        return response.json()
    # Filter 403 error code.
    # Scicat returns 403 error code when the file does not exist.
    # This function is trying to check the existence of the dataset,
    # therefore 403 error code should not be considered as an error.
    elif response.status_code == 403:
        logger.debug("Proposal with id %s does not exist.", proposal_id)
    else:
        logger.warning(
            "Failed to check proposal existence by id %s \n"
            "with status code: %s \n"
            "Error message from scicat backend: \n%s\n"
            "Assuming the proposal does not exist.",
            proposal_id,
            response.status_code,
            response.reason,
        )
    return None

def get_sample_by_id(
    sample_id: str, config: SciCatOptions, logger: logging.Logger
):
    url = urljoin(config.host_address, config.api_endpoints.samples) + f"/{quote_plus(sample_id)}"
    logger.debug("Checking if sample exists by id: %s", sample_id)
    response = _get_from_scicat(
        url=url,
        headers=config.headers,
        timeout=config.timeout,
        stream=config.stream,
        verify=config.verify,
    )

    # Log the response
    if response.ok:
        logger.debug("Retrieved %s sample(s) from SciCat", len(response.json()))
        logger.debug("Sample with id %s exists.", sample_id)
        return response.json()
    # Filter 403 error code.
    # Scicat returns 403 error code when the file does not exist.
    # This function is trying to check the existence of the dataset,
    # therefore 403 error code should not be considered as an error.
    elif response.status_code == 403:
        logger.debug("Sample with id %s does not exist.", sample_id)
    else:
        logger.warning(
            "Failed to check sample existence by id %s \n"
            "with status code: %s \n"
            "Error message from scicat backend: \n%s"
            "Assuming the sample does not exist.",
            sample_id,
            response.status_code,
            response.reason,
        )
    return None

def get_dataset_by_sample_id(
    sample_id: str, config: SciCatOptions, logger: logging.Logger
):
    url = urljoin(config.host_address, config.api_endpoints.samples) + f"/{quote_plus(sample_id)}/datasets"
    logger.debug("Checking if dataset exists by sample id: %s", sample_id)
    response = _get_from_scicat(
        url=url,
        headers=config.headers,
        timeout=config.timeout,
        stream=config.stream,
        verify=config.verify,
    )

    if response.ok:
        logger.debug("Retrieved %s dataset(s) from SciCat", len(response.json()))
        return response.json()
    elif response.status_code == 403:
        logger.debug("Sample with id %s does not exist.", sample_id)
    else:
        logger.error(
            "Failed to check dataset existence by sample id %s \n"
            "with status code: %s \n"
            "Error message from scicat backend: \n%s\n"
            "Assuming the dataset does not exist.",
            sample_id,
            response.status_code,
            response.reason,
        )
    return None

def get_origdatablock_by_datasetId(
    datasetId: str, config: SciCatOptions, logger: logging.Logger
):
    """
    Get origdatablock by datasetId, do not get datafiles for performance reasons
    """
    if not datasetId:
        logger.error("DatasetId is missing")
        return None
    filter_string = '?filter={"where":{"datasetId":"' + datasetId + '"},"fields":{"dataFileList":false}}'
    url = urljoin(config.host_address, config.api_endpoints.origdatablocks) + filter_string
    logger.debug("Checking if origdatablock exists by datasetId: %s", datasetId)
    response = _get_from_scicat(
        url=url,
        headers=config.headers,
        timeout=config.timeout,
        stream=config.stream,
        verify=config.verify,
    )
    dataset = response.json()
    if response.ok:
        logger.debug("Retrieved %s origdatablock(s) from SciCat", len(dataset))
        if len(dataset) > 0:
            logger.debug("Origdatablock with datasetId %s exists.", datasetId)
        else:
            logger.debug("Origdatablock with datasetId %s does not exist.", datasetId)
        return dataset
    # Filter 403 error code.
    # Scicat returns 403 error code when the file does not exist.
    # This function is trying to check the existence of the dataset,
    # therefore 403 error code should not be considered as an error.
    elif response.status_code == 403:
        logger.debug("Origdatablock with datasetId %s does not exist.", datasetId)
    else:
        logger.error(
            "Failed to check origdatablock existence by datasetId %s \n"
            "with status code: %s \n"
            "Error message from scicat backend: \n%s\n"
            "Assuming the origdatablock does not exist.",
            datasetId,
            response.status_code,
            response.reason,
        )
    return None
