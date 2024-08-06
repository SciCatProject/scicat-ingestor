# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import logging
from urllib.parse import urljoin

import requests
from scicat_configuration import SciCatOptions


def retrieve_value_from_scicat(
    *,
    config: SciCatOptions,
    variable_url: str,  # It should be already rendered from variable_recipe["url"]
    field_name: str,  # variable_recipe["field"]
) -> str:
    url = config.host.removesuffix('/') + variable_url
    response: dict = requests.get(
        url, headers={"token": config.token}, timeout=config.timeout
    ).json()
    return response[field_name]


class ScicatDatasetAPIError(Exception):
    pass


def create_scicat_dataset(
    *, dataset: dict, config: SciCatOptions, logger: logging.Logger
) -> dict:
    """
    Execute a POST request to scicat to create a dataset
    """
    logger.info("_create_scicat_dataset: Sending POST request to create new dataset")
    response = requests.request(
        method="POST",
        url=urljoin(config.host, "datasets"),
        json=dataset,
        headers={"token": config.token, **config.headers},
        timeout=config.timeout,
        stream=False,
        verify=True,
    )

    result: dict = response.json()
    if not response.ok:
        logger.error(
            "Failed to create new dataset. \nError message from scicat backend: \n%s",
            result.get("error", {}),
        )
        raise ScicatDatasetAPIError(f"Error creating new dataset: \n{dataset}")

    logger.info(
        "Dataset created successfully. Dataset pid: %s",
        result.get("pid"),
    )
    return result
