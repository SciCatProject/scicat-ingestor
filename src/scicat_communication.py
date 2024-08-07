# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
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
