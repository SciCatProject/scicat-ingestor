# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 ScicatProject contributors (https://github.com/ScicatProject)
import logging
from dataclasses import dataclass

from scicat_communication import _post_to_scicat
from scicat_configuration import SciCatOptions
from scicat_metadata import MetadataSchema, MetadataVariableValueSpec


@dataclass
class ScicatJobPostReport:
    ok: bool
    pid: str | None


def _post_scicat_job(
    *,
    payload: dict,
    config: SciCatOptions,
    logger: logging.Logger,
) -> ScicatJobPostReport:
    """
    Execute a POST request to scicat to create a new job instance.
    """
    logger.info("Sending POST request to create a new job instance.")
    response = _post_to_scicat(
        url=config.urls.jobs,
        posting_obj=payload,
        headers=config.headers,
        timeout=config.timeout,
    )
    result: dict = response.json()
    if not response.ok:
        pid = None
        msg = f"{response.status_code} {response.reason}: {response.text}"
        logger.error(
            "Failed to create a job. Error message from scicat backend: %s . Payload: %s",
            msg,
            payload,
        )
    else:
        pid = result['_id']
        logger.info("Job created successfully with job pid: %s", pid)

    return ScicatJobPostReport(ok=response.ok, pid=pid)


@dataclass
class ScicatJobPostSummary:
    ok: bool
    text: str

    @classmethod
    def from_reports(
        cls, results: dict[str, ScicatJobPostReport]
    ) -> "ScicatJobPostSummary":
        num_jobs = len(results)
        oks = [val.ok for val in results.values()]
        text = f"{sum(oks)}/{num_jobs} Jobs successfully created."
        return ScicatJobPostSummary(ok=all(oks), text=text)


def create_scicat_job(
    *,
    metadata_schema: MetadataSchema,
    scicat_config: SciCatOptions,
    scicat_dataset: dict,  # Final dataset created in scicat.
    variable_map: dict,
    logger: logging.Logger,
) -> ScicatJobPostSummary:
    # Post jobs according to the configuration.
    logger.debug(
        "%d Job(s) found in the schema config file.", len(metadata_schema.jobs)
    )
    # Update existing variables with information of the final dataset.
    updated_variable_map = {
        **variable_map,
        # PID may have been auto generated even though there is
        # schema variable defined by the user.
        # It depends on the configuration of scicat ingestor.
        "pid": MetadataVariableValueSpec(value=scicat_dataset.get("pid")),
    }
    results = {}
    for job_name, job_recipe in metadata_schema.jobs.items():
        logger.debug(
            "Preparing a job %s for dataset %s",
            job_name,
            scicat_dataset.get('pid'),
        )
        job_payload = job_recipe.payload(variable_registry=updated_variable_map)
        logger.debug("Job %s payload: %s", job_name, job_payload)
        results[job_name] = _post_scicat_job(
            payload=job_payload, config=scicat_config, logger=logger
        )
    return ScicatJobPostSummary.from_reports(results)
