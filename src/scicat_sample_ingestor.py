# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
# ruff: noqa: E402

import importlib.metadata
import json
import logging
import multiprocessing as mp
import pathlib
import urllib.parse

from scicat_communication import _get_from_scicat, _post_to_scicat

PACKAGE_NAME = "scicat-ingestor"

try:
    __version__ = importlib.metadata.version(PACKAGE_NAME)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

del importlib
from multiprocessing.synchronize import Lock as LockType
from pathlib import Path

from scicat_configuration import (
    SampleIngestorConfig,
    SciCatOptions,
    build_arg_parser,
    build_dataclass,
    merge_config_and_input_args,
)
from scicat_kafka import build_consumer, run_start_messages
from scicat_logging import build_logger
from scicat_nexus_helper import open_h5file
from system_helpers import handle_daemon_loop_exceptions


def filter_child_processes(
    *, max_processes: int, child_processes: list[mp.Process], timeout: int | None = None
) -> list[mp.Process]:
    """Filter alive child processes and wait if there are too many processes."""
    # TODO: Commit individual messages here.
    cur_proccesses = [p for p in child_processes if p.is_alive()]
    if len(cur_proccesses) >= max_processes:
        cur_proccesses.pop().join(timeout=timeout)
    return [p for p in cur_proccesses if p.is_alive()]


def build_sample_ingestor_config(
    logger: logging.Logger | None = None,
) -> SampleIngestorConfig:
    arg_parser = build_arg_parser(
        SampleIngestorConfig, mandatory_args=('--config-file',)
    )
    arg_namespace = arg_parser.parse_args()
    merged_configuration = merge_config_and_input_args(
        Path(arg_namespace.config_file), arg_namespace
    )

    return build_dataclass(
        tp=SampleIngestorConfig,
        data=merged_configuration,
        logger=logger,
        strict=False,
    )


def _build_child_ingestor_config(config_file_path: str) -> SampleIngestorConfig:
    from scicat_configuration import _load_config

    config = _load_config(config_file=Path(config_file_path))
    config['config_file'] = config_file_path
    return build_dataclass(tp=SampleIngestorConfig, data=config, strict=False)


def check_sample(
    *, config: SciCatOptions, sample_name: str, proposal_id: str, logger: logging.Logger
) -> bool:
    query = urllib.parse.quote(
        string=json.dumps(
            {"where": {"description": sample_name, "proposalId": proposal_id}}
        )
    )
    response = _get_from_scicat(
        url=config.urls.samples + "?filter=" + query,
        headers=config.headers,
        timeout=config.timeout,
        stream=config.stream,
        verify=config.verify,
    )
    matching_samples = response.json()
    logger.debug("Matching Samples: %s", matching_samples)
    return response.ok and matching_samples


def post_sample(
    *, config: SciCatOptions, sample_name: str, proposal_id: str, logger: logging.Logger
) -> None:
    body = {
        "ownerGroup": proposal_id,
        "accessGroups": [proposal_id],
        "description": sample_name,
        "proposalId": proposal_id,
        "isPublished": False,
    }
    response = _post_to_scicat(
        url=config.urls.samples,
        headers=config.headers,
        timeout=config.timeout,
        posting_obj=body,
    )
    if not response.ok:
        logger.warning("Posting a sample failed: %s", response.json())
    else:
        logger.debug("Posting successful: %s", response.json())


def child_ingestor(
    *,
    instrument_lock: LockType,
    config_file_path: str,
    nexus_file_path: str,
) -> None:
    # Use a lock per instrument.
    # Sample dataset ingestions should be synchronized(preferably in order)
    # within instruments.
    with instrument_lock:
        config = _build_child_ingestor_config(config_file_path=config_file_path)
        logger = build_logger(config)
        logger.info("Processing nexus file at %s", nexus_file_path)
        with open_h5file(
            file_path=pathlib.Path(nexus_file_path),
            logger=logger,
            file_handling_config=config.ingestion.file_handling,
            accepted_exceptions=(OSError, BlockingIOError, FileNotFoundError),
        ) as nexus_file:
            sample_name = nexus_file['/entry/sample/name'][...].item().decode("utf-8")
            proposal_id = (
                nexus_file['/entry/experiment_identifier'][...].item().decode("utf-8")
            )
            logger.info("Sample name: %s, in Proposal: %s", sample_name, proposal_id)
            if not check_sample(
                config=config.scicat,
                sample_name=sample_name,
                proposal_id=proposal_id,
                logger=logger,
            ):
                post_sample(
                    config=config.scicat,
                    sample_name=sample_name,
                    proposal_id=proposal_id,
                    logger=logger,
                )


def offline_main() -> None:
    raise NotImplementedError("Manual Sample Dataset Ingestion Not Implemented Yet")


def main() -> None:
    """Main entry point of the app."""
    tmp_config = build_sample_ingestor_config()
    logger = build_logger(tmp_config)
    config = build_sample_ingestor_config(logger=logger)

    logger.info('Starting the Scicat online Sample Ingestor. Version: %s', __version__)
    instrument_name_locks: dict[str, LockType] = {}
    child_processes: list[mp.Process] = []

    with handle_daemon_loop_exceptions(logger=logger):
        # Kafka consumer
        if (consumer := build_consumer(config.kafka, logger)) is None:
            raise RuntimeError("Failed to build the Kafka consumer")

        # Start health server without affecting the online ingestor loop
        # TODO: start_health_server(config, consumer, logger)

        # Receive messages
        for message in run_start_messages(consumer, logger):
            logger.info("Processing message: %.88s", message)

            # Check if we have received a run start message.
            # ``message: None | RunStartInfo``
            if message:
                # extract job id
                job_id = message.job_id
                instrument_name = message.instrument_name
                instrument_lock = instrument_name_locks.setdefault(
                    instrument_name, mp.Lock()
                )
                if config.ingestion.dry_run:
                    logger.info("Dry run mode enabled. Skipping background ingestor.")
                    continue

                logger.info(
                    "Processing message received for instrument: %s, job id: %s",
                    instrument_name,
                    job_id,
                )

                child_processes = filter_child_processes(
                    max_processes=config.ingestion.max_offline_ingestors,
                    child_processes=child_processes,
                    timeout=config.ingestion.offline_ingestors_wait_time,
                )
                new_process = mp.Process(
                    target=child_ingestor,
                    kwargs={
                        'instrument_lock': instrument_lock,
                        'config_file_path': config.config_file,
                        'nexus_file_path': message.filename,
                    },
                )
                new_process.start()
                child_processes.append(new_process)


if __name__ == "__main__":
    main()
