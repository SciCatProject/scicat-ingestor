# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import h5py

from scicat_configuration import FileHandlingOptions


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
    except (OSError, BlockingIOError) as e:
        if len(retry_delays) > 0:
            cur_retry = retry_delays[0]
            logger.info(
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
                retry_delays=retry_delays[1:],
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
    _MAX_RETRY_DELAYS = 120
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
