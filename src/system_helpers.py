# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
import logging
from collections.abc import Generator
from contextlib import contextmanager


def exit(logger: logging.Logger, unexpected: bool = True) -> None:
    """Log the message and exit the program."""
    import sys

    logger.info("Exiting ingestor")
    sys.exit(1 if unexpected else 0)


@contextmanager
def handle_daemon_loop_exceptions(
    *,
    logger: logging.Logger,
    safe_exit_type: tuple[type[BaseException], ...] = (KeyboardInterrupt,),
    ignored_exceptions: tuple[type[BaseException], ...] = (),
) -> Generator[None, None, None]:
    """Exit the program if an exception is raised."""
    try:
        yield
    except safe_exit_type:
        logger.info("Received keyboard interrupt.")
        exit(logger, unexpected=False)
    except ignored_exceptions as ignored_err:
        logger.error(
            "An exception occurred, "
            "but it is in the ignored list: %s \n. Ignored exception is: %s",
            ignored_exceptions,
            ignored_err,
        )
    except Exception as e:
        logger.error("An exception occurred: %s", e)
        exit(logger, unexpected=True)
    else:
        logger.error("Loop finished unexpectedly.")
        exit(logger, unexpected=True)


@contextmanager
def handle_exceptions(
    logger: logging.Logger,
) -> Generator[None, None, None]:
    "Exit the program if an exception is raised."
    try:
        yield
    except Exception as e:
        logger.error("An exception occurred: %s", e)
        exit(logger, unexpected=True)
    else:
        exit(logger, unexpected=False)
