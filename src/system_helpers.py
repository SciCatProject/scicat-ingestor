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
    """Exit the program if an exception is raised.
    This context manager is used in the main loop of the daemon.

    <If an exception is raised>
    If the exception is in the ``safe_exit_type`` tuple,
    the program exits without printing the traceback.

    If the exception is in the ``ignored_exceptions`` tuple,
    the program logs the exception and continues the loop.

    If the exception is not in the above two tuples,
    the program exits and prints the traceback of the exception.

    <If there is no exception>
    Daemon is not expected to exit even if there is *NO* exception.
    Therefore ``else`` block is used to exit the program
    if the loop finishes unexpectedly.

    Params
    ------
    logger:
        The logger object.
    safe_exit_type:
        The tuple of exceptions that should not print the traceback.
        Default is ``(KeyboardInterrupt,)``.
    ignored_exceptions:
        The tuple of exceptions that should be ignored.
        Default is an empty tuple.

    """
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
        logger.exception("An exception occurred: %s", e)
        exit(logger, unexpected=True)
    else:
        logger.error("Loop finished unexpectedly.")
        exit(logger, unexpected=True)


@contextmanager
def handle_exceptions(
    logger: logging.Logger,
) -> Generator[None, None, None]:
    """Exit the program if an exception is raised.

    If an exception is raised, the program logs the error and exits with status code 1.
    If no exception is raised, the program exits with status code 0.
    """
    try:
        yield
    except Exception as e:
        logger.exception("An exception occurred: %s", e)
        exit(logger, unexpected=True)
    else:
        exit(logger, unexpected=False)
