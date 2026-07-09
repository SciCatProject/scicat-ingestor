# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
import logging
import pathlib
import sys
from collections.abc import Generator
from contextlib import contextmanager
from types import TracebackType


def exit(logger: logging.Logger, unexpected: bool = True) -> None:
    """Log the message and exit the program."""
    import sys

    logger.info("Exiting ingestor.")
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
        logger.warning(
            "An exception occurred, "
            "but it is in the ignored list: %s . Ignored exception is: %s",
            ignored_exceptions,
            ignored_err,
        )
    except Exception as e:
        logger.exception("An exception occurred: %s", e, stacklevel=3)
        exit(logger, unexpected=True)
    else:
        logger.error("Loop finished unexpectedly.")
        exit(logger, unexpected=True)


def handle_exceptions(
    logger: logging.Logger | None = None,
    stacklevel: int = 2,
):
    """Overwrite sys.excepthook to log exceptions.

    This function will overwrite `sys.excepthook` with custom logging function
    that also logs the location of the last executed line.

    If the logger is not provided or it does not have any handlers,
    the default excepthook will be used to handle the exception.
    It allows to handle global exceptions without try-except statement.
    """
    original_excepthook = sys.excepthook

    def _handle_exceptions(
        exc_type: type[Exception], exc_value: Exception, exc_traceback: TracebackType
    ):
        if isinstance(logger, logging.Logger) and logger.handlers:
            # Manually extracting the file name and line number.
            # The formatter only shows the line number where the logger.exception
            # is called.
            file_name = pathlib.Path(exc_traceback.tb_frame.f_code.co_filename).name
            lineno = exc_traceback.tb_lasti
            logger.exception(
                "Unexpected error occurred: [%s:%s] %s",
                file_name,
                lineno,
                exc_value,
                exc_info=(exc_type, exc_value, exc_traceback),
                stacklevel=stacklevel,
            )
        else:
            original_excepthook(exc_type, exc_value, exc_traceback)

    sys.excepthook = _handle_exceptions
