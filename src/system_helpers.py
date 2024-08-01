import logging
from collections.abc import Generator
from contextlib import contextmanager


def exit(logger: logging.Logger, unexpected: bool = True) -> None:
    """Log the message and exit the program."""
    import sys

    logger.info("Exiting ingestor")
    sys.exit(1 if unexpected else 0)


@contextmanager
def online_ingestor_exit_at_exceptions(
    logger: logging.Logger, daemon: bool = True
) -> Generator[None, None, None]:
    """Exit the program if an exception is raised."""
    try:
        yield
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt.")
        exit(logger, unexpected=False)
    except Exception as e:
        logger.error("An exception occurred: %s", e)
        exit(logger, unexpected=True)
    else:
        if daemon:
            logger.error("Loop finished unexpectedly.")
            exit(logger, unexpected=True)
        else:
            logger.info("Finished successfully.")
            exit(logger, unexpected=False)

@contextmanager
def offline_ingestor_exit_at_exceptions(
        logger: logging.Logger
) -> Generator[None, None, None]:
    """
    manage exceptions specifically for offline ingestor
    """
    try:
        yield
    except Exception as e:
        logger.error("An exception occurred: %s", e)
    else:
        logger.error("An unexpected error occurred")

    exit(logger, unexpected=True)
