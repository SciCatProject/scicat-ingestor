import logging
from collections.abc import Generator
from contextlib import contextmanager


def quit(logger: logging.Logger, unexpected: bool = True) -> None:
    """Log the message and exit the program."""
    import sys

    logger.info("Exiting ingestor")
    sys.exit(1 if unexpected else 0)


@contextmanager
def exit_at_exceptions(
    logger: logging.Logger, daemon: bool = True
) -> Generator[None, None, None]:
    """Exit the program if an exception is raised."""
    try:
        yield
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt.")
        quit(logger, unexpected=False)
    except Exception as e:
        logger.error("An exception occurred: %s", e)
        quit(logger, unexpected=True)
    else:
        if daemon:
            logger.error("Loop finished unexpectedly.")
            quit(logger, unexpected=True)
        else:
            logger.info("Finished successfully.")
            quit(logger, unexpected=False)
