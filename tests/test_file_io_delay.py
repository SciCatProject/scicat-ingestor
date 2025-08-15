import logging
import pathlib
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager

import pytest

from scicat_configuration import FileHandlingOptions
from scicat_offline_ingestor import open_h5file

# We are using ``subprocess`` to spawn a separate process for the fake file writer
# it is because ``multiprocessing.Process`` does not work so well under pytest environment.
# For example, it internally uses pickle to find out the context of the process
# and it fails if the test file is not recognized as a module
# with `ModuleNotFoundError`: `test.test_file_io_delay`.


class FakeLogger(logging.Logger):
    def __init__(self) -> None:
        self._logged_msg: list = []
        self.level = logging.ERROR

    def info(self, msg, *args) -> None:
        # render the message with args
        msg = msg % args
        self._logged_msg.append(msg)

    def error(self, msg, *args) -> None:
        # render the message with args
        msg = msg % args
        self._logged_msg.append(msg)


@contextmanager
def background_job(
    args: tuple[str, ...], timeout: int = 10
) -> Generator[None, None, None]:
    proc = subprocess.Popen(args=args)
    time.sleep(0.5)  # Ensure the subprocess is running
    try:
        yield None
    except Exception as e:
        proc.wait(timeout=timeout)
        raise e
    else:
        proc.wait(timeout=timeout)


def test_file_io_delay(tmp_path: pathlib.Path) -> None:
    test_file = tmp_path / "test.hdf"
    fw_script_path = (
        pathlib.Path(__file__).parent / "subprocess_helpers" / "_fake_filewriter.py"
    )
    config = FileHandlingOptions()
    logger = FakeLogger()

    with background_job(
        args=("python", fw_script_path.as_posix(), test_file.as_posix(), "--delay", "3")
    ):
        with open_h5file(test_file, file_handling_config=config, logger=logger) as f:
            data = f['/entry/title'][...]

    assert data[0] == b'Test Title'
    # Should have logged an error only once
    assert len(logger._logged_msg) == 1


def test_file_io_delay_fail_after_3(tmp_path: pathlib.Path) -> None:
    test_file = tmp_path / "test.hdf"
    fw_script_path = (
        pathlib.Path(__file__).parent / "subprocess_helpers" / "_fake_filewriter.py"
    )
    config = FileHandlingOptions(
        data_file_open_max_tries=3, data_file_open_retry_delay=[1, 1, 1]
    )
    logger = FakeLogger()

    with background_job(
        args=("python", fw_script_path.as_posix(), test_file.as_posix(), "--delay", "4")
    ):
        with pytest.raises(BlockingIOError, match="Unable to synchronously open file "):
            with open_h5file(
                test_file, file_handling_config=config, logger=logger
            ) as _:
                ...

    # Should have logged an error 3 times and last reports the final failure
    assert len(logger._logged_msg) == 4
    assert logger._logged_msg[-1].startswith(
        'Failed to open HDF5 file after 4 attempts:'
    )
