import pathlib

import pytest

from scicat_configuration import RunOptions, ScicatConfig


@pytest.fixture
def scicat_config(tmp_path: pathlib.Path) -> ScicatConfig:
    return ScicatConfig(
        original_dict=dict(),
        run_options=RunOptions(
            config_file='test',
            verbose=True,
            file_log=True,
            log_filepath_prefix=(tmp_path / pathlib.Path('test')).as_posix(),
            file_log_timestamp=True,
            system_log=False,
            system_log_facility=None,
            log_message_prefix='test',
            log_level='DEBUG',
            check_by_job_id=True,
            pyscicat='test',
        ),
    )


def test_scicat_logging_build_logger(scicat_config: ScicatConfig) -> None:
    from scicat_logging import build_logger

    logger = build_logger(scicat_config)
    assert len(logger.handlers) == 2  # FileHandler and StreamHandler
