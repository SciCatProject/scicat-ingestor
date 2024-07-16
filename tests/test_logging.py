import pathlib

import pytest
from scicat_configuration import (
    GraylogOptions,
    IngesterConfig,
    RunOptions,
    kafkaOptions,
)


@pytest.fixture()
def scicat_config(tmp_path: pathlib.Path) -> IngesterConfig:
    return IngesterConfig(
        original_dict={},
        run_options=RunOptions(
            config_file='test',
            verbose=True,
            file_log=True,
            file_log_base_name=(tmp_path / pathlib.Path('test')).as_posix(),
            file_log_timestamp=True,
            system_log=False,
            system_log_facility=None,
            log_message_prefix='test',
            logging_level='DEBUG',
            check_by_job_id=True,
            pyscicat='test',
        ),
        kafka_options=kafkaOptions(),
        graylog_options=GraylogOptions(),
    )


def test_scicat_logging_build_logger(scicat_config: IngesterConfig) -> None:
    from scicat_logging import build_logger

    logger = build_logger(scicat_config)
    assert len(logger.handlers) == 2  # FileHandler and StreamHandler
