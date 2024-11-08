from dataclasses import dataclass
from pathlib import Path

import pytest
from scicat_configuration import (
    IngestionOptions,
    OfflineIngestorConfig,
    OnlineIngestorConfig,
    _load_config,
    _validate_config_file,
    build_dataclass,
)


@dataclass
class DummyConfig:
    ingestion: IngestionOptions


@pytest.fixture()
def template_config_file() -> Path:
    return Path(__file__).parent / "../resources/config.sample.json"


def test_template_config_file_synchronized(template_config_file: Path) -> None:
    """Template config file should have all the fields in the OnlineIngestorConfig

    If this test fails, it means that the template config file is out of sync with the
    OnlineIngestorConfig class.
    In that case, use the command ``synchronize_config`` to update the template file.

    ```bash
    synchronize_config
    ```
    """
    import json

    assert json.loads(template_config_file.read_text()) == OnlineIngestorConfig(config_file="").to_dict()


def test_config_validator(template_config_file: Path) -> None:
    _validate_config_file(OnlineIngestorConfig, template_config_file)


def test_config_validator_unused_args_raises() -> None:
    with pytest.raises(ValueError, match="Invalid argument found: \n\t\t- config_file"):
        _validate_config_file(
            DummyConfig,
            Path(__file__).parent / "invalid_config.json",
        )


def test_arg_types_match_online_ingestor_config(template_config_file: Path) -> None:
    from typing import get_type_hints

    config_obj = build_dataclass(
        OnlineIngestorConfig,
        {
            **_load_config(template_config_file),
            "config_file": template_config_file.as_posix(),
        },
    )
    for name, tp in get_type_hints(OnlineIngestorConfig).items():
        assert isinstance(getattr(config_obj, name), tp)


def test_arg_types_match_offline_ingestor_config(template_config_file: Path) -> None:
    from typing import get_type_hints

    config_obj = build_dataclass(
        OfflineIngestorConfig,
        {
            **{
                key: value
                for key, value in _load_config(template_config_file).items()
                if key not in ("kafka")
            },
            "config_file": template_config_file.as_posix(),
            "nexus_file": '',
            "done_writing_message_file": '',
        },
    )
    for name, tp in get_type_hints(OfflineIngestorConfig).items():
        assert isinstance(getattr(config_obj, name), tp)
