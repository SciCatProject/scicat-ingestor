from dataclasses import dataclass
from pathlib import Path

import pytest

from scicat_configuration import (
    IngestionOptions,
    OfflineIngestorConfig,
    OnlineIngestorConfig,
    SciCatOptions,
    _load_config,
    _validate_config_file,
    build_dataclass,
)


@dataclass
class DummyConfig:
    ingestion: IngestionOptions


@pytest.fixture
def template_config_file() -> Path:
    return Path(__file__).parent / "../resources/config.sample.yml"


def test_template_config_file_synchronized(template_config_file: Path) -> None:
    """Template config file should have all the fields in the OnlineIngestorConfig

    If this test fails, it means that the template config file is out of sync with the
    OnlineIngestorConfig class.
    In that case, use the command ``synchronize_config`` to update the template file.

    ```bash
    synchronize_config
    ```
    """
    import yaml

    assert (
        yaml.safe_load(template_config_file.read_text())
        == OnlineIngestorConfig(config_file="").to_dict()
    )


def test_config_validator(template_config_file: Path) -> None:
    _validate_config_file(OnlineIngestorConfig, template_config_file)


def test_config_validator_json_file_warns() -> None:
    with pytest.warns(DeprecationWarning, match="deprecated. Please use YAML format"):
        _validate_config_file(
            OnlineIngestorConfig,
            Path(__file__).parent / "resources/legacy_json_config.json",
        )


def test_config_validator_unused_args_raises() -> None:
    with pytest.raises(ValueError, match="Invalid argument found: \n\t\t- config_file"):
        _validate_config_file(
            DummyConfig,
            Path(__file__).parent / "resources/invalid_config.yml",
        )


def test_arg_types_match_online_ingestor_config(template_config_file: Path) -> None:
    from typing import get_type_hints

    config_obj = build_dataclass(
        tp=OnlineIngestorConfig,
        data={
            **_load_config(template_config_file),
            "config_file": template_config_file.as_posix(),
        },
        strict=True,
    )
    for name, tp in get_type_hints(OnlineIngestorConfig).items():
        assert isinstance(getattr(config_obj, name), tp)


def test_arg_types_match_offline_ingestor_config(template_config_file: Path) -> None:
    from typing import get_type_hints

    config_obj = build_dataclass(
        tp=OfflineIngestorConfig,
        data={
            **{
                key: value
                for key, value in _load_config(template_config_file).items()
                if key not in ("kafka", "health_check")
            },
            "config_file": template_config_file.as_posix(),
            "nexus_file": '',
            "done_writing_message_file": '',
        },
        strict=True,
    )
    for name, tp in get_type_hints(OfflineIngestorConfig).items():
        assert isinstance(getattr(config_obj, name), tp)


def test_scicat_health_url_relative_path() -> None:
    options = SciCatOptions(host="https://example.org/api/v3", health_endpoint="health")
    assert options.health_url == "https://example.org/api/v3/health"


def test_scicat_health_url_absolute_url() -> None:
    options = SciCatOptions(
        host="https://example.org/api/v3",
        health_endpoint="https://status.example.org/healthz",
    )
    assert options.health_url == "https://status.example.org/healthz"
