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
from scicat_metadata import MetadataSchema


@dataclass
class DummyConfig:
    ingestion: IngestionOptions


@pytest.fixture
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

    assert (
        json.loads(template_config_file.read_text())
        == OnlineIngestorConfig(config_file="").to_dict()
    )


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
                if key not in ("kafka")
            },
            "config_file": template_config_file.as_posix(),
            "nexus_file": '',
            "done_writing_message_file": '',
        },
        strict=True,
    )
    for name, tp in get_type_hints(OfflineIngestorConfig).items():
        assert isinstance(getattr(config_obj, name), tp)


def test_config_with_schema_imports_directory(tmp_path: Path) -> None:
    """Test that config can specify a schemas directory that contains import-enabled schemas."""
    import json

    schemas_dir = tmp_path / "schemas"
    modules_dir = schemas_dir / "modules"
    schemas_dir.mkdir()
    modules_dir.mkdir()

    module_schema = {
        "id": "test-module-001",
        "variables": {
            "common_var": {
                "source": "VALUE",
                "value": "common_value",
                "value_type": "string",
            }
        },
        "schema": {
            "common_field": {
                "machine_name": "common_field",
                "field_type": "scientific_metadata",
                "value": "<common_var>",
                "type": "string",
            }
        },
    }

    instrument_schema = {
        "id": "test-instrument-schema",
        "name": "Test Instrument Schema",
        "order": 1,
        "instrument": "Test",
        "selector": "filename:starts_with:test",
        "import": ["test-module-001"],
        "variables": {
            "instrument_var": {
                "source": "VALUE",
                "value": "instrument_value",
                "value_type": "string",
            }
        },
        "schema": {
            "instrument_field": {
                "machine_name": "instrument_field",
                "field_type": "scientific_metadata",
                "value": "<instrument_var>",
                "type": "string",
            }
        },
    }

    module_file = modules_dir / "module.imsc.json"
    instrument_file = schemas_dir / "instrument.imsc.json"

    module_file.write_text(json.dumps(module_schema))
    instrument_file.write_text(json.dumps(instrument_schema))

    schema = MetadataSchema.from_file(instrument_file)

    assert schema.id == "test-instrument-schema"
    assert "common_var" in schema.variables
    assert "instrument_var" in schema.variables
    assert "common_field" in schema.schema
    assert "instrument_field" in schema.schema


def test_config_validation_with_import_enabled_schemas(tmp_path: Path) -> None:
    """Test that schemas with imports can be loaded from a directory specified in config."""
    import json

    schemas_dir = tmp_path / "schemas"
    modules_dir = schemas_dir / "modules"
    schemas_dir.mkdir()
    modules_dir.mkdir()

    module_schema = {
        "id": "test-config-module",
        "variables": {
            "module_var": {
                "source": "VALUE",
                "value": "module_value",
                "value_type": "string",
            }
        },
        "schema": {
            "module_field": {
                "machine_name": "module_field",
                "field_type": "scientific_metadata",
                "value": "<module_var>",
                "type": "string",
            }
        },
    }

    schema_with_import = {
        "id": "test-schema",
        "name": "Test Schema",
        "order": 1,
        "instrument": "Test",
        "selector": "filename:starts_with:test",
        "import": ["test-config-module"],
        "variables": {
            "specific_var": {
                "source": "VALUE",
                "value": "specific_value",
                "value_type": "string",
            }
        },
        "schema": {
            "specific_field": {
                "machine_name": "specific_field",
                "field_type": "scientific_metadata",
                "value": "<specific_var>",
                "type": "string",
            }
        },
    }

    module_file = modules_dir / "module.imsc.json"
    schema_file = schemas_dir / "test.imsc.json"

    module_file.write_text(json.dumps(module_schema))
    schema_file.write_text(json.dumps(schema_with_import))

    from scicat_metadata import collect_schemas

    try:
        schemas = collect_schemas(schemas_dir)
        assert len(schemas) == 1
        assert "test-schema" in schemas

        loaded_schema = schemas["test-schema"]
        assert loaded_schema.id == "test-schema"

        assert "module_var" in loaded_schema.variables
        assert "specific_var" in loaded_schema.variables

        assert "module_field" in loaded_schema.schema
        assert "specific_field" in loaded_schema.schema

        assert loaded_schema.variables["module_var"].value == "module_value"
        assert loaded_schema.variables["specific_var"].value == "specific_value"

    except Exception as e:
        pytest.fail(f"Schema loading with imports failed unexpectedly: {e}")
