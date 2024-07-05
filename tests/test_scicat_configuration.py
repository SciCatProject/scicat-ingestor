# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import argparse

import pytest
from scicat_configuration import IngesterConfig


@pytest.fixture()
def main_arg_parser() -> argparse.ArgumentParser:
    """Return the namespace of the main argument parser."""
    from scicat_configuration import build_main_arg_parser

    return build_main_arg_parser()


def test_scicat_arg_parser_configuration_matches(
    main_arg_parser: argparse.ArgumentParser,
) -> None:
    """Test if options in the configuration file matches the argument parser."""
    import json
    import pathlib

    scicat_namespace = main_arg_parser.parse_args(
        ['-c', 'resources/config.sample.json']
    )

    # Check if the configuration file is the same
    assert scicat_namespace.config_file == 'resources/config.sample.json'
    config_path = pathlib.Path(scicat_namespace.config_file)
    config_from_args: dict = vars(scicat_namespace)

    # Parse the configuration file
    assert config_path.exists()
    config_from_file: dict = json.loads(config_path.read_text())
    main_options: dict = config_from_file.get('options', {})

    # Check if all keys matches
    all_keys = set(config_from_args.keys()).union(main_options.keys())
    for key in all_keys:
        assert key in config_from_args
        assert key in main_options


def test_build_scicat_config_default(main_arg_parser: argparse.ArgumentParser) -> None:
    """Test if the configuration can be built from default arguments."""
    from scicat_configuration import build_scicat_ingester_config

    scicat_namespace = main_arg_parser.parse_args()
    scicat_config = build_scicat_ingester_config(scicat_namespace)
    assert scicat_config.run_options.config_file == 'config.20240405.json'


@pytest.fixture()
def ingester_config(main_arg_parser: argparse.ArgumentParser) -> IngesterConfig:
    from scicat_configuration import build_scicat_ingester_config

    scicat_namespace = main_arg_parser.parse_args(
        ['-c', 'resources/config.sample.json', '--verbose']
    )
    return build_scicat_ingester_config(scicat_namespace)


def test_build_scicat_config(ingester_config: IngesterConfig) -> None:
    """Test if the configuration can be built from arguments."""
    assert ingester_config.original_dict['options']['config_file'] == 'config.json'
    assert ingester_config.run_options.config_file == 'resources/config.sample.json'
    assert not ingester_config.original_dict['options']['verbose']
    assert ingester_config.run_options.verbose


def test_scicat_config_original_dict_read_only(ingester_config: IngesterConfig) -> None:
    """Test if the original dictionary is read-only."""
    from types import MappingProxyType

    assert isinstance(ingester_config.original_dict, MappingProxyType)
    for sub_option in ingester_config.original_dict.values():
        assert isinstance(sub_option, MappingProxyType)


def test_scicat_config_kafka_options(ingester_config: IngesterConfig) -> None:
    """Test if the Kafka options are correctly read."""
    assert ingester_config.kafka_options.topics == ["KAFKA_TOPIC_1", "KAFKA_TOPIC_2"]
    assert ingester_config.kafka_options.enable_auto_commit


def test_scicat_background_config_single_run_option() -> None:
    """Test if the single run options are correctly read."""
    from scicat_configuration import (
        build_background_ingestor_arg_parser,
        build_scicat_background_ingester_config,
    )

    arg_parser = build_background_ingestor_arg_parser()
    scicat_namespace = arg_parser.parse_args(
        [
            '-c',
            'resources/config.sample.json',
            '--verbose',
            '--nexus-file',
            'file.nxs',
            '--done-writing-message-file',
            'file.json',
        ]
    )
    background_ingester_config = build_scicat_background_ingester_config(
        scicat_namespace
    )
    assert background_ingester_config.single_run_options.nexus_file == 'file.nxs'
    assert (
        background_ingester_config.single_run_options.done_writing_message_file
        == 'file.json'
    )
