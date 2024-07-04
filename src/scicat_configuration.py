# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import argparse
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Any


def _load_config(config_file: Any) -> dict:
    """Load configuration from the configuration file path."""
    import json
    import pathlib

    if (
        isinstance(config_file, str | pathlib.Path)
        and (config_file_path := pathlib.Path(config_file)).is_file()
    ):
        return json.loads(config_file_path.read_text())
    return {}


def _merge_run_options(config_dict: dict, input_args_dict: dict) -> dict:
    """Merge configuration from the configuration file and input arguments."""
    import copy

    # Overwrite deep-copied options with command line arguments
    run_option_dict: dict = copy.deepcopy(config_dict.setdefault("options", {}))
    for arg_name, arg_value in input_args_dict.items():
        if arg_value is not None:
            run_option_dict[arg_name] = arg_value

    return run_option_dict


def _freeze_dict_items(d: dict) -> MappingProxyType:
    """Freeze the dictionary to make it read-only."""
    return MappingProxyType(
        {
            key: MappingProxyType(value) if isinstance(value, dict) else value
            for key, value in d.items()
        }
    )


def _recursive_deepcopy(obj: Any) -> dict:
    """Recursively deep copy a dictionary."""
    if not isinstance(obj, dict | MappingProxyType):
        return obj

    copied = dict(obj)
    for key, value in copied.items():
        if isinstance(value, Mapping | MappingProxyType):
            copied[key] = _recursive_deepcopy(value)

    return copied


def build_main_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    group = parser.add_argument_group("Scicat Ingestor Options")

    group.add_argument(
        "-c",
        "--cf",
        "--config",
        "--config-file",
        default="config.20240405.json",
        dest="config_file",
        help="Configuration file name. Default: config.20240405.json",
        type=str,
    )
    group.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        help="Provide logging on stdout",
        action="store_true",
        default=False,
    )
    group.add_argument(
        "--file-log",
        dest="file_log",
        help="Provide logging on file",
        action="store_true",
        default=False,
    )
    group.add_argument(
        "--log-filepath-prefix",
        dest="log_filepath_prefix",
        help="Prefix of the log file path",
        default=".scicat_ingestor_log",
    )
    group.add_argument(
        "--file-log-timestamp",
        dest="file_log_timestamp",
        help="Provide logging on the system log",
        action="store_true",
        default=False,
    )
    group.add_argument(
        "--system-log",
        dest="system_log",
        help="Provide logging on the system log",
        action="store_true",
        default=False,
    )
    group.add_argument(
        "--system-log-facility",
        dest="system_log_facility",
        help="Facility for system log",
        default="mail",
    )
    group.add_argument(
        "--log-message-prefix",
        dest="log_message_prefix",
        help="Prefix for log messages",
        default=" SFI: ",
    )
    group.add_argument(
        "--log-level", dest="log_level", help="Logging level", default="INFO", type=str
    )
    group.add_argument(
        "--check-by-job-id",
        dest="check_by_job_id",
        help="Check the status of a job by job_id",
        action="store_true",
        default=True,
    )
    group.add_argument(
        "--pyscicat",
        dest="pyscicat",
        help="Location where a specific version of pyscicat is available",
        default=None,
        type=str,
    )
    group.add_argument(
        "--graylog",
        dest="graylog",
        help="Use graylog for additional logs",
        action="store_true",
        default=False,
    )
    return parser


def build_background_ingestor_arg_parser() -> argparse.ArgumentParser:
    parser = build_main_arg_parser()
    group = parser.add_argument_group('Scicat Background Ingestor Options')

    group.add_argument(
        '-f',
        '--nf',
        '--file',
        '--nexus-file',
        default='',
        dest='nexus_file',
        help='Full path of the input nexus file to be ingested',
        type=str,
    )

    group.add_argument(
        '-m',
        '--dm',
        '--wrdm',
        '--done-writing-message-file',
        default='',
        dest='done_writing_message_file',
        help="""
          Full path of the input done writing message
          file that match the nexus file to be ingested
        """,
        type=str,
    )

    return parser


@dataclass
class GraylogOptions:
    host: str = ""
    port: str = ""
    facility: str = "scicat.ingestor"


@dataclass
class RunOptions:
    """RunOptions dataclass to store the configuration options.

    Most of options don't have default values because they are expected
    to be set by the user either in the configuration file or through
    command line arguments.
    """

    config_file: str
    verbose: bool
    file_log: bool
    log_filepath_prefix: str
    file_log_timestamp: bool
    system_log: bool
    log_message_prefix: str
    log_level: str
    check_by_job_id: bool
    system_log_facility: str | None = None
    pyscicat: str | None = None
    graylog: bool = False


@dataclass
class kafkaOptions:
    """KafkaOptions dataclass to store the configuration options.

    Default values are provided as they are not
    expected to be set by command line arguments.
    """

    topics: list[str] | str = "KAFKA_TOPIC_1,KAFKA_TOPIC_2"
    """List of Kafka topics. Multiple topics can be separated by commas."""
    group_id: str = "GROUP_ID"
    """Kafka consumer group ID."""
    bootstrap_servers: list[str] | str = "localhost:9092"
    """List of Kafka bootstrap servers. Multiple servers can be separated by commas."""
    individual_message_commit: bool = False
    """Commit for each topic individually."""
    enable_auto_commit: bool = True
    """Enable Kafka auto commit."""
    auto_offset_reset: str = "earliest"
    """Kafka auto offset reset."""


@dataclass
class IngesterConfig:
    original_dict: Mapping
    """Original configuration dictionary in the json file."""
    run_options: RunOptions
    """Merged configuration dictionary with command line arguments."""
    kafka_options: kafkaOptions
    """Kafka configuration options read from files."""
    graylog_options: GraylogOptions
    """Graylog configuration options for streaming logs."""

    def to_dict(self) -> dict:
        """Return the configuration as a dictionary."""

        return asdict(
            IngesterConfig(
                _recursive_deepcopy(
                    self.original_dict
                ),  # asdict does not support MappingProxyType
                self.run_options,
                self.kafka_options,
                self.graylog_options,
            )
        )


def build_scicat_ingester_config(input_args: argparse.Namespace) -> IngesterConfig:
    """Merge configuration from the configuration file and input arguments."""
    config_dict = _load_config(input_args.config_file)
    run_option_dict = _merge_run_options(config_dict, vars(input_args))

    # Wrap configuration in a dataclass
    return IngesterConfig(
        original_dict=_freeze_dict_items(config_dict),
        run_options=RunOptions(**run_option_dict),
        kafka_options=kafkaOptions(**config_dict.setdefault("kafka", {})),
        graylog_options=GraylogOptions(**config_dict.setdefault("graylog", {})),
    )


@dataclass
class SingleRunOptions:
    nexus_file: str
    """Full path of the input nexus file to be ingested."""
    done_writing_message_file: str
    """Full path of the done writing message file that match the ``nexus_file``."""


@dataclass
class BackgroundIngestorConfig(IngesterConfig):
    single_run_options: SingleRunOptions
    """Single run configuration options for background ingestor."""

    def to_dict(self) -> dict:
        """Return the configuration as a dictionary."""

        return asdict(
            BackgroundIngestorConfig(
                _recursive_deepcopy(
                    self.original_dict
                ),  # asdict does not support MappingProxyType
                self.run_options,
                self.kafka_options,
                self.graylog_options,
                self.single_run_options,
            )
        )


def build_scicat_background_ingester_config(
    input_args: argparse.Namespace,
) -> BackgroundIngestorConfig:
    """Merge configuration from the configuration file and input arguments."""
    config_dict = _load_config(input_args.config_file)
    input_args_dict = vars(input_args)
    single_run_option_dict = {
        "nexus_file": input_args_dict.pop("nexus_file"),
        "done_writing_message_file": input_args_dict.pop("done_writing_message_file"),
    }
    run_option_dict = _merge_run_options(config_dict, input_args_dict)

    # Wrap configuration in a dataclass
    return BackgroundIngestorConfig(
        original_dict=_freeze_dict_items(config_dict),
        run_options=RunOptions(**run_option_dict),
        kafka_options=kafkaOptions(**config_dict.setdefault("kafka", {})),
        single_run_options=SingleRunOptions(**single_run_option_dict),
        graylog_options=GraylogOptions(**config_dict.setdefault("graylog", {})),
    )
