# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
from scicat_configuration import build_main_arg_parser, build_scicat_config


def main() -> None:
    """Main entry point of the app."""
    arg_parser = build_main_arg_parser()
    arg_namespace = arg_parser.parse_args()
    config = build_scicat_config(arg_namespace)
    print(config)
