# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
import pathlib

from scicat_configuration import FileHandlingOptions


def compose_ingestor_directory(
    fh_options: FileHandlingOptions,
    nexus_file_path: pathlib.Path
) -> pathlib.Path:
    """Select the ingestor directory based on the file path and the options."""
    directory = pathlib.Path(fh_options.ingestor_files_directory)
    if directory.is_absolute():
        return directory
    else:
        directory = nexus_file_path.parents[0] / directory
        return directory.resolve()


def compose_ingestor_output_file_path(
    ingestor_directory: pathlib.Path,
    file_name: str,
    file_extension: str,
) -> pathlib.Path:
    """Compose the ingestor output file path based on the input provided."""

    return ingestor_directory / (
        pathlib.Path(
            ".".join(
                (
                    file_name,
                    file_extension,
                )
            )
        )
    )
