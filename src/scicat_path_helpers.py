# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scicatproject contributors (https://github.com/ScicatProject)
import pathlib

from scicat_configuration import FileHandlingOptions


def select_target_directory(
    fh_options: FileHandlingOptions, file_path: pathlib.Path
) -> pathlib.Path:
    """Select the target directory based on the file path and the options."""
    if fh_options.hdf_structure_output == "SOURCE_FOLDER":
        return file_path.parent / pathlib.Path(fh_options.ingestor_files_directory)
    else:
        return pathlib.Path(fh_options.local_output_directory)
