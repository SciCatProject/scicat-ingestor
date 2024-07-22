# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
import pathlib

from jinja2 import Template

_CUR_DIR = pathlib.Path(__file__).parent
_SINGLE_TEMPLATE_PATH = _CUR_DIR / pathlib.Path("single_datafile.json.jinja")
_DATASET_SCHEMA_TEMPLATE_PATH = _CUR_DIR / pathlib.Path("dataset.schema.json.jinja")
_ORIG_DATABLOCK_SCHEMA_TEMPLATE_PATH = _CUR_DIR / pathlib.Path(
    "origdatablock.schema.json.jinja"
)


def load_single_datafile_template() -> Template:
    """Load the template for the single datafile schema."""
    return Template((_SINGLE_TEMPLATE_PATH).read_text())


def load_dataset_schema_template() -> Template:
    """Load the template for the dataset schema."""
    return Template((_CUR_DIR / _DATASET_SCHEMA_TEMPLATE_PATH).read_text())


def load_origdatablock_schema_template() -> Template:
    """Load the template for the original data block schema."""
    return Template((_CUR_DIR / _ORIG_DATABLOCK_SCHEMA_TEMPLATE_PATH).read_text())
