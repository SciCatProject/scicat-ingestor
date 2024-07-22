# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)


def test_dataset_schema_path_helper() -> None:
    from scicat_path_helpers import get_dataset_schema_template_path

    path = get_dataset_schema_template_path()
    assert path.name == "dataset.schema.json.jinja"
    assert path.exists()


def test_origdatablock_schema_path_helper() -> None:
    from scicat_path_helpers import get_origdatablock_schema_template_path

    path = get_origdatablock_schema_template_path()
    assert path.name == "origdatablock.schema.json.jinja"
    assert path.exists()
