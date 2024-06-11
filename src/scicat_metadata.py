# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 ScicatProject contributors (https://github.com/ScicatProject)
from importlib.metadata import entry_points
from typing import Callable


def load_metadata_extractors(extractor_name: str) -> Callable:
    """Load metadata extractors from the entry points."""

    return entry_points(group="scicat_ingestor.metadata_extractor")[
        extractor_name
    ].load()
