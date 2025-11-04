"""
BundleConfig-model definition
"""

from typing import Optional
from dataclasses import dataclass, field
from pathlib import Path

from dcm_common.models import DataModel


@dataclass
class BundleTarget(DataModel):
    """
    BundleTarget `DataModel`

    Keyword arguments:
    path -- path to target
    as_path -- output path in archive-file
               (default None)
    """

    path: Path
    as_path: Optional[Path] = None

    @DataModel.serialization_handler("path")
    @classmethod
    def path_serialization_handler(cls, value):
        """Handles `path`-serialization."""
        return str(value)

    @DataModel.deserialization_handler("path")
    @classmethod
    def path_deserialization_handler(cls, value):
        """Handles `path`-deserialization."""
        return Path(value)

    @DataModel.serialization_handler("as_path", "asPath")
    @classmethod
    def as_path_serialization_handler(cls, value):
        """Handles `as_path`-serialization."""
        if value is None:
            DataModel.skip()
        return str(value)

    @DataModel.deserialization_handler("as_path", "asPath")
    @classmethod
    def as_path_deserialization_handler(cls, value):
        """Handles `as_path`-deserialization."""
        if value is None:
            DataModel.skip()
        return Path(value)


@dataclass
class BundleConfig(DataModel):
    """
    BundleConfig `DataModel`

    Keyword arguments:
    targets -- list of bundle targets
               (default [])
    """

    targets: list[BundleTarget] = field(default_factory=list)
