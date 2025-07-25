"""
ImportSource data-model definition
"""

from typing import Optional, Mapping
from dataclasses import dataclass

from dcm_common.models import DataModel


@dataclass
class ImportSource(DataModel):
    """
    Data model for an import source.

    Keyword arguments:
    id_ -- import source identifier
    name -- display-name for the import source
    path -- relative path to import source as str; relative to `FS_MOUNT_POINT`
    description -- description of the import source
    """
    id_: str
    name: str
    path: str
    description: Optional[str] = None

    @DataModel.serialization_handler("id_", "id")
    @classmethod
    def id__serialization_handler(cls, value):
        """Handles `id_`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("id_", "id")
    @classmethod
    def id__deserialization_handler(cls, value):
        """Handles `id_`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @property
    def row(self) -> dict:
        """Convert to database row."""
        row = {
            "id": self.id_,
            "name": self.name,
            "path": self.path,
            "description": self.description,
        }
        return row

    @classmethod
    def from_row(cls, row: Mapping) -> "ImportSource":
        """Initialize instance from database row."""
        kwargs = {
            "id_": row["id"],
            "name": row["name"],
            "path": row["path"],
            "description": row.get("description"),
        }
        return cls(**kwargs)
