"""
WorkspaceConfig data-model definition
"""

from typing import Optional, Mapping

from dcm_common.models import DataModel

from .cu_metadata import CuMetadata


class WorkspaceConfig(CuMetadata):
    """
    Data model for a workspace config.

    Keyword arguments:
    id_ -- workspace identifier
    name -- workspace display-name
    users -- list of user-ids associated with this workspace
    templates -- list of template-ids associated with this workspace

    Inherits metadata-fields from `CuMetadata`.
    """
    id_: Optional[str]
    name: str
    users: list[str]
    templates: list[str]

    def __init__(
        self,
        *,
        id_: Optional[str] = None,
        name: str,
        users: Optional[list[str]] = None,
        templates: Optional[list[str]] = None,
        **kwargs,
    ) -> None:
        self.id_ = id_
        self.name = name
        self.users = [] if users is None else users
        self.templates = [] if templates is None else templates
        super().__init__(**kwargs)

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
            "name": self.name,
        }
        if self.id_ is not None:
            row["id"] = self.id_
        if self.user_created is not None:
            row["user_created"] = self.user_created
        if self.datetime_created is not None:
            row["datetime_created"] = self.datetime_created
        if self.user_modified is not None:
            row["user_modified"] = self.user_modified
        if self.datetime_modified is not None:
            row["datetime_modified"] = self.datetime_modified
        return row

    @classmethod
    def from_row(cls, row: Mapping) -> "WorkspaceConfig":
        """Initialize instance from database row."""
        row_ = row.copy()
        row_["id_"] = row["id"]
        del row_["id"]
        return cls(**row_)
