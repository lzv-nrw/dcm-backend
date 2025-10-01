"""
TemplateConfig data-model definition
"""

from typing import Optional, Mapping
from dataclasses import dataclass, field

from dcm_common.models import DataModel, JSONObject

from .cu_metadata import CuMetadata


@dataclass
class PluginInfo(DataModel):
    """Plugin info `DataModel`"""

    plugin: Optional[str] = None
    args: Optional[JSONObject] = None

    @property
    def row(self) -> dict:
        """Convert to database row."""
        return self.json

    @classmethod
    def from_row(cls, row: Mapping) -> "PluginInfo":
        """Initialize instance from database row."""
        return cls(
            plugin=row.get("plugin"),
            args=row.get("args"),
        )


@dataclass
class HotfolderInfo(DataModel):
    """Hotfolder info `DataModel`"""

    source_id: Optional[str] = None

    @DataModel.serialization_handler("source_id", "sourceId")
    @classmethod
    def source_id_serialization_handler(cls, value):
        """Handles `source_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("source_id", "sourceId")
    @classmethod
    def source_id_deserialization_handler(cls, value):
        """Handles `source_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @property
    def row(self) -> dict:
        """Convert to database row."""
        return {"source_id": self.source_id}

    @classmethod
    def from_row(cls, row: Mapping) -> "HotfolderInfo":
        """Initialize instance from database row."""
        return cls(
            source_id=row.get("source_id"),
        )


@dataclass
class TransferUrlFilter(DataModel):
    """Transfer-URL filter `DataModel`"""

    regex: str
    path: Optional[str] = None

    @property
    def row(self) -> dict:
        """Convert to database row."""
        return self.json

    @classmethod
    def from_row(cls, row: Mapping) -> "TransferUrlFilter":
        """Initialize instance from database row."""
        return cls(
            regex=row["regex"],
            path=row.get("path"),
        )


@dataclass
class OAIInfo(DataModel):
    """OAI info `DataModel`"""

    url: Optional[str] = None
    metadata_prefix: Optional[str] = None
    transfer_url_filters: list[TransferUrlFilter] = field(default_factory=list)

    @DataModel.serialization_handler("metadata_prefix", "metadataPrefix")
    @classmethod
    def metadata_prefix_serialization_handler(cls, value):
        """Handles `metadata_prefix`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("metadata_prefix", "metadataPrefix")
    @classmethod
    def metadata_prefix_deserialization_handler(cls, value):
        """Handles `metadata_prefix`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler(
        "transfer_url_filters", "transferUrlFilters"
    )
    @classmethod
    def transfer_url_filters_serialization_handler(cls, value):
        """Handles `transfer_url_filters`-serialization."""
        return [f.json for f in value]

    @DataModel.deserialization_handler(
        "transfer_url_filters", "transferUrlFilters"
    )
    @classmethod
    def transfer_url_filters_deserialization_handler(cls, value):
        """Handles `transfer_url_filters`-deserialization."""
        return [TransferUrlFilter.from_json(f_kwargs) for f_kwargs in value]

    @property
    def row(self) -> dict:
        """Convert to database row."""
        return {
            "url": self.url,
            "metadata_prefix": self.metadata_prefix,
            "transfer_url_filters": [f.row for f in self.transfer_url_filters],
        }

    @classmethod
    def from_row(cls, row: Mapping) -> "OAIInfo":
        """Initialize instance from database row."""
        return cls(
            url=row.get("url"),
            metadata_prefix=row.get("metadata_prefix"),
            transfer_url_filters=[
                TransferUrlFilter.from_row(f_kwargs)
                for f_kwargs in row["transfer_url_filters"]
            ],
        )


INFO_INDEX = {"plugin": PluginInfo, "hotfolder": HotfolderInfo, "oai": OAIInfo}


@dataclass
class TargetArchive(DataModel):
    """Data model for archive target-information."""
    id_: Optional[str] = None

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


class TemplateConfig(CuMetadata):
    """
    Data model for a template config.

    Keyword arguments:
    id_ -- template identifier
    status -- template status
    workspace_id -- id of associated workspace
    name -- template display-name
    description -- template description
    type_ -- connection type for this template
    additional_information -- type-specific data
    target_archive -- target archive

    Inherits metadata-fields from `CuMetadata`.
    """

    id_: Optional[str]
    status: str
    workspace_id: Optional[str]
    name: Optional[str]
    description: Optional[str]
    type_: Optional[str]
    additional_information: Optional[
        JSONObject | PluginInfo | HotfolderInfo | OAIInfo
    ]
    target_archive: Optional[TargetArchive]

    def __init__(
        self,
        *,
        id_: Optional[str] = None,
        status: str,
        workspace_id: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        type_: Optional[str] = None,
        additional_information: Optional[
            JSONObject | PluginInfo | HotfolderInfo | OAIInfo
        ] = None,
        target_archive: Optional[TargetArchive] = None,
        **kwargs,
    ) -> None:
        self.id_ = id_
        self.status = status
        self.workspace_id = workspace_id
        self.name = name
        self.description = description
        self.type_ = type_
        self.additional_information = additional_information
        self.target_archive = target_archive
        super().__init__(**kwargs)

    @classmethod
    def from_json(cls, json: JSONObject):
        """
        Returns `TemplateConfig` initialized with data from `json`.

        Explicit implementation ensures proper handling of
        `additional_information`.
        """
        kwargs = {
            "status": json["status"],
            "id_": json.get("id"),
            "workspace_id": json.get("workspaceId"),
            "name": json.get("name"),
            "description": json.get("description"),
            "type_": json.get("type"),
            "user_created": json.get("userCreated"),
            "datetime_created": json.get("datetimeCreated"),
            "user_modified": json.get("userModified"),
            "datetime_modified": json.get("datetimeModified"),
        }

        if kwargs["type_"] is not None and json["additionalInformation"] != {}:
            if kwargs["type_"] not in INFO_INDEX:
                raise ValueError(
                    f"Got unexpected 'type' of '{kwargs['type_']}' while "
                    + "deserializing 'TemplateConfig'."
                )
            # accept raw JSON in case of draft
            if kwargs["status"] == "draft":
                kwargs["additional_information"] = json[
                    "additionalInformation"
                ]
            else:
                kwargs["additional_information"] = INFO_INDEX[
                    kwargs["type_"]
                ].from_json(json["additionalInformation"])

        if json.get("targetArchive") is not None:
            kwargs["target_archive"] = TargetArchive.from_json(
                json["targetArchive"]
            )

        return cls(**kwargs)

    @DataModel.serialization_handler("id_", "id")
    @classmethod
    def id__serialization_handler(cls, value):
        """Handles `id_`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("workspace_id", "workspaceId")
    @classmethod
    def workspace_id_serialization_handler(cls, value):
        """Handles `workspace_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("type_", "type")
    @classmethod
    def type__serialization_handler(cls, value):
        """Handles `type_`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler(
        "additional_information", "additionalInformation"
    )
    @classmethod
    def additional_information_serialization_handler(cls, value):
        """Handles `additional_information`-serialization."""
        if value is None:
            DataModel.skip()
        if isinstance(value, dict):
            return value
        return value.json

    @DataModel.serialization_handler("target_archive", "targetArchive")
    @classmethod
    def target_archive_serialization_handler(cls, value):
        """Handles `target_archive`-serialization."""
        if value is None:
            DataModel.skip()
        return value.json

    @property
    def row(self) -> dict:
        """Convert to database row."""
        row = {
            "status": self.status,
            "workspace_id": self.workspace_id,
            "name": self.name,
            "description": self.description,
            "type": self.type_,
        }
        if self.additional_information is not None:
            if isinstance(self.additional_information, dict):
                row["additional_information"] = self.additional_information
            else:
                row["additional_information"] = self.additional_information.row
        if self.target_archive is not None:
            row["target_archive"] = self.target_archive.json
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
    def from_row(cls, row: Mapping) -> "TemplateConfig":
        """Initialize instance from database row."""
        kwargs = {
            "status": row["status"],
            "id_": row.get("id"),
            "workspace_id": row.get("workspace_id"),
            "name": row.get("name"),
            "description": row.get("description"),
            "type_": row.get("type"),
            "user_created": row.get("user_created"),
            "datetime_created": row.get("datetime_created"),
            "user_modified": row.get("user_modified"),
            "datetime_modified": row.get("datetime_modified"),
        }
        if kwargs["type_"] is not None:
            # accept raw JSON in case of draft
            if kwargs["status"] == "draft":
                kwargs["additional_information"] = row.get(
                    "additional_information"
                )
            else:
                kwargs["additional_information"] = INFO_INDEX[
                    kwargs["type_"]
                ].from_row(row.get("additional_information"))
        if row.get("target_archive") is not None:
            kwargs["target_archive"] = TargetArchive.from_json(
                row["target_archive"]
            )
        return cls(**kwargs)
