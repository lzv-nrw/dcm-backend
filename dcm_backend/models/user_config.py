"""
UserConfig data-model definition
"""

from typing import Optional, Mapping
from dataclasses import dataclass

from dcm_common.models import DataModel, JSONObject

from .cu_metadata import CuMetadata


@dataclass
class GroupMembership(DataModel):
    """
    Data model for a group membership.

    Keyword arguments:
    id -- group identifier
    workspace -- workspace identifier
    """
    id_: str
    workspace: Optional[str] = None

    @DataModel.serialization_handler("id_", "id")
    @classmethod
    def id__serialization_handler(cls, value):
        """Handles `id_`-serialization."""
        return value

    @DataModel.deserialization_handler("id_", "id")
    @classmethod
    def id__deserialization_handler(cls, value):
        """Handles `id_`-deserialization."""
        return value


class UserConfig(CuMetadata):
    """
    Data model for a user config.

    Keyword arguments:
    id_ -- local user id
    external_id -- external user id
    username -- user name
    status -- account status
              (default "inactive")
    firstname -- first name
    lastname -- last name
    email -- email address
    groups -- list of group memberships
              (default empty list)
    widget_config -- widget configuration

    Inherits metadata-fields from `CuMetadata`.
    """
    id_: Optional[str] = None
    external_id: Optional[str] = None
    username: Optional[str] = None
    status: Optional[str] = None
    firstname: Optional[str] = None
    lastname: Optional[str] = None
    email: Optional[str] = None
    groups: Optional[list[GroupMembership]] = None
    widget_config: Optional[JSONObject] = None

    def __init__(
        self,
        *,
        id_: Optional[str] = None,
        external_id: Optional[str] = None,
        username: Optional[str] = None,
        status: Optional[str] = None,
        firstname: Optional[str] = None,
        lastname: Optional[str] = None,
        email: Optional[str] = None,
        groups: Optional[list[GroupMembership]] = None,
        widget_config: Optional[JSONObject] = None,
        **kwargs,
    ) -> None:
        self.id_ = id_
        self.external_id = external_id
        self.username = username
        self.status = "inactive" if status is None else status
        self.firstname = firstname
        self.lastname = lastname
        self.email = email
        self.groups = [] if groups is None else groups
        self.widget_config = widget_config
        super().__init__(**kwargs)

    @DataModel.serialization_handler("id_", "id")
    @classmethod
    def id__serialization_handler(cls, value):
        """Handles `id_`-serialization."""
        return value

    @DataModel.deserialization_handler("id_", "id")
    @classmethod
    def id__deserialization_handler(cls, value):
        """Handles `id_`-deserialization."""
        return value

    @DataModel.serialization_handler("external_id", "externalId")
    @classmethod
    def external_id_serialization_handler(cls, value):
        """Handles `external_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("external_id", "externalId")
    @classmethod
    def external_id_deserialization_handler(cls, value):
        """Handles `external_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("groups")
    @classmethod
    def groups_deserialization_handler(cls, value):
        """Handles `groups`-deserialization."""
        if value is None:
            DataModel.skip()
        return [GroupMembership.from_json(group) for group in value]

    @DataModel.serialization_handler("widget_config", "widgetConfig")
    @classmethod
    def widget_config_serialization_handler(cls, value):
        """Handles `widget_config`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("widget_config", "widgetConfig")
    @classmethod
    def widget_config_deserialization_handler(cls, value):
        """Handles `widget_config`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @property
    def row(self) -> dict:
        """Convert to database row."""
        row = {
            "external_id": self.external_id,
            "username": self.username,
            "status": self.status,
            "firstname": self.firstname,
            "lastname": self.lastname,
            "email": self.email,
            "widget_config": self.widget_config,
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
    def from_row(cls, row: Mapping) -> "UserConfig":
        """Initialize instance from database row."""
        row_ = row.copy()
        row_["id_"] = row["id"]
        del row_["id"]
        return cls(**row_)


@dataclass(kw_only=True)
class UserSecrets(DataModel):
    """Data model for user secrets."""
    id_: Optional[str] = None
    user_id: Optional[str] = None

    # hashed and encoded password (this is the value stored in the database)
    password: Optional[str] = None

    # introduced for alternative user-activation where the actual password
    # needs to be returned via the API
    password_raw: Optional[str] = None

    @property
    def row(self) -> dict:
        """Convert to database row."""
        row = {
            "user_id": self.user_id,
            "password": self.password,
        }
        if self.id_ is not None:
            row["id"] = self.id_
        return row

    @classmethod
    def from_row(cls, row: Mapping) -> "UserSecrets":
        """Initialize instance from database row."""
        row["id_"] = row["id"]
        del row["id"]
        return cls(**row)


@dataclass
class UserConfigWithSecrets(DataModel):
    config: UserConfig
    secrets: UserSecrets


@dataclass
class UserCredentials(DataModel):
    """Data model for user credentials."""
    username: str
    password: str

    @DataModel.serialization_handler("username", "userId")
    @classmethod
    def username_serialization_handler(cls, value):
        """Handles `username`-serialization."""
        return value

    @DataModel.deserialization_handler("username", "userId")
    @classmethod
    def username_deserialization_handler(cls, value):
        """Handles `username`-deserialization."""
        return value
