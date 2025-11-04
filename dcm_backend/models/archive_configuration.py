"""
TargetArchive and related data-model definitions
"""

from typing import Optional, Mapping
from dataclasses import dataclass
from pathlib import Path

from dcm_common.models import DataModel

from .archive_api import ArchiveAPI


class RosettaRestV0Details(DataModel):
    """
    Data model for configuration details of a target-archive.

    Keyword arguments:
    url -- base-url for API
    material_flow -- material-flow identifier
    producer -- producer identifier
    auth_file -- path to basic auth-header file
                 (default None)
    basic_auth -- basic auth-header (e.g., "Authorization: Basic ...")
                  (default None)
    proxy -- proxy-information (see `requests`-library for details)
             (default None)
    """

    url: str
    material_flow: str
    producer: str
    auth_file: Optional[Path]
    basic_auth: Optional[str]
    proxy: Optional[Mapping[str, str]]

    def __init__(
        self,
        url: str,
        material_flow: str,
        producer: str,
        auth_file: Optional[Path] = None,
        basic_auth: Optional[str] = None,
        proxy: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.url = url
        self.material_flow = material_flow
        self.producer = producer
        if auth_file is not None and not auth_file.is_file():
            raise ValueError(
                f"Authentication file '{auth_file}' does not exist."
            )
        if auth_file is None and basic_auth is None:
            raise ValueError(
                f"Model '{self.__class__.__name__}' needs either 'auth_file' "
                + "or explicit 'basic_auth'."
            )
        self.auth_file = auth_file
        self.basic_auth = basic_auth
        self.proxy = proxy

    @DataModel.serialization_handler("material_flow", "materialFlow")
    @classmethod
    def material_flow_serialization_handler(cls, value):
        """Handles `material_flow`-serialization."""
        return value

    @DataModel.deserialization_handler("material_flow", "materialFlow")
    @classmethod
    def material_flow_deserialization_handler(cls, value):
        """Handles `material_flow`-deserialization."""
        return value

    @DataModel.serialization_handler("auth_file", "authFile")
    @classmethod
    def auth_file_serialization_handler(cls, value):
        """Handles `auth_file`-serialization."""
        if value is None:
            DataModel.skip()
        return str(value)

    @DataModel.deserialization_handler("auth_file", "authFile")
    @classmethod
    def auth_file_deserialization_handler(cls, value):
        """Handles `auth_file`-deserialization."""
        if value is None:
            DataModel.skip()
        return Path(value)

    @DataModel.serialization_handler("basic_auth", "basicAuth")
    @classmethod
    def basic_auth_serialization_handler(cls, value):
        """Handles `basic_auth`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("basic_auth", "basicAuth")
    @classmethod
    def basic_auth_deserialization_handler(cls, value):
        """Handles `basic_auth`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("proxy")
    @classmethod
    def proxy_serialization_handler(cls, value):
        """Handles `proxy`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("proxy")
    @classmethod
    def proxy_deserialization_handler(cls, value):
        """Handles `proxy`-deserialization."""
        if value is None:
            DataModel.skip()
        return value


ARCHIVE_CONFIGURATION_DETAILS_MAP = {
    ArchiveAPI.ROSETTA_REST_V0.value: RosettaRestV0Details,
}


@dataclass
class ArchiveConfiguration(DataModel):
    """
    Data model for an archive configuration.

    Keyword arguments:
    id_ -- archive identifier
    name -- display-name for the archive
    type_ -- archive type (see also ArchiveAPI-enum)
    details -- type_-dependent configuration details
    description -- description of the archive
                   (default None)
    """

    id_: str
    name: str
    type_: ArchiveAPI
    details: RosettaRestV0Details
    description: Optional[str] = None

    @DataModel.serialization_handler("id_", "id")
    @classmethod
    def id__serialization_handler(cls, value):
        """Handles `id_`-serialization."""
        return value

    @DataModel.serialization_handler("type_", "type")
    @classmethod
    def type__serialization_handler(cls, value):
        """Handles `type_`-serialization."""
        return value.value

    # custom implementation to properly handle type_-dependent details
    @classmethod
    def from_json(cls, json) -> "ArchiveConfiguration":
        return cls(
            id_=json["id"],
            name=json["name"],
            type_=ArchiveAPI(json["type"]),
            details=ARCHIVE_CONFIGURATION_DETAILS_MAP[
                json["type"]
            ].from_json(json["details"]),
            description=json.get("description"),
        )
