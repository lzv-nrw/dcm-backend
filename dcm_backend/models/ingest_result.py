"""
Deposit data-model definition
"""

from typing import Optional
from abc import abstractmethod
from dataclasses import dataclass

from dcm_common.models import DataModel, JSONObject

from .archive_api import ArchiveAPI


@dataclass
class ResultDetails(DataModel):
    """
    Generic JobData.details model.

    An implementation requires the definition of
    * the property `_archive_api: ArchiveAPI = ArchiveAPI.X`
    """

    @abstractmethod
    def _archive_api(self) -> ArchiveAPI:
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define "
            + "'_archive_api'."
        )

    @DataModel.serialization_handler("_archive_api", "archiveApi")
    @classmethod
    def archive_api_serialization_handler(cls, _):
        """Handles `archive_api`-serialization."""
        # pylint: disable=no-member
        return cls._archive_api.value


@dataclass
class RosettaResult(ResultDetails):
    """Rosetta-specific JobData.details."""

    deposit: Optional[JSONObject] = None
    sip: Optional[JSONObject] = None

    _archive_api: ArchiveAPI = ArchiveAPI.ROSETTA_REST_V0


@dataclass
class IngestResult(DataModel):
    """
    IngestResult `DataModel`

    Keyword arguments:
    success -- `True` if the request has been successful; `None` if
               undetermined yet
    details -- archive-api-dependent data
    """

    success: Optional[bool] = None
    details: Optional[
        RosettaResult  # | ...
    ] = None

    @classmethod
    def from_json(cls, json):
        """
        Custom implementation to properly handle`archive_api`-dependent
        `details`.
        """
        kwargs = {}

        if "success" in json:
            kwargs["success"] = json["success"]

        if "details" in json:
            match ArchiveAPI(json["details"]["archiveApi"]):
                case ArchiveAPI.ROSETTA_REST_V0:
                    kwargs["details"] = RosettaResult.from_json(
                        json["details"]
                    )

        return cls(**kwargs)
