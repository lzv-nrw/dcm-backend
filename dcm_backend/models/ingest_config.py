"""
IngestConfig data-model definition
"""

from typing import Optional
from dataclasses import dataclass

from dcm_common.models import DataModel, JSONObject


@dataclass
class RosettaTarget(DataModel):
    """
    RosettaTarget `DataModel` containing the Rosetta-related target

    Keyword arguments:
    subdirectory -- name of the directory to be ingested in Rosetta
    producer -- ID referencing a producer in Rosetta
                (default None)
    material_flow -- ID referencing a Material Flow in Rosetta
                     (default None)
    """
    # via API request
    subdirectory: str

    # from configuration
    producer: Optional[str] = None
    material_flow: Optional[str] = None


@dataclass
class IngestConfig(DataModel):
    """
    IngestConfig `DataModel`

    Keyword arguments:
    archive_id -- archive system configuration
    target -- `archive_type`-specific object
    """

    archive_id: str
    target: JSONObject | RosettaTarget  # | ...

    @DataModel.serialization_handler("archive_id", "archiveId")
    @classmethod
    def archive_id_serialization_handler(cls, value):
        """Handles `archive_id`-serialization."""
        return value

    @DataModel.deserialization_handler("archive_id", "archiveId")
    @classmethod
    def archive_id_deserialization_handler(cls, value):
        """Handles `archive_id`-deserialization."""
        return value

    @DataModel.deserialization_handler("target", "target")
    @classmethod
    def target_deserialization_handler(cls, value):
        """Handles `target`-deserialization."""
        # target will be processed into specific Target-class during job
        # (cannot be done here since it requires archive configuration)
        return value
