"""
IngestConfig data-model definition
"""

from typing import Optional
from dataclasses import dataclass

from dcm_common.models import DataModel


@dataclass
class RosettaBody(DataModel):
    """
    RosettaBody `DataModel` containing the Rosetta-related request body

    Keyword arguments:
    subdir -- name of the directory to be ingested in Rosetta
    producer -- ID referencing a producer in Rosetta
                (default None)
    material_flow -- ID referencing a Material Flow in Rosetta
                     (default None)
    """

    subdir: str
    producer: Optional[str] = None
    material_flow: Optional[str] = None


@dataclass
class IngestConfig(DataModel):
    """
    IngestConfig `DataModel`

    Keyword arguments:
    archive_identifier -- identifier of an archive system
    rosetta -- `RosettaBody` object containing
                Rosetta-related request body
    """

    archive_identifier: str
    rosetta: Optional[RosettaBody] = None

    @DataModel.deserialization_handler("rosetta")
    @classmethod
    def rosetta_deserialization_handler(cls, value):
        """Handles `rosetta`-deserialization."""
        if value is None:
            DataModel.skip()
        return RosettaBody.from_json(value)
