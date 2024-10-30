"""
IngestResult data-model definition
"""

from typing import Optional
from dataclasses import dataclass

from dcm_common.models import DataModel

from dcm_backend.models import Deposit


@dataclass
class IngestResult(DataModel):
    """
    IngestResult `DataModel`

    Keyword arguments:
    success -- overall success of the job
    deposit -- details about a deposit in the archive system
    """

    success: Optional[bool] = None
    deposit: Optional[Deposit] = None

    @DataModel.deserialization_handler("deposit")
    @classmethod
    def deposit_deserialization_handler(cls, value):
        """Handles `deposit`-deserialization."""
        if value is None:
            DataModel.skip()
        return Deposit.from_json(value)
