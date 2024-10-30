"""
Deposit data-model definition
"""

from typing import Optional
from dataclasses import dataclass, field

from dcm_common.models import DataModel


@dataclass
class Deposit(DataModel):
    """
    Class to represent a deposit activity.

    Keyword arguments:
    id_ -- id of the deposit activity
    status -- status of the deposit activity.
              (default "PENDING" -> no deposit activity has been triggered yet)
    sip_reason -- reason the deposit activity was rejected or declined
                  (default None)
    """

    id_: str
    status: str = field(default_factory=lambda: "PENDING")
    sip_reason: Optional[str] = None

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
