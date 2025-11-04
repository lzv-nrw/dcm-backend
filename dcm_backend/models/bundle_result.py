"""
BundleResult-model definition
"""

from typing import Optional
from dataclasses import dataclass

from dcm_common.models import DataModel


@dataclass
class BundleInfo(DataModel):
    """
    BundleInfo `DataModel`

    Keyword arguments:
    id_ -- bundle identifier
    size -- bundle size in MB
    """

    id_: str
    size: float

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


@dataclass
class BundleResult(DataModel):
    """
    BundleResult `DataModel`

    Keyword arguments:
    success -- `True` if the request has been successful; `None` if
               undetermined yet
               (default None)
    bundle -- bundle-info
              (default None)
    """

    success: Optional[bool] = None
    bundle: Optional[BundleInfo] = None
