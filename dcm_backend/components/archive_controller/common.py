"""Common definitions for archive-controller clients."""

from typing import Any
from dataclasses import dataclass

from dcm_common import Logger


@dataclass
class ClientResponse:
    """Response-class for archive-controller clients."""
    success: bool
    log: Logger
    data: Any
