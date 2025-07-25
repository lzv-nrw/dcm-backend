"""Archive types definition."""

from enum import Enum


class ArchiveAPI(Enum):
    """Supported archive API types."""
    ROSETTA_REST_V0 = "rosetta-rest-api-v0"
