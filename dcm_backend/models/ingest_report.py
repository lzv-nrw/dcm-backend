"""
Report data-model definition
"""

from dataclasses import dataclass, field

from dcm_common.orchestra import Report as BaseReport

from dcm_backend.models.ingest_result import IngestResult


@dataclass
class IngestReport(BaseReport):
    data: IngestResult = field(default_factory=IngestResult)
