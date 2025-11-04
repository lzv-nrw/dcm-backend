"""
Report data-model definition
"""

from dataclasses import dataclass, field

from dcm_common.orchestra import Report as BaseReport

from dcm_backend.models.bundle_result import BundleResult


@dataclass
class BundleReport(BaseReport):
    data: BundleResult = field(default_factory=BundleResult)
