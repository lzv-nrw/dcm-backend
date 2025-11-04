"""Test module for the `Report` data model."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import IngestReport, BundleReport


test_report_json = get_model_serialization_test(
    IngestReport, (
        ((), {"host": ""}),
    )
)


test_bundle_report_json = get_model_serialization_test(
    BundleReport, (
        ((), {"host": ""}),
    )
)
