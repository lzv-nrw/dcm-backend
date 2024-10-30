"""Test module for the `Report` data model."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import Report


test_report_json = get_model_serialization_test(
    Report, (
        ((), {"host": ""}),
    )
)
