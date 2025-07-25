"""Test module for the `IngestResult` data model."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import IngestResult, RosettaResult


test_rosettaresult_json = get_model_serialization_test(
    RosettaResult,
    (
        ((), {}),
        (
            (),
            {"deposit": {}, "sip": {}},
        ),
    ),
)

print(RosettaResult().json)

test_ingestresult_json = get_model_serialization_test(
    IngestResult,
    (
        ((), {}),
        (
            (),
            {"success": True, "details": RosettaResult()},
        ),
    ),
)
