"""Test module for the `IngestResult` data model."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import IngestResult, Deposit


test_ingestconfig_json = get_model_serialization_test(
    IngestResult, (
        ((), {}),
        ((True,), {}),
        ((True,), {"deposit": Deposit("")}),
    )
)
