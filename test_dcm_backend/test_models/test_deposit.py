"""
Test module for the `Deposit` data model.
"""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import Deposit


test_deposit_json = get_model_serialization_test(
    Deposit, (
        (("",), {}),
        (("", "some-status"), {}),
        (("", "some-status"), {"sip_reason": "some-reason"}),
    )
)
