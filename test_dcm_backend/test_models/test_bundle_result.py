"""Test module for the `BundleResult` and related data models."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import BundleInfo, BundleResult


test_bundleinfo_json = get_model_serialization_test(
    BundleInfo,
    ((("-", 1), {}),),
)


test_bundleresult_json = get_model_serialization_test(
    BundleResult,
    (
        ((), {}),
        ((True, BundleInfo("-", 1)), {}),
    ),
)
