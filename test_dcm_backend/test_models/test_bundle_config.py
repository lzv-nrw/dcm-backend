"""Test module for the `BundleConfig` and related data models."""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import BundleTarget, BundleConfig


test_bundletarget_json = get_model_serialization_test(
    BundleTarget,
    (((Path("file_storage"),), {}),),
)


test_bundleconfig_json = get_model_serialization_test(
    BundleConfig,
    (
        ((), {}),
        (
            (
                [
                    BundleTarget(
                        Path("file_storage/1"), Path("file_storage/a")
                    ),
                    BundleTarget(
                        Path("file_storage/2"), Path("file_storage/b")
                    ),
                ],
            ),
            {},
        ),
    ),
)
