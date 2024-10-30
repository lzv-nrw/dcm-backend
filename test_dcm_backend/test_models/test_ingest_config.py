"""IngestConfig-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import IngestConfig, RosettaBody


test_rosettabody_json = get_model_serialization_test(
    RosettaBody, (
        (("",), {}),
        ((), {
            "subdir": "subdir",
            "producer": "producer",
            "material_flow": "material_flow"
        }),
    )
)


test_ingestconfig_json = get_model_serialization_test(
    IngestConfig, (
        (("",), {}),
        (("",), {"rosetta": RosettaBody("")}),
    )
)
