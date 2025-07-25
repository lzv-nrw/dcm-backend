"""JobInfo-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import (
    TriggerType,
    Record,
    JobInfo,
)


test_record_json = get_model_serialization_test(
    Record,
    (
        (("some-id", True), {}),
        (
            ("some-id", True),
            {
                "token": "token",
                "external_id": "a",
                "origin_system_id": "b",
                "sip_id": "c",
                "ie_id": "d",
                "datetime_processed": "0",
            },
        ),
    ),
)


test_job_info_json = get_model_serialization_test(
    JobInfo,
    (
        (("some-token",), {}),
        (
            ("some-token",),
            {
                "job_config_id": "some-id",
                "user_triggered": "some-id",
                "datetime_triggered": "<datetime>",
                "trigger_type": TriggerType.MANUAL,
                "status": "running",
                "success": True,
                "datetime_started": "<datetime>",
                "datetime_ended": "<datetime>",
                "report": {"some": "report"},
                "template_id": "some-id",
                "workspace_id": "some-id",
                "records": [Record("some-id", True)],
            },
        ),
    ),
)
