"""JobInfo-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import TriggerType, JobInfo


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
            },
        ),
    ),
)
