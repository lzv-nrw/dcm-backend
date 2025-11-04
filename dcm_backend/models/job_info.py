"""
JobInfo data-model definition
"""

from typing import Optional, Mapping
from dataclasses import dataclass
from enum import Enum

from dcm_common.models import DataModel, JSONObject


class TriggerType(Enum):
    """Job execution triggers"""

    MANUAL = "manual"
    SCHEDULED = "scheduled"
    ONETIME = "onetime"
    TEST = "test"


@dataclass
class JobInfo(DataModel):
    """JobInfo datamodel."""
    token: str
    job_config_id: Optional[str] = None
    user_triggered: Optional[str] = None
    datetime_triggered: Optional[str] = None
    trigger_type: Optional[str] = None
    status: Optional[str] = None
    success: Optional[bool] = None
    datetime_started: Optional[str] = None
    datetime_ended: Optional[str] = None
    report: Optional[JSONObject] = None
    template_id: Optional[str] = None
    workspace_id: Optional[str] = None

    @DataModel.serialization_handler("job_config_id", "jobConfigId")
    @classmethod
    def job_config_id_serialization_handler(cls, value):
        """Handles `job_config_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("job_config_id", "jobConfigId")
    @classmethod
    def job_config_id_deserialization_handler(cls, value):
        """Handles `job_config_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("user_triggered", "userTriggered")
    @classmethod
    def user_triggered_serialization_handler(cls, value):
        """Handles `user_triggered`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("user_triggered", "userTriggered")
    @classmethod
    def user_triggered_deserialization_handler(cls, value):
        """Handles `user_triggered`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("datetime_triggered", "datetimeTriggered")
    @classmethod
    def datetime_triggered_serialization_handler(cls, value):
        """Handles `datetime_triggered`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler(
        "datetime_triggered", "datetimeTriggered"
    )
    @classmethod
    def datetime_triggered_deserialization_handler(cls, value):
        """Handles `datetime_triggered`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("trigger_type", "triggerType")
    @classmethod
    def trigger_type_serialization(cls, value):
        """Performs `trigger_type`-serialization."""
        if value is None:
            DataModel.skip()
        return value.value

    @DataModel.deserialization_handler("trigger_type", "triggerType")
    @classmethod
    def trigger_type_deserialization(cls, value):
        """Performs `trigger_type`-deserialization."""
        if value is None:
            DataModel.skip()
        return TriggerType(value)

    @DataModel.serialization_handler("datetime_started", "datetimeStarted")
    @classmethod
    def datetime_started_serialization_handler(cls, value):
        """Handles `datetime_started`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("datetime_started", "datetimeStarted")
    @classmethod
    def datetime_started_deserialization_handler(cls, value):
        """Handles `datetime_started`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("datetime_ended", "datetimeEnded")
    @classmethod
    def datetime_ended_serialization_handler(cls, value):
        """Handles `datetime_ended`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("datetime_ended", "datetimeEnded")
    @classmethod
    def datetime_ended_deserialization_handler(cls, value):
        """Handles `datetime_ended`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("template_id", "templateId")
    @classmethod
    def template_id_serialization_handler(cls, value):
        """Handles `template_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("template_id", "templateId")
    @classmethod
    def template_id_deserialization_handler(cls, value):
        """Handles `template_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("workspace_id", "workspaceId")
    @classmethod
    def workspace_id_serialization_handler(cls, value):
        """Handles `workspace_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("workspace_id", "workspaceId")
    @classmethod
    def workspace_id_deserialization_handler(cls, value):
        """Handles `workspace_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @classmethod
    def from_row(cls, row: Mapping) -> "JobInfo":
        """Initialize instance from database row."""
        # omit cols that are not table-related
        return cls(
            token=row["token"],
            job_config_id=row.get("job_config_id"),
            user_triggered=row.get("user_triggered"),
            datetime_triggered=row.get("datetime_triggered"),
            trigger_type=(
                None
                if "trigger_type" not in row
                else TriggerType(row["trigger_type"])
            ),
            status=row.get("status"),
            success=row.get("success"),
            datetime_started=row.get("datetime_started"),
            datetime_ended=row.get("datetime_ended"),
            report=row.get("report"),
        )
