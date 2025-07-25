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
class Record(DataModel):
    """Record datamodel."""
    report_id: str
    success: bool
    token: Optional[str] = None
    external_id: Optional[str] = None
    origin_system_id: Optional[str] = None
    sip_id: Optional[str] = None
    ie_id: Optional[str] = None
    datetime_processed: Optional[str] = None

    @DataModel.serialization_handler("report_id", "reportId")
    @classmethod
    def report_id_serialization_handler(cls, value):
        """Handles `report_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("report_id", "reportId")
    @classmethod
    def report_id_deserialization_handler(cls, value):
        """Handles `report_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("external_id", "externalId")
    @classmethod
    def external_id_serialization(cls, value):
        """Performs `external_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("external_id", "externalId")
    @classmethod
    def external_id_deserialization(cls, value):
        """Performs `external_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("origin_system_id", "originSystemId")
    @classmethod
    def origin_system_id_serialization(cls, value):
        """Performs `origin_system_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("origin_system_id", "originSystemId")
    @classmethod
    def origin_system_id_deserialization(cls, value):
        """Performs `origin_system_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("sip_id", "sipId")
    @classmethod
    def sip_id_serialization(cls, value):
        """Performs `sip_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("sip_id", "sipId")
    @classmethod
    def sip_id_deserialization(cls, value):
        """Performs `sip_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("ie_id", "ieId")
    @classmethod
    def ie_id_serialization(cls, value):
        """Performs `ie_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("ie_id", "ieId")
    @classmethod
    def ie_id_deserialization(cls, value):
        """Performs `ie_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("datetime_processed", "datetimeProcessed")
    @classmethod
    def datetime_processed_serialization(cls, value):
        """Performs `datetime_processed`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler(
        "datetime_processed", "datetimeProcessed"
    )
    @classmethod
    def datetime_processed_deserialization(cls, value):
        """Performs `datetime_processed`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @classmethod
    def from_row(cls, row: Mapping) -> "Record":
        """Initialize instance from database row."""
        return cls(
            report_id=row["report_id"],
            success=row["success"],
            token=row.get("job_token"),
            origin_system_id=row.get("origin_system_id"),
            external_id=row.get("external_id"),
            sip_id=row.get("sip_id"),
            ie_id=row.get("ie_id"),
            datetime_processed=row.get("datetime_processed"),
        )


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
    records: Optional[list[Record]] = None

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

    @DataModel.serialization_handler("records", "records")
    @classmethod
    def records_serialization_handler(cls, value):
        """Handles `records`-serialization."""
        if value is None:
            DataModel.skip()
        return [record.json for record in value]

    @DataModel.deserialization_handler("records", "records")
    @classmethod
    def records_deserialization_handler(cls, value):
        """Handles `records`-deserialization."""
        if value is None:
            DataModel.skip()
        return [Record.from_json(record) for record in value]

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
