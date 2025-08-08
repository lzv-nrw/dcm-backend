"""
JobConfig data-model definition
"""

from typing import Optional, Mapping
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from dcm_common.models import DataModel, JSONObject

from .cu_metadata import CuMetadata


@dataclass
class DataSelectionOAI(DataModel):
    """OAI-DataSelection DataModel."""
    identifiers: Optional[list[str]] = None
    sets: Optional[list[str]] = None
    from_: Optional[str] = None
    until: Optional[str] = None

    @DataModel.serialization_handler("from_", "from")
    @classmethod
    def from__serialization_handler(cls, value):
        """Handles `from_`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("from_", "from")
    @classmethod
    def from__deserialization_handler(cls, value):
        """Handles `from_`-deserialization."""
        if value is None:
            DataModel.skip()
        return value


@dataclass
class DataSelectionHotfolder(DataModel):
    """Hotfolder-DataSelection DataModel."""
    path: Optional[str] = None


def get_data_selection_from_json(
    json: Optional[JSONObject],
) -> Optional[DataSelectionOAI | DataSelectionHotfolder]:
    """Heuristically select an appropriate `DataSelection`-type."""
    if json is None:
        return None
    if json == {}:
        return None
    if "path" in json:
        return DataSelectionHotfolder.from_json(json)
    return DataSelectionOAI.from_json(json)


@dataclass
class PluginConfig(DataModel):
    """
    Configuration for generic plugin.

    Keyword arguments
    plugin -- plugin identifier
    args -- plugin arguments
    """

    plugin: Optional[str] = None
    args: Optional[JSONObject] = None


@dataclass
class FileConfig(DataModel):
    """
    Configuration for file-data.

    Keyword arguments
    contents -- mapping-script file contents
    name -- mapping-script file name
    datetime_uploaded -- mapping-script upload datetime
    """
    contents: Optional[str] = None
    name: Optional[str] = None
    datetime_uploaded: Optional[str] = None

    @DataModel.serialization_handler("datetime_uploaded", "datetimeUploaded")
    @classmethod
    def datetime_uploaded_serialization_handler(cls, value):
        """Handles `datetime_uploaded`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("datetime_uploaded", "datetimeUploaded")
    @classmethod
    def datetime_uploaded_deserialization_handler(cls, value):
        """Handles `datetime_uploaded`-deserialization."""
        if value is None:
            DataModel.skip()
        return value


class DataProcessingMappingType(Enum):
    """Enum class for the type of metadata mapping."""
    PLUGIN = "plugin"
    XSLT = "xslt"
    PYTHON = "python"


@dataclass
class DataProcessingMapping(DataModel):
    """DataModel for Mapping-part of DataProcessing."""
    type_: DataProcessingMappingType
    data: PluginConfig | FileConfig

    @classmethod
    def from_json(cls, json: JSONObject):
        """
        Returns `DataProcessingMapping` initialized with data from
        `json`.

        Explicit implementation ensures proper handling of `data`.
        """
        kwargs = {
            "type_": DataProcessingMappingType(json["type"]),
        }

        if kwargs["type_"] is DataProcessingMappingType.PLUGIN:
            kwargs["data"] = PluginConfig.from_json(json["data"])
        else:
            kwargs["data"] = FileConfig.from_json(json["data"])

        return cls(**kwargs)

    @DataModel.serialization_handler("type_", "type")
    @classmethod
    def type__serialization_handler(cls, value):
        """Handles `type_`-serialization."""
        return value.value


@dataclass
class DataProcessingPreparation(DataModel):
    """DataModel for Preparation-part of DataProcessing."""
    rights_operations: Optional[list[JSONObject]] = None
    sig_prop_operations: Optional[list[JSONObject]] = None
    preservation_operations: Optional[list[JSONObject]] = None

    @DataModel.serialization_handler("rights_operations", "rightsOperations")
    @classmethod
    def rights_operations_serialization_handler(cls, value):
        """Handles `rights_operations`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("rights_operations", "rightsOperations")
    @classmethod
    def rights_operations_deserialization_handler(cls, value):
        """Handles `rights_operations`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler(
        "sig_prop_operations", "sigPropOperations"
    )
    @classmethod
    def sig_prop_operations_serialization_handler(cls, value):
        """Handles `sig_prop_operations`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler(
        "sig_prop_operations", "sigPropOperations"
    )
    @classmethod
    def sig_prop_operations_deserialization_handler(cls, value):
        """Handles `sig_prop_operations`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler(
        "preservation_operations", "preservationOperations"
    )
    @classmethod
    def preservation_operations_serialization_handler(cls, value):
        """Handles `preservation_operations`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler(
        "preservation_operations", "preservationOperations"
    )
    @classmethod
    def preservation_operations_deserialization_handler(cls, value):
        """Handles `preservation_operations`-deserialization."""
        if value is None:
            DataModel.skip()
        return value


@dataclass
class DataProcessing(DataModel):
    """DataModel for DataProcessing."""
    mapping: Optional[DataProcessingMapping] = None
    preparation: Optional[DataProcessingPreparation] = None


class TimeUnit(Enum):
    """Enum class for the time units."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


@dataclass
class Repeat(DataModel):
    """
    Data model for repeat. Describes the rule set for repeating jobs.

    Keyword arguments:
    unit -- time unit for repetition
            (day, week, or month)
    interval -- positive integer greater than or equal to 1 for the
                interval of the repetition
                (default 1)
    """
    unit: TimeUnit
    interval: int = 1

    @DataModel.serialization_handler("unit")
    @classmethod
    def unit_serialization_handler(cls, value):
        """Handles `unit`-serialization."""
        return value.value

    @DataModel.deserialization_handler("unit")
    @classmethod
    def unit_deserialization_handler(cls, value):
        """Handles `unit`-deserialization."""
        return TimeUnit(value)


@staticmethod
def _load_datetime(
    _datetime: Optional[str | datetime]
) -> Optional[datetime]:
    """ Convert the input of string or datetime into a datetime object. """
    if _datetime is None:
        return None
    if isinstance(_datetime, str):
        return datetime.fromisoformat(_datetime)
    return _datetime


class Schedule(DataModel):
    """
    Data model for schedule. Describes the scheduling rule set.

    If active, `start` needs to be provided. (If `repeat is None`, use
    `start` for onetime-execution. Otherwise `start` is used as origin
    for repeated scheduling.)

    Keyword arguments:
    active -- boolean whether the schedule is active or not
    start -- ISO-datetime for the start time of a scheduling
             (default None)
    end -- ISO-datetime for the end time of a scheduling
           (default None)
    repeat -- repeat object for scheduling
              (default None)
    """
    active: bool
    start: Optional[str | datetime]
    end: Optional[str | datetime]
    repeat: Optional[Repeat]

    def __init__(
        self,
        active: bool,
        start: Optional[str | datetime] = None,
        end: Optional[str | datetime] = None,
        repeat: Optional[Repeat] = None,
    ) -> None:
        self.active = active
        self.start = _load_datetime(start)
        self.end = _load_datetime(end)
        self.repeat = repeat

    @DataModel.serialization_handler("start")
    @classmethod
    def start_serialization_handler(cls, value):
        """Handles `start`-serialization."""
        if value is None:
            DataModel.skip()
        return value.isoformat()

    @DataModel.deserialization_handler("start")
    @classmethod
    def start_deserialization_handler(cls, value):
        """Handles `start`-deserialization."""
        if value is None:
            DataModel.skip()
        return _load_datetime(value)

    @DataModel.serialization_handler("end")
    @classmethod
    def end_serialization_handler(cls, value):
        """Handles `end`-serialization."""
        if value is None:
            DataModel.skip()
        return value.isoformat()

    @DataModel.deserialization_handler("end")
    @classmethod
    def end_deserialization_handler(cls, value):
        """Handles `end`-deserialization."""
        if value is None:
            DataModel.skip()
        return _load_datetime(value)

    @DataModel.deserialization_handler("repeat")
    @classmethod
    def repeat_deserialization_handler(cls, value):
        """Handles `repeat`-deserialization."""
        if value is None:
            DataModel.skip()
        return Repeat.from_json(value)


class JobConfig(CuMetadata):
    """
    Data model for a job config.

    Keyword arguments:
    template_id -- id of associated template
    status -- status of the job configuration
    id_ -- (unique) identifier of the job config
           (default None)
    latest_exec -- token value for latest execution of this job config
                   (default None)
    name -- configuration display name
            (default None)
    description -- configuration description
                   (default None)
    contact_info -- configuration contact information
                    (default None)
    data_selection -- data selection-configuration
                      (default None)
    data_processing -- data processing-configuration
                       (default None)
    schedule -- scheduling-configuration
                (default None)
    workspace_id -- id of associated workspace
                    (default None)
    scheduled_exec -- datetime of planned execution
                      (default None)

    Inherits metadata-fields from `CuMetadata`.
    """

    template_id: str
    status: str
    id_: Optional[str]
    latest_exec: Optional[str]
    name: Optional[str]
    description: Optional[str]
    contact_info: Optional[str]
    data_selection: Optional[DataSelectionOAI | DataSelectionHotfolder]
    data_processing: Optional[DataProcessing]
    schedule: Optional[Schedule]
    workspace_id: Optional[str]
    scheduled_exec: Optional[datetime]

    def __init__(
        self,
        *,
        template_id: str,
        status: str,
        id_: Optional[str] = None,
        latest_exec: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        contact_info: Optional[str] = None,
        data_selection: Optional[
            DataSelectionOAI | DataSelectionHotfolder
        ] = None,
        data_processing: Optional[DataProcessing] = None,
        schedule: Optional[Schedule] = None,
        workspace_id: Optional[str] = None,
        scheduled_exec: Optional[datetime] = None,
        **kwargs,
    ) -> None:
        self.template_id = template_id
        self.status = status
        self.id_ = id_
        self.latest_exec = latest_exec
        self.name = name
        self.description = description
        self.contact_info = contact_info
        self.data_selection = data_selection
        self.data_processing = data_processing
        self.schedule = schedule
        self.workspace_id = workspace_id
        self.scheduled_exec = scheduled_exec
        super().__init__(**kwargs)

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

    @DataModel.serialization_handler("latest_exec", "latestExec")
    @classmethod
    def latest_exec_serialization_handler(cls, value):
        """Handles `latest_exec`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("latest_exec", "latestExec")
    @classmethod
    def latest_exec_deserialization_handler(cls, value):
        """Handles `latest_exec`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("id_", "id")
    @classmethod
    def id__serialization_handler(cls, value):
        """Handles `id_`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("id_", "id")
    @classmethod
    def id__deserialization_handler(cls, value):
        """Handles `id_`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("contact_info", "contactInfo")
    @classmethod
    def contact_info_serialization_handler(cls, value):
        """Handles `contact_info`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("contact_info", "contactInfo")
    @classmethod
    def contact_info_deserialization_handler(cls, value):
        """Handles `contact_info`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("data_selection", "dataSelection")
    @classmethod
    def data_selection_serialization_handler(cls, value):
        """Handles `data_selection`-serialization."""
        if value is None:
            DataModel.skip()
        return value.json

    @DataModel.deserialization_handler("data_selection", "dataSelection")
    @classmethod
    def data_selection_deserialization_handler(cls, value):
        """Handles `data_selection`-deserialization."""
        return get_data_selection_from_json(value)

    @DataModel.serialization_handler("data_processing", "dataProcessing")
    @classmethod
    def data_processing_serialization_handler(cls, value):
        """Handles `data_processing`-serialization."""
        if value is None:
            DataModel.skip()
        return value.json

    @DataModel.deserialization_handler("data_processing", "dataProcessing")
    @classmethod
    def data_processing_deserialization_handler(cls, value):
        """Handles `data_processing`-deserialization."""
        if value is None:
            DataModel.skip()
        return DataProcessing.from_json(value)

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

    @DataModel.serialization_handler("scheduled_exec", "scheduledExec")
    @classmethod
    def scheduled_exec_serialization_handler(cls, value):
        """Handles `scheduled_exec`-serialization."""
        if value is None:
            DataModel.skip()
        return value.isoformat()

    @DataModel.deserialization_handler("scheduled_exec", "scheduledExec")
    @classmethod
    def scheduled_exec_deserialization_handler(cls, value):
        """Handles `scheduled_exec`-deserialization."""
        if value is None:
            DataModel.skip()
        return datetime.fromisoformat(value)

    @property
    def row(self) -> dict:
        """Convert to database row."""
        # some properties like workspace_id or scheduled_exec are not
        # included in the job_configs-table
        row = {
            "template_id": self.template_id,
            "status": self.status,
            "latest_exec": self.latest_exec,
            "name": self.name,
            "description": self.description,
            "contact_info": self.contact_info,
            "data_selection": (
                None
                if self.data_selection is None
                else self.data_selection.json
            ),
            "data_processing": (
                None
                if self.data_processing is None
                else self.data_processing.json
            ),
            "schedule": (
                None
                if self.schedule is None
                else self.schedule.json
            ),
        }
        if self.id_ is not None:
            row["id"] = self.id_
        if self.user_created is not None:
            row["user_created"] = self.user_created
        if self.datetime_created is not None:
            row["datetime_created"] = self.datetime_created
        if self.user_modified is not None:
            row["user_modified"] = self.user_modified
        if self.datetime_modified is not None:
            row["datetime_modified"] = self.datetime_modified
        return row

    @classmethod
    def from_row(cls, row: Mapping) -> "JobConfig":
        """Initialize instance from database row."""
        # some properties like workspace_id or scheduled_exec are not
        # included in the job_configs-table
        return cls(
            template_id=row.get("template_id"),
            status=row.get("status"),
            id_=row.get("id"),
            latest_exec=row.get("latest_exec"),
            name=row.get("name"),
            description=row.get("description"),
            contact_info=row.get("contact_info"),
            data_selection=get_data_selection_from_json(
                row.get("data_selection")
            ),
            data_processing=(
                None
                if row.get("data_processing") is None
                else DataProcessing.from_json(row.get("data_processing"))
            ),
            schedule=(
                None
                if row.get("schedule") is None
                else Schedule.from_json(row.get("schedule"))
            ),
            user_created=row.get("user_created"),
            datetime_created=row.get("datetime_created"),
            user_modified=row.get("user_modified"),
            datetime_modified=row.get("datetime_modified"),
        )
