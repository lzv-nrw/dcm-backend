"""
JobConfig data-model definition
"""

from typing import Optional
from datetime import datetime
from enum import Enum

from dcm_common.util import now
from dcm_common.models import DataModel, JSONObject


class TimeUnit(Enum):
    """Enum class for the time unit."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"


class Weekday(Enum):
    """Enum class for the weekdays."""
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"


class Repeat(DataModel):
    """
    Data model for repeat. Describes the rule set for repeating jobs.

    Keyword arguments:
    unit -- time unit of repeating
            (seconds, minutes, hours, day, week, or weekday)
    interval -- positive integer greater than or equal to 1 for the
                interval of the repetition

    Example outputs:
    For every two days: {"unit": "day", "interval": 2}

    For every Friday: {"unit": "Friday", "interval": 1}
    """
    unit: TimeUnit | Weekday
    interval: int

    def __init__(self, unit: str | TimeUnit | Weekday, interval: int) -> None:
        if isinstance(unit, str):
            try:
                self.unit = TimeUnit(unit)
            except ValueError:
                self.unit = Weekday(unit)
        else:
            self.unit = unit
        self.interval = interval

    @DataModel.serialization_handler("unit")
    @classmethod
    def unit_serialization_handler(cls, value):
        """Handles `unit`-serialization."""
        return value.value

    @DataModel.deserialization_handler("unit")
    @classmethod
    def unit_deserialization_handler(cls, value):
        """Handles `unit`-deserialization."""
        try:
            return TimeUnit(value)
        except ValueError:
            return Weekday(value)


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

    Keyword arguments:
    active -- boolean whether the schedule is active or not
              (i.e., not paused or paused, respectively)
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


class JobConfig(DataModel):
    """
    Data model for a job config.

    Keyword arguments:
    job -- key-value pairs of arguments field for job-config details
    id_ -- (unique) identifier of the job config
           (default None)
    name -- user-defined name for config
            (default None)
    last_modified -- ISO-8601 datetime-stamp of last modification
                     (default None; automatically sets this via
                     `dcm_common.util.now`)
    schedule -- the scheduling rule set
                (default None)
    """
    job: JSONObject
    id_: Optional[str] = None
    name: Optional[str] = None
    last_modified: Optional[str] = None
    schedule: Optional[Schedule] = None

    def __init__(
        self,
        job: JSONObject,
        id_: Optional[str] = None,
        name: Optional[str] = None,
        last_modified: Optional[str] = None,
        schedule: Optional[Schedule] = None
    ) -> None:
        self.job = job
        self.id_ = id_
        self.name = name
        if last_modified:
            datetime.fromisoformat(last_modified)
            self.last_modified = last_modified
        else:
            self.last_modified = now().isoformat()
        self.schedule = schedule

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

    @DataModel.deserialization_handler("name")
    @classmethod
    def name_deserialization_handler(cls, value):
        """Handles `name`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("last_modified")
    @classmethod
    def last_modified_deserialization_handler(cls, value):
        """Handles `last_modified`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("schedule")
    @classmethod
    def schedule_deserialization_handler(cls, value):
        """Handles `schedule`-deserialization."""
        if value is None:
            DataModel.skip()
        return Schedule.from_json(value)
