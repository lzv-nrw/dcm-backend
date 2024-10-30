"""JobConfig-data model test-module."""

from datetime import datetime

import pytest
from dcm_common.util import now
from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import Repeat, Schedule, \
    JobConfig, TimeUnit, Weekday


def test_unit_enum():
    """Test enums of class `TimeUnit`."""
    assert TimeUnit.MINUTE.value == "minute"
    assert TimeUnit("day") == TimeUnit.DAY


def test_weekday_enum():
    """Test enums of class `Weekday`."""
    assert Weekday.WEDNESDAY.value == "wednesday"
    assert Weekday("friday") == Weekday.FRIDAY


test_repeat_json = get_model_serialization_test(
    Repeat, (
        ((TimeUnit.DAY, 1), {}),
        ((Weekday.THURSDAY, 2), {}),
        (("minute", 2), {}),
        (("monday", 3), {}),
    )
)


test_schedule_json = get_model_serialization_test(
    Schedule, (
        ((True,), {}),
        ((True,), {"start": "2024-01-01T00:00:00+01:00"}),
        ((True,), {"start": now()}),
        ((True,), {"end": "2024-01-01T00:00:00+01:00"}),
        ((True,), {"end": now()}),
        ((True,), {"repeat": Repeat(TimeUnit.DAY, 1)}),
    )
)


def test_job_config_constructor():
    """Test constructor logic of class `JobConfig`."""
    t0 = now()
    config = JobConfig({})
    t1 = now()

    assert t0 <= datetime.fromisoformat(config.last_modified) <= t1

    with pytest.raises(ValueError):
        JobConfig({}, last_modified="-")


test_jobconfig_json = get_model_serialization_test(
    JobConfig, (
        (({"key": "value"}), {}),
        (("", "2024-01-01T00:00:00+01:00", {"key": "value"}), {
            "schedule": Schedule(True)
        }),
    )
)
