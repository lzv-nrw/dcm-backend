"""Test module for `Scheduler` and related components."""

from typing import Optional
import os
from time import time, sleep
from datetime import datetime, timedelta
import zoneinfo
import threading
from dataclasses import dataclass

import pytest

from dcm_backend.models import Schedule, Repeat, TimeUnit
from dcm_backend.components.scheduler import (
    Timeout,
    ScheduledJobConfig,
    Scheduler,
)


VERY_SHORT = float(os.environ.get("TEST_SCHEDULER_VERY_SHORT_VALUE", 0.01))
VERY_LONG = 10000.0


class Callback:
    """Test-callback."""

    def __init__(self):
        self.first_run = threading.Event()
        self.has_been_called = False
        self.has_been_called_with = []
        self.call_count = 0

    def callback(self, *args, **kwargs):
        """Generic callback-method."""
        self.has_been_called = True
        self.has_been_called_with.append((args, kwargs))
        self.call_count += 1
        self.first_run.set()


@dataclass
class JobConfig(ScheduledJobConfig):
    """Minimal implementation of the `ScheduledJobConfig` interface."""

    id_: str
    schedule: Optional[Schedule] = None


def test_timeout_basic():
    """Test basic timeout."""
    on_timeout = Callback()
    on_success = Callback()
    on_completion = Callback()

    t = Timeout(
        VERY_SHORT,
        on_timeout.callback,
        on_success=on_success.callback,
        on_completion=on_completion.callback,
    )
    t.start()
    t.wait()

    assert on_timeout.has_been_called
    assert on_success.has_been_called
    assert on_completion.has_been_called


def test_timeout_cancel():
    """Test canceling timeout."""
    on_timeout = Callback()
    on_cancel = Callback()
    on_completion = Callback()

    t = Timeout(
        VERY_LONG,
        on_timeout.callback,
        on_cancel=on_cancel.callback,
        on_completion=on_completion.callback,
    )
    assert not t.running
    t.start()
    assert t.running
    assert not t.canceled
    time0 = time()
    t.wait(VERY_SHORT)
    t.cancel(True)
    assert VERY_SHORT == pytest.approx(
        # validate actual wait-time was roughly VERY_SHORT
        time() - time0,
        rel=VERY_SHORT,
        abs=VERY_SHORT,
    )
    assert not t.running
    assert t.canceled

    assert not on_timeout.has_been_called
    assert on_cancel.has_been_called
    assert on_completion.has_been_called


def test_timeout_error():
    """
    Test behavior for `Timeout` if a callback raises an error.
    """

    def raise_error():
        raise ValueError("Test")

    on_error = Callback()

    t = Timeout(VERY_SHORT, raise_error, on_error=on_error.callback)
    t.start()
    t.wait()

    assert on_error.has_been_called
    assert len(on_error.has_been_called_with) == 1
    assert isinstance(on_error.has_been_called_with[0][0][0], ValueError)


def test_timeout_max_duration():
    """
    Test timeout for extremely long duration (about 292 years) does not
    raise an error.
    """
    t = Timeout(292 * 365 * 24 * 60 * 60, Callback().callback)
    t.start()
    t.wait(VERY_SHORT)
    t.cancel(True)


def test_scheduler_schedule_at_now():
    """Test basic use of `schedule_at` (default)."""
    on_timeout = Callback()
    s = Scheduler(lambda c: on_timeout.callback)
    plan = s.schedule_at(JobConfig("test-id"))
    plan.timeout.wait()

    assert on_timeout.has_been_called
    assert on_timeout.call_count == 1

    # does not get re-scheduled
    assert len(s.get_plans(plan.config.id_)) == 0


def test_scheduler_schedule_at_datetime():
    """Test basic use of `schedule_at` (with datetime)."""
    on_timeout = Callback()
    s = Scheduler(lambda c: on_timeout.callback)
    plan = s.schedule_at(
        JobConfig("test-id"),
        datetime.now() + timedelta(seconds=2 * VERY_SHORT),
    )
    # does not run immediately
    sleep(VERY_SHORT)
    assert not on_timeout.has_been_called

    # does run at some point
    on_timeout.first_run.wait()

    assert on_timeout.has_been_called
    assert on_timeout.call_count == 1

    # does not get re-scheduled
    assert len(s.get_plans(plan.config.id_)) == 0


def test_scheduler_schedule_at_datetime_with_timezone():
    """Test use of `schedule_at` (with datetime and tzinfo)."""
    on_timeout = Callback()
    s = Scheduler(lambda c: on_timeout.callback, "Etc/GMT-1")

    # schedule for one hour from now but in a different timezone
    plan = s.schedule_at(
        JobConfig("test-id"),
        datetime.now(zoneinfo.ZoneInfo("Etc/GMT+1")) + timedelta(hours=1),
    )

    # scheduler correctly translates to one hour from now
    assert plan.timeout.timeout == pytest.approx(3600)

    s.clear(True)


@pytest.mark.parametrize(
    "clear",
    [
        lambda s, plan_id, job_id: s.clear(True),
        lambda s, plan_id, job_id: s.clear_jobs(job_id, True),
        lambda s, plan_id, job_id: s.clear_plan(plan_id, True),
    ],
    ids=["clear", "clear_jobs", "clear_plan"],
)
def test_scheduler_schedule_at_clear(clear):
    """Test `Scheduler.clear`-type methods."""
    on_timeout = Callback()
    s = Scheduler(lambda c: on_timeout.callback)
    plan = s.schedule_at(
        JobConfig("test-id"), datetime.now() + timedelta(seconds=VERY_LONG)
    )

    # cancel all
    clear(s, plan.id_, plan.config.id_)

    # check for canceled plan and job did not run
    assert not plan.timeout.running
    assert not on_timeout.has_been_called
    assert len(s.get_plans(plan.config.id_)) == 0


def test_scheduler_schedule():
    """
    Test behavior of `Scheduler.schedule` for different `Schedules`.
    """
    s = Scheduler(lambda c: Callback().callback)
    # no schedule -> None
    assert s.schedule(JobConfig("test-id", None), None) is None
    # inactive schedule -> None
    assert s.schedule(JobConfig("test-id", Schedule(False)), None) is None
    # active schedule but missing start -> error
    with pytest.raises(ValueError):
        s.schedule(JobConfig("test-id", Schedule(True)), None)
    # active schedule with start -> ok
    assert (
        s.schedule(
            JobConfig("test-id", Schedule(True, start=datetime.now())), None
        )
        is not None
    )
    # active schedule with start but unknown unit in repeat -> error
    with pytest.raises(NotImplementedError):
        s.schedule(
            JobConfig(
                "test-id",
                Schedule(
                    True, start=datetime.now(), repeat=Repeat("unknown", 1)
                ),
            ),
            None,
        )
    # active schedule with start and repeat -> ok
    assert (
        s.schedule(
            JobConfig(
                "test-id",
                Schedule(
                    True, start=datetime.now(), repeat=Repeat(TimeUnit.DAY, 1)
                ),
            ),
            None,
        )
        is not None
    )
    s.clear(True)


def test_scheduler_schedule_wo_repeat():
    """Test basic use of `schedule` without repeat."""
    on_timeout = Callback()
    s = Scheduler(lambda c: on_timeout.callback)
    plan = s.schedule(
        JobConfig(
            "test-id",
            Schedule(
                True, start=datetime.now() + timedelta(seconds=2 * VERY_SHORT)
            ),
        )
    )
    # does not run immediately
    sleep(VERY_SHORT)
    assert not on_timeout.has_been_called

    # does run at some point
    on_timeout.first_run.wait()
    plan.timeout.wait()

    assert on_timeout.has_been_called
    assert on_timeout.call_count == 1

    # does not get re-scheduled
    assert len(s.get_plans(plan.config.id_)) == 0


def test_scheduler_schedule_repeat():
    """Test basic use of `schedule` without repeat."""
    on_timeout = Callback()
    s = Scheduler(lambda c: on_timeout.callback)
    plan = s.schedule(
        JobConfig(
            "test-id",
            Schedule(
                True,
                start=datetime.now() + timedelta(seconds=2 * VERY_SHORT),
                repeat=Repeat(TimeUnit.DAY),
            ),
        )
    )
    # does not run immediately
    sleep(VERY_SHORT)
    assert not on_timeout.has_been_called

    # does run at some point
    on_timeout.first_run.wait()
    plan.timeout.wait()

    assert on_timeout.has_been_called
    assert on_timeout.call_count == 1

    # does get re-scheduled
    assert len(s.get_plans(plan.config.id_)) == 1

    s.clear(True)


def test_scheduler_schedule_repeat_start_in_past():
    """
    Test use of `schedule` without repeat but start/previous-date in
    past.
    """
    on_timeout = Callback()
    s = Scheduler(lambda c: on_timeout.callback)
    plan = s.schedule(
        JobConfig(
            "test-id",
            Schedule(
                True,
                start=datetime.now()
                + timedelta(minutes=-1, seconds=2 * VERY_SHORT),
                repeat=Repeat(TimeUnit.SECOND),
            ),
        )
    )
    # does run immediately
    plan.timeout.wait(VERY_SHORT)
    assert on_timeout.has_been_called

    # wait a VERY_SHORT while longer
    sleep(VERY_SHORT)

    # did not run repeatedly
    assert on_timeout.call_count == 1

    s.clear(True)


def test_scheduler_plan_end():
    """Test method `Scheduler.plan` for job with end-date."""
    now = datetime.now().astimezone()
    past = now + timedelta(days=-1)

    s = Scheduler(lambda c: Callback().callback)

    assert s.plan(JobConfig("test-id", Schedule(True, start=now)), None) == now
    assert (
        s.plan(JobConfig("test-id", Schedule(True, start=now, end=past)), None)
        is None
    )


def test_scheduler_plan_onetime():
    """Test method `Scheduler.plan` for onetime-scheduling."""
    now = datetime.now().astimezone()
    future = now + timedelta(days=1)

    s = Scheduler(lambda c: Callback().callback)

    assert (
        s.plan(JobConfig("test-id"), None) is None
    ), "case: no schedule, no previous"
    assert (
        s.plan(JobConfig("test-id", Schedule(False)), None) is None
    ), "case: schedule-false, no previous"
    assert (
        s.plan(JobConfig("test-id", Schedule(True, start=now)), None) == now
    ), "case: schedule now, no previous"
    assert (
        s.plan(JobConfig("test-id", Schedule(True, start=future)), None)
        == future
    ), "case: schedule future, no previous"
    assert (
        s.plan(JobConfig("test-id", Schedule(True, start=now)), now) is None
    ), "case: schedule now, previously now"
    assert (
        s.plan(JobConfig("test-id", Schedule(True, start=future)), now) is None
    ), "case: schedule future, previously now"
    assert (
        s.plan(JobConfig("test-id", Schedule(True, start=future)), future)
        is None
    ), "case: schedule future, previously future"


@pytest.mark.parametrize(
    ("timedelta_unit", "enum_unit"),
    [
        ("seconds", TimeUnit.SECOND),
        ("minutes", TimeUnit.MINUTE),
        ("hours", TimeUnit.HOUR),
        ("days", TimeUnit.DAY),
        ("weeks", TimeUnit.WEEK),
    ],
    ids=["seconds", "minutes", "hours", "days", "weeks"],
)
def test_scheduler_plan_simple_units(timedelta_unit, enum_unit):
    """Test method `Scheduler.plan` for simple unit-scheduling."""
    now = datetime.now().astimezone()
    future = now + timedelta(**{timedelta_unit: 1})

    s = Scheduler(lambda c: Callback().callback)

    assert (
        s.plan(
            JobConfig(
                "test-id",
                Schedule(True, start=now, repeat=Repeat(enum_unit)),
            ),
            None,
        )
        == now
    ), "case: schedule now, no previous"
    assert (
        s.plan(
            JobConfig(
                "test-id",
                Schedule(True, start=now, repeat=Repeat(enum_unit)),
            ),
            now,
        )
        == future
    ), "case: schedule now, previously now"
    assert (
        s.plan(
            JobConfig(
                "test-id",
                Schedule(True, start=now, repeat=Repeat(enum_unit)),
            ),
            now + timedelta(**{timedelta_unit: 0.4}),
        )
        == future
    ), f"case: schedule now, previously now + .4 {timedelta_unit}"
    assert s.plan(
        JobConfig(
            "test-id",
            Schedule(True, start=now, repeat=Repeat(enum_unit)),
        ),
        now + timedelta(**{timedelta_unit: 0.6}),
    ) == future + timedelta(
        **{timedelta_unit: 1}
    ), f"case: schedule now, previously now + .6 {timedelta_unit}"
    assert s.plan(
        JobConfig(
            "test-id",
            Schedule(True, start=now, repeat=Repeat(enum_unit, 2)),
        ),
        now,
    ) == now + timedelta(
        **{timedelta_unit: 2}
    ), f"case: schedule now, previously now, 2-{timedelta_unit} interval"


def test_scheduler_plan_monthly():
    """Test method `Scheduler.plan` for monthly-scheduling."""
    tzinfo = datetime.now().astimezone().tzinfo
    now = datetime(2025, 1, 1, 12)
    future = datetime(2025, 2, 1, 12)

    s = Scheduler(lambda c: Callback().callback)

    assert s.plan(
        JobConfig(
            "test-id",
            Schedule(True, start=now, repeat=Repeat(TimeUnit.MONTH)),
        ),
        None,
    ).astimezone(tzinfo) == now.astimezone(
        tzinfo
    ), "case: schedule now, no previous"
    assert s.plan(
        JobConfig(
            "test-id",
            Schedule(True, start=now, repeat=Repeat(TimeUnit.MONTH)),
        ),
        now,
    ).astimezone(tzinfo) == future.astimezone(
        tzinfo
    ), "case: schedule now, previously now"
    assert s.plan(
        JobConfig(
            "test-id",
            Schedule(True, start=now, repeat=Repeat(TimeUnit.MONTH)),
        ),
        datetime(2025, 1, 13, 12),
    ).astimezone(tzinfo) == future.astimezone(
        tzinfo
    ), "case: schedule now, previously now + ~.4 months"
    assert s.plan(
        JobConfig(
            "test-id",
            Schedule(True, start=now, repeat=Repeat(TimeUnit.MONTH)),
        ),
        datetime(2025, 1, 18, 12),
    ).astimezone(tzinfo) == datetime(2025, 3, 1, 12).astimezone(
        tzinfo
    ), "case: schedule now, previously now + ~.6 months"
    assert s.plan(
        JobConfig(
            "test-id",
            Schedule(True, start=now, repeat=Repeat(TimeUnit.MONTH, 2)),
        ),
        now,
    ).astimezone(tzinfo) == datetime(2025, 3, 1, 12).astimezone(
        tzinfo
    ), "case: schedule now, previously now, 2-months interval"

    #  # special cases

    #  ## start-date at end of month
    assert s.plan(  # starting on Jan 31st
        JobConfig(
            "test-id",
            Schedule(
                True,
                start=datetime(2025, 1, 31, 12, tzinfo=tzinfo),
                repeat=Repeat(TimeUnit.MONTH),
            ),
        ),
        datetime(2025, 1, 31, 12, tzinfo=tzinfo),
    ).astimezone(tzinfo) == datetime(2025, 2, 28, 12, tzinfo=tzinfo)
    assert s.plan(  # starting on Jan 31st, previous on Feb 28th
        JobConfig(
            "test-id",
            Schedule(
                True,
                start=datetime(2025, 1, 31, 12, tzinfo=tzinfo),
                repeat=Repeat(TimeUnit.MONTH),
            ),
        ),
        datetime(2025, 2, 28, 12, tzinfo=tzinfo),
    ).astimezone(tzinfo) == datetime(2025, 3, 31, 12, tzinfo=tzinfo)
    assert s.plan(  # starting on Jan 31st, previous on Mar 31st
        JobConfig(
            "test-id",
            Schedule(
                True,
                start=datetime(2025, 1, 31, 12, tzinfo=tzinfo),
                repeat=Repeat(TimeUnit.MONTH),
            ),
        ),
        datetime(2025, 3, 31, 12, tzinfo=tzinfo),
    ).astimezone(tzinfo) == datetime(2025, 4, 30, 12, tzinfo=tzinfo)
    assert s.plan(  # starting on Jan 31st, previous on Sep 30th
        JobConfig(
            "test-id",
            Schedule(
                True,
                start=datetime(2025, 1, 31, 12, tzinfo=tzinfo),
                repeat=Repeat(TimeUnit.MONTH),
            ),
        ),
        datetime(2025, 9, 30, 13, tzinfo=tzinfo),
    ).astimezone(tzinfo) == datetime(2025, 10, 31, 12, tzinfo=tzinfo)

    # ## start date close to end of year
    assert s.plan(
        JobConfig(
            "test-id",
            Schedule(
                True,
                start=datetime(2024, 12, 15, 12, tzinfo=tzinfo),
                repeat=Repeat(TimeUnit.MONTH),
            ),
        ),
        datetime(2024, 12, 15, 12, tzinfo=tzinfo),
    ).astimezone(tzinfo) == datetime(2025, 1, 15, 12, tzinfo=tzinfo)


def test_scheduler_plan_monthly_with_default_timezone():
    """
    Test method `Scheduler.plan` for monthly-scheduling and a specific
    default timezone.
    """
    s = Scheduler(lambda c: Callback().callback, "Etc/GMT+6")

    # check test does not use default timezone
    now = datetime.now()
    local_tzinfo = now.astimezone().tzinfo
    if s.zoneinfo.utcoffset(now) == local_tzinfo.utcoffset(now):
        s = Scheduler(lambda c: Callback().callback, "Etc/GMT+5")

    # check whether scheduler uses default timezone
    assert (
        s.plan(
            JobConfig(
                "test-id",
                Schedule(
                    True,
                    start=datetime(2025, 1, 15, 12),
                    repeat=Repeat(TimeUnit.MONTH),
                ),
            ),
            None,
        ).tzinfo
        == s.zoneinfo
    )

    # check whether scheduler uses timezone of start if available
    assert (
        s.plan(
            JobConfig(
                "test-id",
                Schedule(
                    True,
                    start=datetime(2025, 1, 15, 12).astimezone(local_tzinfo),
                    repeat=Repeat(TimeUnit.MONTH),
                ),
            ),
            None,
        ).tzinfo
        == local_tzinfo
    )
