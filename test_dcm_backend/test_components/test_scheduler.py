"""ArchiveController-component test-module."""

from typing import Optional
from time import sleep, time
from datetime import datetime, timedelta
from dataclasses import dataclass

import pytest
from dcm_common.util import now
from dcm_common import Logger, LoggingContext as Context
from dcm_common.services import APIResult

from dcm_backend.components import Scheduler
from dcm_backend.models import Schedule, Repeat, TimeUnit, Weekday


RESPONSES_DICT = {}


@dataclass
class _Job:
    """
    Helper dataclass for saving the job results.

    Keyword arguments:
    timestamp -- datetime stamp
    info -- results
    """

    timestamp: datetime
    info: APIResult


@pytest.fixture(name="scheduler")
def _scheduler(job_processor_adapter):

    def submit_job(config) -> tuple[Optional[str], Logger]:
        """
        Submit a job to a Service for further processing.

        Keyword arguments:
        config -- configuration for the job that has to be scheduled
        """

        if config.id_ not in RESPONSES_DICT:
            RESPONSES_DICT[config.id_] = []

        job_processor_adapter.run(
            config.job,
            None,
            info := APIResult()
        )

        RESPONSES_DICT[config.id_].append(
            _Job(
                timestamp=now(),
                info=info
            )
        )

    return Scheduler(
        job_cmd=submit_job
    )


@pytest.mark.parametrize(
    "time_unit",
    [
        TimeUnit.SECOND,
        TimeUnit.MINUTE,
        TimeUnit.HOUR,
        TimeUnit.DAY,
        TimeUnit.WEEK
    ],
)
def test_schedule_time_unit(scheduler, time_unit, job_config):
    """
    Minimal test for the method `schedule` of the `Scheduler`
    with a `TimeUnit` object in the `Repeat` object.
    """

    _interval = 3

    # update the job_config
    job_config.schedule.repeat = Repeat(
        unit=time_unit,
        interval=_interval
    )

    scheduled, _ = scheduler.schedule(job_config)
    assert scheduled
    assert len(scheduler.get_jobs()) == 1
    job = scheduler.get_jobs()[0]
    assert job.interval == _interval
    assert job.unit == time_unit.value + "s"


@pytest.mark.parametrize(
    "unit",
    [
        Weekday.MONDAY,
        Weekday.TUESDAY,
        Weekday.WEDNESDAY,
        Weekday.THURSDAY,
        Weekday.FRIDAY,
        Weekday.SATURDAY,
        Weekday.SUNDAY,
    ],
)
def test_schedule_weekday(scheduler, unit, job_config):
    """
    Minimal test for the method `schedule` of the `Scheduler`
    with weekdays in the `Repeat` object.
    """

    _interval = 1  # The only interval currently supported for weekdays

    # update the job_config
    job_config.schedule.repeat = Repeat(
        unit=unit,
        interval=_interval
    )

    scheduled, _ = scheduler.schedule(job_config)
    assert scheduled
    assert len(scheduler.get_jobs()) == 1
    job = scheduler.get_jobs()[0]
    assert job.interval == _interval
    assert job.unit == "weeks"


def test_cancel_minimal(scheduler, job_config):
    """
    Minimal test for the method `cancel` of the `Scheduler`.
    """

    # update the job_config
    job_config.schedule = Schedule(
        active=True,
        end=now(),
        repeat=Repeat(unit=TimeUnit.MINUTE, interval=1)
    )

    scheduled, _ = scheduler.schedule(job_config)
    assert scheduled
    assert len(scheduler.get_jobs()) == 1

    scheduler.cancel(job_config.id_)
    assert len(scheduler.get_jobs()) == 0


def test_run_pending(
    scheduler,
    run_job_processor_dummy,
    job_config,
    job_id,
    jp_token,
    jp_report
):
    """
    Minimal test for the method `run_pending` of the `Scheduler`.
    """

    RESPONSES_DICT.clear()
    run_job_processor_dummy(
        post_response=jp_token,
        get_response=jp_report
    )

    scheduler.schedule(job_config)

    # a pause is required to ensure `run_pending` will execute the job
    sleep(1)
    scheduler.run_pending()

    assert job_id in RESPONSES_DICT
    assert isinstance(RESPONSES_DICT[job_id], list)
    assert len(RESPONSES_DICT[job_id]) == 1
    assert isinstance(RESPONSES_DICT[job_id][-1], _Job)
    assert RESPONSES_DICT[job_id][-1].info.report["token"]["value"] == \
        jp_token["value"]

    sleep(1)
    scheduler.run_pending()

    assert len(RESPONSES_DICT[job_id]) == 2
    assert isinstance(RESPONSES_DICT[job_id][-1], _Job)


def test_run_pending_as_thread(
    scheduler,
    run_job_processor_dummy,
    job_config,
    job_id,
    jp_token,
    jp_report
):
    """
    Minimal test for the method `run_pending` of the `Scheduler`
    via `as_thread`.
    """

    RESPONSES_DICT.clear()
    run_job_processor_dummy(
        post_response=jp_token,
        get_response=jp_report
    )

    t = scheduler.as_thread(interval=0.1, daemon=True)
    t.start()

    scheduler.schedule(job_config)

    time0 = time()
    while len(RESPONSES_DICT.get(job_id, [])) == 0 and time() - time0 <= 2:
        sleep(0.01)
    scheduler.stop()

    time0 = time()
    while t.is_alive() and time() - time0 <= 2:
        sleep(0.01)

    assert isinstance(RESPONSES_DICT[job_id], list)
    assert len(RESPONSES_DICT[job_id]) == 1

    sleep(1)

    assert len(RESPONSES_DICT[job_id]) == 1


def test_schedule_end(scheduler, job_config):
    """
    Test for using a `Scheduler` object to run scheduled jobs.
    """

    # update the job_config
    job_config.schedule = Schedule(
        active=True,
        end=now() + timedelta(seconds=1),
        repeat=Repeat(unit=TimeUnit.SECOND, interval=1)
    )

    scheduler.schedule(job_config)
    assert len(scheduler.get_jobs()) == 1  # job is scheduled

    # Run pending jobs to get next schedule
    sleep(2)
    scheduler.run_pending()

    assert len(scheduler.get_jobs()) == 0  # job has been cancelled


def test_no_connection(scheduler, run_job_processor_dummy, job_config, job_id):
    """
    Test logging of error when the `Scheduler` cannot establish
    a connection to the Job Processor.
    """

    run_job_processor_dummy(error_code=503)

    scheduler.schedule(job_config)

    sleep(1)
    scheduler.run_pending()

    _log = Logger.from_json(RESPONSES_DICT[job_id][-1].info.report["log"])
    assert isinstance(_log, Logger)
    assert Context.ERROR in _log


def test_schedule_weekday_interval_unsupported(scheduler, job_config):
    """
    Test for the method `schedule` of the `Scheduler`
    with a weekday and an unsupported interval in the `Repeat` object.
    """

    # update the job_config
    job_config.schedule.repeat = Repeat(
        unit=Weekday.MONDAY, interval=2
    )

    scheduled, log = scheduler.schedule(job_config)
    assert not scheduled
    assert Context.ERROR in log
