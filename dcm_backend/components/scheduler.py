"""
This module defines the `Scheduler` component of the dcm-backend-app.
"""

from typing import Optional, Callable
from time import sleep
import threading

import schedule as schedule_lib
from dcm_common import Logger, LoggingContext as Context
from dcm_common.util import now

from dcm_backend.models import JobConfig, TimeUnit, Weekday


class Scheduler:
    """
    A `Scheduler` is used to schedule jobs according to a schedule rule set.

    Keyword arguments:
    job_cmd -- the job to be executed
    default_unit -- default value for the time unit of the repetition
                    (default TimeUnit.DAY)
    default_interval -- default value for the interval of the repetition
                        (default 1)
    """
    _TAG: str = "Scheduler"

    def __init__(
        self,
        job_cmd: Callable,
        default_unit: Optional[TimeUnit | Weekday] = TimeUnit.DAY,
        default_interval: Optional[int] = 1
    ) -> None:
        self.job_cmd = job_cmd
        self.default_unit = default_unit
        self.default_interval = default_interval
        self._scheduler = schedule_lib.Scheduler()

        # mapping of job-config-id and scheduler-job
        self.jobs: dict[str, schedule_lib.Job] = {}

        # threading-related features
        self._jobs_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    @property
    def running(self) -> bool:
        """
        Returns `True` if a `Thread` associated with this instance is
        currently running.
        """
        return self._thread is not None and self._thread.is_alive()

    def schedule(
        self,
        config: JobConfig
    ) -> tuple[bool, Logger]:
        """
        Schedules a job according to its configuration.
        Returns a tuple with a boolean whether job was scheduled successfully
        and a `Logger` object.

        Keyword arguments:
        config -- configuration for the job that has to be scheduled
        """

        _log = Logger(default_origin=self._TAG)

        # If the job is already scheduled, cancel previous job
        # and continue to register the updated job.
        if config.id_ in self.jobs:
            self.cancel(config.id_)
            _log.log(
                Context.INFO,
                body=f"Updating scheduled job '{config.id_}'"
            )

        # check if conditions for scheduling job are fulfilled
        if config.schedule is None:
            _log.log(
                Context.INFO,
                body=f"Job '{config.id_}' contains no scheduling rule set."
            )
            return False, _log
        if not config.schedule.active:
            _log.log(
                Context.INFO,
                body=f"Job '{config.id_}' is paused."
            )
            return False, _log

        def _job() -> None:
            """
            Performs checks for start and end time, and executes job_cmd.
            """

            assert config.schedule is not None  # mypy-hint

            # if start time is in the future, do nothing
            if (
                config.schedule.start is not None
                and config.schedule.start > now()
            ):
                return
            # if end time is in the past, cancel job
            if (
                config.schedule.end is not None
                and config.schedule.end < now()
            ):
                self.cancel(config.id_)
                return
            # execute job_cmd
            self.job_cmd(config=config)

        if config.schedule.repeat is None:
            _unit = self.default_unit
            _interval = self.default_interval
        else:
            _unit = config.schedule.repeat.unit
            _interval = config.schedule.repeat.interval

        # Set the interval for repeating the job
        assert isinstance(_interval, int)  # mypy-hint
        _schedule_every_x = self._scheduler.every(_interval)

        if isinstance(_unit, Weekday):
            if _interval != 1:
                _log.log(
                    Context.ERROR,
                    body=(
                        f"Job '{config.id_}' cannot be scheduled. Scheduling "
                        + "on a specific day is only allowed for weekly jobs. "
                        + "No support for interval greater than 1."
                    )
                )
                return False, _log
            # Run job on the day defined in unit at 00:00:00
            job = getattr(
                _schedule_every_x, _unit.value
            ).at("00:00:00").do(_job)

        else:
            match _unit:

                case TimeUnit.SECOND:
                    # Run job every X seconds
                    job = _schedule_every_x.seconds.do(_job)

                case TimeUnit.MINUTE:
                    # Run job every X minutes at the 0th second
                    job = _schedule_every_x.minutes.at(":00").do(_job)

                case TimeUnit.HOUR:
                    # Run job every X hours at 00:00
                    job = _schedule_every_x.hours.at("00:00").do(_job)

                case TimeUnit.DAY:
                    # Run job every X days at 00:00:00
                    job = _schedule_every_x.days.at("00:00:00").do(_job)

                case TimeUnit.WEEK:
                    # Run job every X weeks
                    # FIXME can I set the exact day and time of execution?
                    job = _schedule_every_x.weeks.do(_job)

        with self._jobs_lock:
            self.jobs[config.id_] = job
        return True, _log

    def cancel(self, config_id: str) -> None:
        """
        Cancels a scheduled job from the Scheduler.

        Keyword arguments:
        config_id -- id of the configuration for the job
                     that has to be canceled
        """
        if config_id in self.jobs:
            with self._jobs_lock:
                self._scheduler.cancel_job(self.jobs[config_id])
                del self.jobs[config_id]

    def get_jobs(self) -> list[schedule_lib.Job]:
        """ Get scheduled jobs. """
        return self._scheduler.get_jobs()

    def run_pending(self) -> None:
        """ Run all jobs that are scheduled to run. """
        if self.running:
            raise RuntimeError(
                "Tried to manually run pending jobs while Scheduler-thread is "
                + "already running."
            )
        return self._run_pending()

    def _run_pending(self) -> None:
        """ Run all jobs that are scheduled to run. """
        return self._scheduler.run_pending()

    def stop(self):
        """Stop a running scheduling-loop."""
        self._stop.set()

    def _run(self, interval):
        """Run a scheduling-loop."""
        while not self._stop.is_set():
            self._run_pending()
            sleep(interval)
        self._stop.clear()

    def as_thread(
        self,
        interval: float = 1.0,
        daemon: bool = False,
    ) -> threading.Thread:
        """
        Returns `Thread` that, when executed, enters a scheduling loop.

        Note that only a single instance of the scheduling-loop
        should run at a given time. Hence, this method raises a
        `RuntimeError` if a new thread is requested while a previous
        thread is still running.

        Keyword arguments:
        interval -- interval with which scheduling is checked
                    (default 1.0)
        daemon -- whether to run as daemon
                  (default False)
        """
        if self.running:
            raise RuntimeError(
                "Tried to create Scheduler-thread while already running."
            )
        # reset and configure
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            args=(interval,),
            daemon=daemon
        )
        return self._thread
