"""
This module defines the `Scheduler` component of the dcm-backend-app.
"""

from typing import Optional, Callable
import sys
from uuid import uuid4
from dataclasses import dataclass
from datetime import datetime, timedelta
import zoneinfo as zoneinfo_
import threading
import abc
import traceback

import dateutil

from dcm_backend.models import Schedule, TimeUnit


class Timeout:
    """
    Thread-based timeout-implementation.

    Keyword arguments:
    timeout -- timeout duration
    on_timeout -- callback on regular timeout
    on_success -- callback on successful timeout
                  (default None)
    on_cancel -- callback on canceled timeout
                 (default None)
    on_completion -- callback before termination (after either timeout
                     or cancel)
                     (default None)
    on_error -- callback on error during other callback execution
                (default None)
    """

    def __init__(
        self,
        timeout: float,
        on_timeout: Callable[[], None],
        on_success: Optional[Callable[[], None]] = None,
        on_cancel: Optional[Callable[[], None]] = None,
        on_completion: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> None:
        self._timeout = timeout

        self._on_timeout = on_timeout
        self._on_success = on_success or (lambda: None)
        self._on_cancel = on_cancel or (lambda: None)
        self._on_completion = on_completion or (lambda: None)
        self._on_error = on_error or (lambda e: None)

        self._start_event = threading.Event()
        self._timeout_event = threading.Event()
        self._cancel_event = threading.Event()

        self._t = threading.Thread(target=self._target)

    def _target(self) -> Callable[[], None]:
        """
        Thread-target that handles waiting until timing out and
        running callbacks given to the `Timeout`-constructor.
        """
        self._start_event.set()
        try:
            if self._cancel_event.wait(self._timeout):
                self._on_cancel()
            else:
                self._on_timeout()
                self._on_success()
            self._on_completion()
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            self._on_error(exc_info)
        self._timeout_event.set()

    def start(self) -> None:
        """Starts the timeout."""
        if self._start_event.is_set():
            raise RuntimeError("Cannot re-use Timeout objects.")
        self._t.start()
        if not self._start_event.wait(5):
            raise RuntimeError("Timeout did not start.")

    def cancel(
        self, wait: bool = False, timeout: Optional[float] = None
    ) -> None:
        """
        Cancels the timeout, will allow completion-callback to run. If
        `wait`, halts until all callbacks are completed or `timeout` is
        exceeded.
        """
        self._cancel_event.set()
        if not self._start_event.is_set():
            return
        if wait:
            self.wait(timeout)

    @property
    def timeout(self) -> float:
        """Returns total (initial) duration for this timeout."""
        return self._timeout

    @property
    def running(self) -> bool:
        """Returns `True` if the timeout is running."""
        return self._t.is_alive()

    @property
    def canceled(self) -> bool:
        """
        Returns `True` if the timeout has been canceled or stopped.
        """
        return self._cancel_event.is_set()

    def wait(self, timeout: Optional[float] = None) -> None:
        """
        Halts execution until timeout-callback is completed or `timeout`
        is exceeded.
        """
        self._timeout_event.wait(timeout)


class ScheduledJobConfig(metaclass=abc.ABCMeta):
    """Minimal interface for scheduler-supported job configurations."""

    id_: str
    schedule: Optional[Schedule]


@dataclass
class ExecutionPlan:
    """Record class for planned executions of `ScheduledJobConfig`."""

    id_: str
    config: ScheduledJobConfig
    at: datetime
    timeout: Timeout


class Scheduler:
    """
    Scheduling planner.

    Jobs can be scheduled using an implementation of the
    `ScheduledJobConfig`-interface. This class implements both onetime
    and continuous scheduling (based on `ScheduledJobConfig.schedule`).

    Keyword arguments:
    factory -- factory for creation of jobs in the form of callable
               functions
    zoneinfo -- timezone identifier; allowed values are given by
                `zoneinfo.available_timezones()`
                (default None; uses system default)
    """

    def __init__(
        self,
        factory: Callable[[ScheduledJobConfig], Callable[[], None]],
        zoneinfo: Optional[str] = None,
    ) -> None:
        self._factory = factory
        if zoneinfo is None:
            self.zoneinfo = datetime.now().astimezone().tzinfo
        else:
            if zoneinfo not in zoneinfo_.available_timezones():
                raise ValueError(
                    f"Unknown timezone '{zoneinfo}'. Run 'import zoneinfo; "
                    + "zoneinfo.available_timezones()' for a set of options."
                )
            self.zoneinfo = zoneinfo_.ZoneInfo(zoneinfo)

        self._plans: dict[str, ExecutionPlan] = {
            # mapping of ExecutionPlan-id and associated ExecutionPlan-object
        }
        self._plans_lock = threading.RLock()

    def _should_be_scheduled(self, schedule: Optional[Schedule]) -> bool:
        """Returns `True` if the input is relevant for scheduling."""
        if schedule is None:
            return False
        if schedule.active and schedule.start is None:
            raise ValueError("Unable to schedule job without start-date.")
        if schedule.end is not None:
            # skip jobs with end-date in the past
            if self._make_tz_aware(schedule.end).astimezone(
                self.zoneinfo
            ) < datetime.now(self.zoneinfo):
                return False
        return schedule.active

    @staticmethod
    def _datetime_tz_aware(d: datetime) -> bool:
        """Returns `True` if `d` is timezone-aware."""
        # see https://stackoverflow.com/a/27596917
        return d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None

    def _make_tz_aware(self, d: datetime, tzinfo=None) -> datetime:
        """Returns timezone-aware datetime."""
        if self._datetime_tz_aware(d):
            return d
        return d.astimezone(tzinfo or self.zoneinfo)

    def _plan_onetime(
        self, config: ScheduledJobConfig, previous: Optional[datetime]
    ) -> Optional[datetime]:
        """
        Returns datetime of next execution for onetime-scheduling.
        """
        if previous is not None:
            # ran already
            return None
        return self._make_tz_aware(config.schedule.start)

    def _plan_x(
        self,
        config: ScheduledJobConfig,
        previous: Optional[datetime],
        unit: str,
        factor: int,
        report_deviation: bool = False,
    ) -> Optional[datetime]:
        """
        Returns datetime of next execution for x-scheduling. `unit` is
        the keyword-name for a call to `datetime.timedelta` and factor
        is the conversion factor from seconds to that unit.
        """
        if previous is None:
            return self._make_tz_aware(config.schedule.start)

        # convert to timezone-aware datetimes (with timezone of start)
        _start = self._make_tz_aware(config.schedule.start)
        _previous = self._make_tz_aware(previous, _start.tzinfo)

        # get timedelta-factor of previous iteration
        # then return the next in line
        previous_iteration = (
            (_previous - _start).total_seconds()
            / config.schedule.repeat.interval
            / factor
        )
        if report_deviation and 0.33 < previous_iteration % 1 < 0.66:
            print(
                "Large relative deviation from schedule in previous execution "
                + f"of job '{config.id_}' detected (fraction "
                + f"{round(previous_iteration % 1, 2)} {unit}).",
                file=sys.stderr,
            )
        return _start + timedelta(
            **{
                unit: round(
                    previous_iteration + config.schedule.repeat.interval
                )
            }
        )

    def _plan_monthly(
        self, config: ScheduledJobConfig, previous: Optional[datetime]
    ) -> Optional[datetime]:
        """
        Returns datetime of next execution for monthly-scheduling.
        """
        if previous is None:
            return self._make_tz_aware(config.schedule.start)

        # convert to timezone-aware datetimes (with timezone of start)
        _start = self._make_tz_aware(config.schedule.start)
        _previous = self._make_tz_aware(previous, _start.tzinfo)

        # get timedelta-factor of previous iteration
        # then return the next in line
        previous_iteration = (
            (_previous - _start).total_seconds()
            / config.schedule.repeat.interval
            / 604800
            / 4.345
        )
        if 0.33 < previous_iteration % 1 < 0.66:
            print(
                "Large relative deviation from schedule in previous execution "
                + f"of job '{config.id_}' detected (fraction "
                + f"{round(previous_iteration % 1, 2)} months).",
                file=sys.stderr,
            )
        return datetime.combine(
            (
                _start.date()
                + dateutil.relativedelta.relativedelta(
                    months=round(
                        previous_iteration + config.schedule.repeat.interval
                    )
                )
            ),
            _start.time(),
            _start.tzinfo,
        )

    def plan(
        self, config: ScheduledJobConfig, previous: Optional[datetime]
    ) -> Optional[datetime]:
        """
        Returns datetime of next execution for `config` based on
        `previous` execution.
        """
        if not self._should_be_scheduled(config.schedule):
            return None

        if config.schedule.repeat is None:
            # active without repeat-info
            return self._plan_onetime(config, previous)

        match config.schedule.repeat.unit:
            case TimeUnit.SECOND:
                return self._plan_x(config, previous, "seconds", 1, False)
            case TimeUnit.MINUTE:
                return self._plan_x(config, previous, "minutes", 60, False)
            case TimeUnit.HOUR:
                return self._plan_x(config, previous, "hours", 3600, True)
            case TimeUnit.DAY:
                return self._plan_x(config, previous, "days", 86400, True)
            case TimeUnit.WEEK:
                return self._plan_x(config, previous, "weeks", 604800, True)
            case TimeUnit.MONTH:
                return self._plan_monthly(config, previous)

        raise NotImplementedError(
            f"Unable to plan schedule for job-configuration '{config.id_}'."
            f"(Job configuration: {config}; previous: {previous})"
        )

    def _dispatch(
        self,
        config: ScheduledJobConfig,
        at: datetime,
        re_schedule: bool = False,
    ) -> ExecutionPlan:
        """
        Creates `Timeout` for the given `config` and returns
        `ExecutionPlan`. If `re_schedule`, re-schedules this job on
        completion of timeout-callback. The plan is configured for the
        local timezone.
        """
        # work in default-timezone
        _at = self._make_tz_aware(at).astimezone(self.zoneinfo)

        # configure re-schedule callback
        if re_schedule:
            # `at` may lie in the past, this enables proper re-scheduling
            _re_schedule_at = max(_at, datetime.now(self.zoneinfo))

            def on_success():
                self.schedule(config, _re_schedule_at)
        else:
            on_success = None

        id_ = str(uuid4())
        plan = ExecutionPlan(
            id_=id_,
            config=config,
            at=_at,
            timeout=Timeout(
                timeout=max(
                    0, (_at - datetime.now(self.zoneinfo)).total_seconds()
                ),
                on_timeout=self._factory(config),
                on_success=on_success,
                # remove record from planned jobs
                on_completion=lambda: self.clear_plan(id_),
                on_error=lambda e: print(
                    "Error while running scheduled job for configuration "
                    + f"'{config.id_}': {traceback.format_exc()}",
                    file=sys.stderr,
                ),
            ),
        )
        with self._plans_lock:
            self._plans[id_] = plan
        plan.timeout.start()
        return plan

    def get_plans(self, id_: Optional[str] = None) -> list[ExecutionPlan]:
        """
        Returns all registered `ExecutionPlan`s. If a
        `ScheduledJobConfig.id_` is given, filter for associated plans.
        """
        if id_ is None:
            return list(self._plans.values())
        return [p for p in self._plans.values() if p.config.id_ == id_]

    def clear_jobs(
        self, id_: str, wait: bool = False, timeout: Optional[float] = None
    ) -> None:
        """
        Cancels all `ExecutionPlan`s for a given `ScheduledJobConfig.id_`
        and cleans up internal records. Both `wait` and `timeout` are
        passed on to the individual `Timeout.cancel`-calls.
        """
        while (plans := self.get_plans(id_)):
            for plan in plans:
                plan.timeout.cancel(wait, timeout)
                with self._plans_lock:
                    try:
                        del self._plans[plan.id_]
                    except KeyError:
                        pass

    def clear_plan(
        self, id_: str, wait: bool = False, timeout: Optional[float] = None
    ) -> None:
        """
        Cancels given `ExecutionPlan` by its `id_` and cleans up
        internal records. Both `wait` and `timeout` are passed on to the
        `Timeout.cancel`-method. Already canceled plans are skipped
        (they are expected to clean up after themselves).
        """
        plan = self._plans.get(id_)
        if plan is None:
            return
        if not plan.timeout.canceled:
            plan.timeout.cancel(wait, timeout)
            with self._plans_lock:
                del self._plans[id_]

    def clear(
        self, wait: bool = False, timeout: Optional[float] = None
    ) -> None:
        """
        Cancel all running `Timeout`s. Both `wait` and `timeout` are
        passed on to the individual `Timeout.cancel`-calls.
        """
        while self._plans:
            # use list-constructor to make a copy
            plans = list(self._plans.values())
            for plan in plans:
                plan.timeout.cancel(wait, timeout)

                with self._plans_lock:
                    try:
                        del self._plans[plan.id_]
                    except KeyError:
                        pass

    def schedule_at(
        self, config: ScheduledJobConfig, at: Optional[datetime] = None
    ) -> Optional[ExecutionPlan]:
        """
        Schedule execution of a given `config` at a given datetime
        ignoring any scheduling-info. If `at` is None, run immediately.
        """
        return self._dispatch(config, at or datetime.now(), False)

    def schedule(
        self, config: ScheduledJobConfig, previous: Optional[datetime] = None
    ) -> Optional[ExecutionPlan]:
        """Schedule execution of a given `config`."""
        at = self.plan(config, previous)
        if at is None:
            return None

        return self._dispatch(config, at, True)
