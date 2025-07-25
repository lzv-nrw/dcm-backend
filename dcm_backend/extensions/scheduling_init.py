"""Scheduling initialization-extension."""

from typing import Optional, Iterable
from threading import Thread, Event
import signal
from datetime import datetime

from dcm_common.services.extensions.common import (
    print_status,
    startup_flask_run,
    add_signal_handler,
    ExtensionLoaderResult,
    _ExtensionRequirement,
)

from dcm_backend.models import JobConfig, JobInfo


def _scheduling_init(config, scheduler, db, abort, result, requirements):
    first_try = True
    while not _ExtensionRequirement.check_requirements(
        requirements,
        "Initializing scheduler delayed until '{}' is ready.",
    ):
        first_try = False
        abort.wait(config.SCHEDULER_INIT_STARTUP_INTERVAL)
        if abort.is_set():
            return

    for job_config in db.get_rows("job_configs").eval(
        "scheduler initialization"
    ):
        config = JobConfig.from_row(job_config)
        # load previous execution
        if config.latest_exec is not None:
            info = JobInfo.from_row(
                db.get_row(
                    "jobs",
                    config.latest_exec,
                    cols=["token", "datetime_started"],
                ).eval("scheduler initialization")
                or {"token": config.latest_exec}
            )
        else:
            info = None
        plan = scheduler.schedule(
            config,
            (
                None
                if info is None or info.datetime_started is None
                else datetime.fromisoformat(info.datetime_started)
            ),
        )
        if plan is not None:
            print_status(f"Scheduled job '{job_config['id']}' at '{plan.at}'.")

    if not first_try:
        print_status("Scheduler initialized.")

    result.ready.set()


def scheduling_init_loader(
    app,
    config,
    scheduler,
    db,
    as_process,
    requirements: Optional[Iterable[_ExtensionRequirement]] = None,
) -> ExtensionLoaderResult:
    """
    Register the scheduling initialization extension, which loads
    existing job configurations into the scheduler

    If `as_process`, the call to `init` is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    function is executed directly, i.e., in the same process from which
    this process has been called.
    """
    abort = Event()
    result = ExtensionLoaderResult()
    thread = Thread(
        target=_scheduling_init,
        args=(config, scheduler, db, abort, result, requirements or []),
    )
    result.data = thread
    if config.SCHEDULING_AT_STARTUP:
        if as_process:
            # app in separate process via app.run
            startup_flask_run(app, (thread.start,))
        else:
            # app native execution
            thread.start()

    # perform clean shutdown on exit
    def _exit():
        """Terminate connections."""
        abort.set()

    add_signal_handler(signal.SIGINT, _exit)
    add_signal_handler(signal.SIGTERM, _exit)

    return result
