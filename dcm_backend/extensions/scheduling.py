"""Scheduling startup-extension."""

import atexit

from dcm_common.daemon import FDaemon
from dcm_common.services.extensions.common import startup_flask_run


def scheduling(app, config, scheduler, as_process) -> FDaemon:
    """
    Register the `scheduling` extension.

    If `as_process`, the daemon-startup call is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    daemon is executed directly, i.e., in the same process from which
    this process has been called.
    """
    daemon = FDaemon(
        scheduler.as_thread, kwargs={
            "interval": config.SCHEDULING_INTERVAL
        }
    )
    if config.SCHEDULING_AT_STARTUP:
        if as_process:
            # app in separate process via app.run
            startup_flask_run(
                app, (
                    lambda: daemon.run(config.SCHEDULING_DAEMON_INTERVAL),
                )
            )
        else:
            # app native execution
            daemon.run(config.SCHEDULING_DAEMON_INTERVAL)

    # perform clean shutdown on exit
    atexit.register(
        lambda block: (
            daemon.stop(block=block),
            scheduler.stop(),
        ),
        block=True
    )

    return daemon
