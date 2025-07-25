"""Scheduling startup-extension."""

import signal

from dcm_common.services.extensions.common import (
    add_signal_handler,
    ExtensionLoaderResult,
)


def scheduling_loader(scheduler) -> ExtensionLoaderResult:
    """
    Register the `scheduling` extension.

    Only required for a clean shutdown.
    """
    # perform clean shutdown on exit
    def _exit():
        """Clear all scheduled jobs."""
        scheduler.clear()

    add_signal_handler(signal.SIGINT, _exit)
    add_signal_handler(signal.SIGTERM, _exit)

    return ExtensionLoaderResult().toggle()
