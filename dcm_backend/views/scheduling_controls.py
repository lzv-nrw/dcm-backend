"""
This module contains a flask-Blueprint definition for controlling the
scheduling of the dcm-backend.
"""

from typing import Optional
from time import sleep

from flask import Blueprint, jsonify, request, Response
from dcm_common.daemon import Daemon

from dcm_backend.components import Scheduler


def get_scheduling_controls(
    scheduler: Scheduler,
    daemon: Optional[Daemon] = None,
    name: Optional[str] = None,
    default_scheduler_settings: Optional[dict] = None,
    default_daemon_settings: Optional[dict] = None,
) -> Blueprint:
    """
    Returns a blueprint with routes for control over the given
    `Scheduler` and (optionally) an associated `FDaemon` (
    expected to be configured to use `Scheduler.as_thread`
    as factory). The `Daemon` is expected to be configured to use the
    scheduler's default signals.

    Keyword arguments:
    scheduler -- instance of a `Scheduler`
    daemon -- pre-configured `FDaemon` instance
              (default None)
    name -- `Blueprint`'s name
            (default None; uses 'Scheduler Controls')
    default_scheduler_settings -- default set of settings that are
                                  used for running scheduler as
                                  dictionary
                                  (default None)
    default_daemon_settings -- default set of settings that are used for
                               running daemon as dictionary
                               (default None)
    """
    bp = Blueprint(name or "Scheduler Controls", __name__)

    _default_scheduler_settings = default_scheduler_settings or {}
    _default_daemon_settings = default_daemon_settings or {}

    @bp.route("/schedule", methods=["GET"])
    def get():
        """
        Returns status of scheduler, and (if available) daemon.
        """
        return jsonify(
            {
                "scheduler": {
                    "running": scheduler.running,
                    "scheduled": list(scheduler.jobs.keys())
                }
            } | (
                {
                    "daemon": {
                        "active": daemon.active,
                        "status": daemon.status
                    }
                }
                if daemon else {}
            )
        ), 200

    @bp.route("/schedule", methods=["PUT"])
    def put():
        """
        Manually start the scheduling.

        Note that for the given json to take effect, the scheduling
        has to be stopped first. Furthermore, scheduling-settings
        can only be changed when using no `Daemon` at all or a
        `CDaemon`.

        Accepts json to overwrite defaults (all optional):
        {
            "scheduler": {
                "interval": ...,
                "daemon": ...
            },
            "daemon": {
                "interval": ...,
                "daemon": ...
            }
        }
        """
        scheduler_settings = (
            _default_scheduler_settings | request.json.get("scheduler", {})
        )
        daemon_settings = (
            _default_daemon_settings
            | request.json.get("daemon", {})
        )
        if daemon:
            daemon.reconfigure(**scheduler_settings)
            daemon.run(**daemon_settings, block=True)
        else:
            try:
                scheduler.run(**scheduler_settings)
            except RuntimeError:
                return Response(
                    "BUSY (already running)", mimetype="text/plain",
                    status=503
                )
        return Response(
            "OK", mimetype="text/plain", status=200
        )

    @bp.route("/schedule", methods=["DELETE"])
    def delete():
        """Gracefully shut down the scheduler."""
        if daemon:
            daemon.stop(True)
        scheduler.stop()
        while scheduler.running:
            sleep(0.1)
        return Response("OK", mimetype="text/plain", status=200)

    return bp
