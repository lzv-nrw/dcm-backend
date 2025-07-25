"""
This module contains a flask-Blueprint definition for controlling the
scheduling of the dcm-backend.
"""

from typing import Optional

from flask import Blueprint, jsonify, Response

from dcm_backend.components import Scheduler


def get_scheduling_controls(
    scheduler: Scheduler,
    name: Optional[str] = None,
) -> Blueprint:
    """
    Returns a blueprint with routes for control over the given
    `scheduler`.
    """
    bp = Blueprint(name or "Scheduler Controls", __name__)

    @bp.route("/schedule", methods=["GET"])
    def get():
        """Returns status of scheduler."""
        return (
            jsonify(
                settings={
                    "zoneinfo": str(scheduler.zoneinfo),
                },
                scheduled=[
                    {
                        "id": plan.id_,
                        "jobConfig": plan.config.id_,
                        "at": plan.at.isoformat(),
                    }
                    for plan in scheduler.get_plans()
                ],
            ),
            200,
        )

    @bp.route("/schedule", methods=["DELETE"])
    def delete():
        """Gracefully shut down the scheduler."""
        scheduler.clear(True)
        return Response("OK", mimetype="text/plain", status=200)

    return bp
