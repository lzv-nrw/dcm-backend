"""
Ingest View-class definition
"""

from typing import Optional

from flask import Blueprint, jsonify, Response
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext as Context
from dcm_common.orchestration import JobConfig, Job
from dcm_common import services

from dcm_backend.config import AppConfig
from dcm_backend.handlers import deposit_id_handler, get_ingest_handler
from dcm_backend.models import IngestConfig, Deposit
from dcm_backend.components import ArchiveController


class IngestView(services.OrchestratedView):
    """View-class for triggering sip-ingest."""

    NAME = "ingest"

    def __init__(
        self, config: AppConfig, *args, **kwargs
    ) -> None:
        super().__init__(config, *args, **kwargs)

        # initialize components
        self.archive_controller = ArchiveController(
            auth=self.config.ROSETTA_AUTH_FILE,
            url=self.config.ARCHIVE_API_BASE_URL,
            proxies=self.config.ARCHIVE_API_PROXY
        )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        @bp.route("/ingest", methods=["GET"])
        @flask_handler(  # unknown query
            handler=deposit_id_handler,
            json=flask_args,
        )
        def get_deposit_status(id_: str):
            """
            Returns status of deposit activity associated with `id_`.
            """
            deposit, log = self.archive_controller.get_deposit(id_)
            if deposit is not None:
                return jsonify({"deposit": deposit.json}), 200
            if Context.ERROR in log:
                return Response(
                    f"Problem occurred while fetching data for id '{id_}' from"
                    + f" archive system: {log.fancy(False, flatten=True)}",
                    mimetype="text/plain",
                    status=502
                )
            return Response(
                f"Unknown problem for id '{id_}': empty deposit",
                mimetype="text/plain",
                status=502
            )

        @bp.route("/ingest", methods=["POST"])
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process ingest
            handler=get_ingest_handler(
                default_producer=self.config.ROSETTA_PRODUCER,
                default_material_flow=self.config.ROSETTA_MATERIAL_FLOW
            ),
            json=flask_json,
        )
        def ingest(
            ingest: IngestConfig,
            callback_url: Optional[str] = None
        ):
            """Submit dir for ingesting in the archive system."""
            token = self.orchestrator.submit(
                JobConfig(
                    request_body={
                        "ingest": ingest.json,
                        "callback_url": callback_url
                    },
                    context=self.NAME
                )
            )
            return jsonify(token.json), 201

        self._register_abort_job(bp, "/ingest")

    def get_job(self, config: JobConfig) -> Job:
        return Job(
            cmd=lambda push, data: self.ingest(
                push, data, IngestConfig.from_json(
                    config.request_body["ingest"]
                )
            ),
            hooks={
                "startup": services.default_startup_hook,
                "success": services.default_success_hook,
                "fail": services.default_fail_hook,
                "completion": services.termination_callback_hook_factory(
                    config.request_body.get("callback_url", None),
                )
            },
            name="Backend"
        )

    def ingest(
        self, push, report, ingest_config: IngestConfig
    ):
        """
        Job instructions for the '/ingest' endpoint.

        Orchestration standard-arguments:
        push -- (orchestration-standard) push `report` to host process
        report -- (orchestration-standard) common report-object shared
                  via `push`

        Keyword arguments:
        ingest_config -- an `IngestConfig`-config
        """

        # send post request
        report.progress.verbose = (
            f"requesting ingest of '{ingest_config.rosetta.subdir}' "
            + f"by archive system '{ingest_config.archive_identifier}'"
        )
        report.log.log(
            Context.EVENT,
            body=f"Attempting ingest of '{ingest_config.rosetta.subdir}' "
            + f"in archive system '{ingest_config.archive_identifier}'."
        )
        push()

        _id, ac_log_post = self.archive_controller.post_deposit(
            subdirectory=ingest_config.rosetta.subdir,
            producer=ingest_config.rosetta.producer,
            material_flow=ingest_config.rosetta.material_flow
        )

        report.log.merge(ac_log_post)
        push()

        # eval results
        if _id is not None and Context.ERROR not in ac_log_post:
            report.data.success = True
            report.data.deposit = Deposit(
                id_=_id, status="TRIGGERED"
            )
            report.log.log(
                Context.INFO,
                body="Ingest triggered successfully."
            )
            push()

            # perform a get request to retrieve actual deposit status
            report.progress.verbose = (
                f"requesting status of deposit activity '{_id}'"
            )
            report.log.log(
                Context.EVENT,
                body="Attempting to retrieve actual ingest status of "
                + f"submitted deposit activity with id '{_id}'."
            )
            push()
            _deposit, ac_log_get = self.archive_controller.get_deposit(_id)

            report.log.merge(ac_log_get)
            push()

            if _deposit is not None and Context.ERROR not in ac_log_get:
                # success flag is not affected by any error in the get request
                report.data.deposit = _deposit
                report.log.log(
                    Context.INFO,
                    body="Actual ingest status retrieved successfully."
                )
                push()

            return

        report.data.success = False
        report.log.log(
            Context.ERROR,
            body="Triggering ingest failed."
        )
        push()
