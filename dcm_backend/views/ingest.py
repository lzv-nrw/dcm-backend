"""
Ingest View-class definition
"""

from typing import Optional
import json
from uuid import uuid4

from flask import Blueprint, jsonify, Response, request
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from data_plumber_http.settings import Responses
from dcm_common import LoggingContext as Context
from dcm_common.orchestra import JobConfig, JobContext, JobInfo
from dcm_common import services

from dcm_backend import handlers
from dcm_backend.models import (
    Report,
    IngestConfig,
    RosettaTarget,
    ArchiveAPI,
    IngestResult,
    RosettaResult,
)
from dcm_backend.components import archive_controller


class IngestView(services.OrchestratedView):
    """View-class for triggering sip-ingest."""

    NAME = "ingest"

    def register_job_types(self):
        self.config.worker_pool.register_job_type(
            self.NAME, self.ingest, Report
        )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        @bp.route("/ingest", methods=["GET"])
        @flask_handler(  # unknown query
            handler=handlers.get_ingest_handler,
            json=flask_args,
        )
        def get_ingest_status(archive_id: str, deposit_id: str):
            """
            Returns status of ingest associated with
            `archive_id`.
            """

            # TODO: load archive config via archiveID; then create correct
            # archive-controller type with details from configuration
            ac = archive_controller.RosettaAPIClient0(
                auth=self.config.ROSETTA_AUTH_FILE,
                url=self.config.ARCHIVE_API_BASE_URL,
                proxies=self.config.ARCHIVE_API_PROXY,
            )

            ingest = IngestResult(details=RosettaResult())
            # collect deposit
            deposit = ac.get_deposit(deposit_id)
            if not deposit.success:
                return Response(
                    "A problem occurred while fetching deposit-data for id "
                    + f"'{deposit_id}' from archive system: "
                    + deposit.log.fancy(False, flatten=True),
                    mimetype="text/plain",
                    status=502,
                )
            ingest.details.deposit = deposit.data

            # collect sip if possible
            if deposit.data is not None and "sip_id" in deposit.data:
                sip = ac.get_sip(deposit.data["sip_id"])
                if not sip.success:
                    return Response(
                        "A problem occurred while fetching sip-data for id "
                        + f"'{deposit.data['sip_id']}' from archive system: "
                        + sip.log.fancy(False, flatten=True),
                        mimetype="text/plain",
                        status=502,
                    )
                ingest.details.sip = sip.data

            ingest.success = True
            return jsonify(ingest.json), 200

        @bp.route("/ingest", methods=["POST"])
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process ingest
            handler=handlers.post_ingest_handler,
            json=flask_json,
        )
        def ingest(
            ingest: IngestConfig,
            token: Optional[str] = None,
            callback_url: Optional[str] = None,
        ):
            """Submit dir for ingesting in the archive system."""
            try:
                token = self.config.controller.queue_push(
                    token or str(uuid4()),
                    JobInfo(
                        JobConfig(
                            self.NAME,
                            original_body=request.json,
                            request_body={
                                "ingest": ingest.json,
                                "callback_url": callback_url,
                            },
                        ),
                        report=Report(
                            host=request.host_url, args=request.json
                        ),
                    ),
                )
            # pylint: disable=broad-exception-caught
            except Exception as exc_info:
                return Response(
                    f"Submission rejected: {exc_info}",
                    mimetype="text/plain",
                    status=500,
                )

            return jsonify(token.json), 201

        self._register_abort_job(bp, "/ingest")

    def ingest(self, context: JobContext, info: JobInfo):
        """Job instructions for the '/ingest' endpoint."""
        ingest_config = IngestConfig.from_json(
            info.config.request_body["ingest"]
        )
        info.report.log.set_default_origin("Backend")

        # TODO: validate target and initialize correct model
        # get configuration from api

        # TODO: branch into different ingest-methods depending on
        # target-type/archive-configuration

        # archive_config = ArchiveConfiguration.from_row(self.db.select(...))
        # ...
        # match archive_config...:
        #     case ArchiveAPI.ROSETTA_REST:
        #         self.ingest_...(...)

        # ROSETTA_REST - currently only supported archive
        validation = handlers.post_ingest_rosetta_target_handler.run(
            json=ingest_config.target
        )
        if validation.last_status == Responses().GOOD.status:
            self._ingest_rosetta(
                context,
                info,
                ingest_config,
                validation.data.value,
            )
        else:
            self._handle_bad_target(
                context,
                info,
                validation.last_record,
                ArchiveAPI.ROSETTA_REST_V0,
            )

        # make callback; rely on _run_callback to push progress-update
        info.report.progress.complete()
        self._run_callback(
            context, info, info.config.request_body.get("callback_url")
        )

    def _handle_bad_target(
        self,
        context: JobContext,
        info: JobInfo,
        record,
        archive_api: ArchiveAPI,
    ) -> None:
        """Logs an ERROR that describes the problem with the target."""
        info.report.log.log(
            Context.ERROR,
            body=(
                "Ingest failed. Supplied target information incompatible with "
                + f"archive-type '{archive_api.value}': {record.message}."
            ),
        )
        info.report.success = False
        context.push()

    def _ingest_rosetta(
        self, context: JobContext, info: JobInfo, _, target: RosettaTarget
    ):
        # log
        info.report.progress.verbose = (
            f"requesting ingest of '{target.subdirectory}' via "
            + f"'{ArchiveAPI.ROSETTA_REST_V0.value}'-client"
        )
        info.report.log.log(
            Context.EVENT,
            body=(
                f"Attempting ingest of '{target.subdirectory}' using "
                + f"'{ArchiveAPI.ROSETTA_REST_V0.value}'-client."
            ),
        )
        context.push()

        # create archive_controller
        ac = archive_controller.RosettaAPIClient0(
            auth=self.config.ROSETTA_AUTH_FILE,
            url=self.config.ARCHIVE_API_BASE_URL,
            proxies=self.config.ARCHIVE_API_PROXY,
        )

        # run deposit
        deposit = ac.post_deposit(
            # FIXME
            producer=self.config.ROSETTA_PRODUCER,
            material_flow=self.config.ROSETTA_MATERIAL_FLOW,
            **target.json,
        )

        # eval results of first stage (deposit)
        info.report.log.merge(deposit.log)
        info.report.data.success = (
            deposit.success and deposit.data.get("id") is not None
        )
        info.report.data.details = RosettaResult(deposit.data)
        context.push()

        if info.report.data.success:
            info.report.log.log(
                Context.INFO,
                body=(
                    f"Ingest of '{target.subdirectory}' successful. Assigned "
                    + f"deposit id: {deposit.data['id']}."
                ),
            )
        else:
            info.report.log.log(
                Context.INFO,
                body=(
                    f"Ingest of '{target.subdirectory}' not successful. "
                    + f"Archive returned: {json.dumps(deposit.data)}"
                ),
            )
            context.push()
            return

        if deposit.data.get("sip_id") is None:
            # this does not change `success` for now since we have no
            # guarantee from the documentation that the sip is already created
            # at this point
            info.report.log.log(
                Context.INFO,
                body=(
                    "Missing sip-id, unable to collect sip-data for deposit "
                    + f"'{deposit.data['id']}'."
                ),
            )
            context.push()
            return

        # continue with second stage (collect sip information)
        info.report.progress.verbose = (
            f"requesting sip for deposit '{deposit.data['id']}'"
        )
        info.report.log.log(
            Context.EVENT,
            body=(
                f"Attempting to retrieve sip '{deposit.data['sip_id']}' "
                + f"associated with deposit '{deposit.data['id']}'."
            ),
        )
        context.push()

        sip = ac.get_sip(deposit.data["sip_id"])

        # eval results of second stage (sip)
        info.report.log.merge(sip.log)
        info.report.data.details.sip = sip.data
        if sip.data is None:
            info.report.log.log(
                Context.INFO,
                body=(
                    f"No sip collected for deposit '{deposit.data['id']}' and "
                    + f"sip id '{deposit.data['sip_id']}'."
                ),
            )
        context.push()
