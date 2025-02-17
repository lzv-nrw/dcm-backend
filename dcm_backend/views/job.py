"""
Job View-class definition
"""

from typing import Optional

from flask import Blueprint, jsonify, Response
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common.logger import LoggingContext as Context
from dcm_common.db import KeyValueStoreAdapter
from dcm_common import services
from dcm_common.services.views.interface import View
from dcm_common.orchestration import ChildJob

from dcm_backend.config import AppConfig
from dcm_backend.models import JobConfig
from dcm_backend.components import Scheduler, JobProcessorAdapter
from dcm_backend import handlers


class JobView(View):
    """
    View-class for managing job-execution.

    Keyword arguments:
    config -- `AppConfig`-object
    config_db -- adapter for job configuration-database
    report_db -- adapter for job report-database
    scheduler -- `Scheduler`-object
    adapter -- `JobProcessorAdapter`-object
    """

    NAME = "job"

    def __init__(
        self,
        config: AppConfig,
        config_db: KeyValueStoreAdapter,
        report_db: KeyValueStoreAdapter,
        scheduler: Scheduler,
        adapter: JobProcessorAdapter,
    ) -> None:
        super().__init__(config)
        self.config_db = config_db
        self.report_db = report_db
        self.scheduler = scheduler
        self.adapter = adapter
        self.processor_abort_url = self.config.JOB_PROCESSOR_HOST + "/process"

    def _get_info(self, bp: Blueprint):
        @bp.route(
            "/job", methods=["GET"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=services.handlers.report_handler,
            json=flask_args,
        )
        def get_info(token: str):
            """Fetch job info associated with given `token`."""
            info = self.report_db.read(token)
            if info:
                return jsonify(
                    {
                        "report": info["report"],
                        "token": info["token"],
                        "metadata": info["metadata"]
                    }
                ), 200
            return Response(
                f"Unknown job '{token}'.", mimetype="text/plain", status=404
            )

    def _post_job(self, bp: Blueprint):
        @bp.route(
            "/job", methods=["POST"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_json,
        )
        def post_job(id_: str):
            """Start job associated with config `id_`."""
            # get config
            config_json = self.config_db.read(id_)
            if config_json is None:
                return Response(
                    f"Unknown config '{id_}'.",
                    mimetype="text/plain",
                    status=404
                )
            config = JobConfig.from_json(config_json)

            # submit
            token = self.adapter.submit(
                None,
                {"process": config.job, "id": id_},
                info := services.APIResult()
            )
            if token is None:
                return Response(
                    "Error during submission: "
                    + "; ".join(
                        map(
                            lambda x: x["body"],
                            info.report.get(
                                "log", {}
                            ).get(
                                Context.ERROR.name, []
                            )
                        )
                    ),
                    mimetype="text/plain",
                    status=502
                )

            # poll until completed
            # <this can be used to also perform tasks after job is done>
            # t = threading.Thread(
            #     target=self.adapter.poll,
            #     args=(token.value, info := services.APIResult()),
            #     daemon=True
            # )
            # t.start()

            if info.report is None:
                return Response(
                    "Unknown error during submission.",
                    mimetype="text/plain",
                    status=500
                )

            return jsonify(token.to_dict()), 201

    def _list_jobs(self, bp: Blueprint):
        @bp.route(
            "/job", methods=["OPTIONS"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(False, ["status"]),
            json=flask_args,
        )
        @flask_handler(  # process query
            handler=handlers.job_status_filter_handler,
            json=flask_args,
        )
        def list_jobs(
            id_: Optional[str] = None, status: Optional[list[str]] = None
        ):
            """Collect (filtered) list of jobs."""
            _status = status or []
            result = []
            if "scheduled" in _status:
                for config_id in self.scheduler.jobs.keys():
                    if id_ is None or config_id == id_:
                        result.append({"status": "scheduled", "id": config_id})
            if any(
                x in _status
                for x in ["queued", "running", "completed", "aborted"]
            ):
                for token in self.report_db.keys():
                    info = self.report_db.read(token)
                    this_status = info.get(
                        "report", {}
                    ).get(
                        "progress", {}
                    ).get("status", "unknown")
                    config_id = info.get(
                        "config", {}
                    ).get(
                        "original_body", {}
                    ).get("id")
                    if (
                        this_status in _status
                        and (id_ is None or config_id == id_)
                    ):
                        result.append(
                            {
                                "status": this_status,
                                "token": info.get("token")
                            } | (
                                {} if config_id is None else {"id": config_id}
                            )
                        )

            return jsonify(result), 200

    def _delete_job(self, bp: Blueprint):
        @bp.route(
            "/job", methods=["DELETE"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=services.handlers.report_handler,
            json=flask_args,
        )
        @flask_handler(
            handler=services.abort_body_handler,
            json=flask_json,
        )
        def delete_job(
            token: str,
            origin: Optional[str] = None,
            reason: Optional[str] = None
        ):
            """Stop job associated with `token`."""
            ok, msg = ChildJob(
                self.processor_abort_url, token=token
            ).abort(origin=origin, reason=reason)
            if not ok:
                return Response(
                    "Error while aborting child at "
                    + f"'{self.processor_abort_url}' using token '{token}': "
                    + msg,
                    mimetype="text/plain", status=502
                )
            return Response(
                f"successfully aborted '{token}'",
                mimetype="text/plain",
                status=200
            )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        self._get_info(bp)
        self._post_job(bp)
        self._list_jobs(bp)
        self._delete_job(bp)
