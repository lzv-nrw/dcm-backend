"""
Job View-class definition
"""

from typing import Optional
import sys
from uuid import UUID

from flask import Blueprint, jsonify, Response
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common.logger import LoggingContext as Context
from dcm_common.db import SQLAdapter
from dcm_common.db.sql.adapter.interface import _Statement
from dcm_common import util, services
from dcm_common.services.views.interface import View

from dcm_backend.config import AppConfig
from dcm_backend.models import JobConfig, TriggerType, Record, JobInfo
from dcm_backend.components import Scheduler, JobProcessorAdapter
from dcm_backend import handlers


class JobView(View):
    """
    View-class for managing job-execution.

    Keyword arguments:
    config -- `AppConfig`-object
    db -- database adapter
    scheduler -- `Scheduler`-object
    adapter -- `JobProcessorAdapter`-object
    """

    NAME = "job"

    def __init__(
        self,
        config: AppConfig,
        db: SQLAdapter,
        scheduler: Scheduler,
        adapter: JobProcessorAdapter,
    ) -> None:
        super().__init__(config)
        self.db = db
        self.scheduler = scheduler
        self.adapter = adapter
        self.processor_abort_url = self.config.JOB_PROCESSOR_HOST + "/process"

    def _get_info(self, bp: Blueprint):
        @bp.route(
            "/job", methods=["GET"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_job_handler,
            json=flask_args,
        )
        def get_info(token: str, keys: Optional[str] = None):
            """Fetch job info associated with given `token`."""
            # validate token format
            try:
                UUID(token)
            except ValueError as exc_info:
                print(
                    f"Filtered dangerous value '{token}' while building "
                    + f"get-info query via 'token': {exc_info}",
                    file=sys.stderr,
                )
                return Response(
                    "FAILED", mimetype="text/plain", status=422
                )
            # query for token and requested cols
            query = self.db.get_row(
                "jobs",
                token,
                (
                    None
                    if keys is None
                    else (
                        # token is always included
                        ["token"]
                        # job_config_id is potentially needed to fetch
                        # template-/workspace-info
                        + (
                            ["job_config_id"]
                            if "jobConfigId" in keys
                            or "workspaceId" in keys
                            or "templateId" in keys
                            else []
                        )
                        # others can be iterated
                        + [
                            col
                            for col, key in [
                                ("status", "status"),
                                ("success", "success"),
                                ("user_triggered", "userTriggered"),
                                ("datetime_triggered", "datetimeTriggered"),
                                ("trigger_type", "triggerType"),
                                ("datetime_started", "datetimeStarted"),
                                ("datetime_ended", "datetimeEnded"),
                                ("report", "report"),
                            ]
                            if key in keys
                        ]
                    )
                ),
            ).eval()

            if query is None:
                return Response(
                    f"Unknown job '{token}'.",
                    mimetype="text/plain",
                    status=404,
                )

            info = JobInfo.from_row(query)

            # collect additional (optional) data
            # * relations (only if not a test-job)
            if info.job_config_id is not None:
                if (
                    keys is None
                    or "workspaceId" in keys
                    or "templateId" in keys
                ):
                    info.template_id = self.db.get_row(
                        "job_configs",
                        info.job_config_id,
                        ["id", "template_id"],
                    ).eval()["template_id"]
                if keys is None or "workspaceId" in keys:
                    info.workspace_id = self.db.get_row(
                        "templates",
                        info.template_id,
                        ["id", "workspace_id"],
                    ).eval()["workspace_id"]
            # * records
            if keys is None or "records" in keys:
                info.records = [
                    Record.from_row(row)
                    for row in self.db.get_rows(
                        "records",
                        info.token,
                        "job_token",
                        [
                            "report_id",
                            "success",
                            "external_id",
                            "origin_system_id",
                            "sip_id",
                            "ie_id",
                            "datetime_processed",
                        ],
                    ).eval()
                ]
            return (
                jsonify(
                    {
                        k: v
                        for k, v in info.json.items()
                        if keys is None or k in (["token"] + keys.split(","))
                    }
                ),
                200,
            )

    def _post_job(self, bp: Blueprint):

        @bp.route("/job", methods=["POST"], provide_automatic_options=False)
        @flask_handler(  # process query
            handler=handlers.post_job_handler,
            json=flask_json,
        )
        def post_job(
            id_: str,
            token: Optional[str] = None,
            user_triggered: Optional[str] = None,
        ):
            """Start job associated with config `id_`."""
            # get job config
            query = self.db.get_row("job_configs", id_).eval()
            if query is None:
                return Response(
                    f"Unknown job config '{id_}'.",
                    mimetype="text/plain",
                    status=404
                )
            config = JobConfig.from_row(query)

            # get user-config if needed
            if user_triggered is not None:
                if not self.db.get_row("user_configs", user_triggered).success:
                    return Response(
                        f"Unknown user '{user_triggered}'.",
                        mimetype="text/plain",
                        status=404
                    )

            # submit
            token = self.adapter.submit(
                None,
                self.adapter.build_request_body(
                    job_config=config,
                    base_request_body={
                        "context": {
                            "jobConfigId": id_,
                            "userTriggered": user_triggered,
                            "datetimeTriggered": util.now().isoformat(),
                            "triggerType": TriggerType.MANUAL.value,
                        }
                        | ({} if token is None else {"token": token})
                    },
                ),
                info := services.APIResult(),
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

            # update config in database
            self.db.update(
                "job_configs",
                {"id": config.id_, "latest_exec": token.value},
            )

            return jsonify(token.to_dict()), 201

    def _post_test_job(self, bp: Blueprint):
        @bp.route(
            "/job-test",
            methods=["POST"],
            provide_automatic_options=False,
        )
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.get_job_config_handler(False),
            json=flask_json,
        )
        def post_test_job(config: JobConfig):
            """Run test-job for given `config`."""
            # submit
            token = self.adapter.submit(
                None,
                self.adapter.build_request_body(
                    job_config=config,
                    base_request_body={
                        "context": {
                            "datetimeTriggered": util.now().isoformat(),
                            "triggerType": TriggerType.TEST.value,
                        }
                    },
                    test_mode=True,
                ),
                info := services.APIResult(),
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
            handler=handlers.list_jobs_handler,
            json=flask_args,
        )
        def list_jobs(
            id_: Optional[str] = None,
            status: Optional[str] = None,
            from_: Optional[str] = None,
            to: Optional[str] = None,
            success: Optional[str] = None,
        ):
            """Collect (filtered) list of job tokens."""
            # this method uses a custom command via the database adapter
            # which needs to be done very cautiously
            # both the handler and the implementation should ensure a
            # safe use
            # as additional precaution only generic messages are
            # returned if a problem is encountered
            base_statement = self.db.get_select_statement(
                "jobs", cols=["token"]
            )

            filters = []
            if id_ is not None:
                # safeguard by properly escaping input
                try:
                    filters.append(self.db.get_select_statement(
                        "jobs", id_, "job_config_id"
                    ).value.split("WHERE ")[1])
                # pylint: disable=broad-exception-caught
                except Exception as exc_info:
                    print(
                        "Failed to build job-query filter for 'id' "
                        + f"unexpectedly: {exc_info}",
                        file=sys.stderr,
                    )
                    return Response(
                        "FAILED", mimetype="text/plain", status=422
                    )

            if status is not None:
                # FIXME: 'scheduled' needs to use data from scheduler
                # make safe via controlled vocab
                filters.append(
                    "("
                    + " OR ".join(
                        map(
                            lambda s: f"status = '{s}'",
                            filter(
                                lambda s: s
                                in [
                                    "queued",
                                    "running",
                                    "completed",
                                    "aborted",
                                ],
                                status.split(","),
                            ),
                        )
                    )
                    + ")"
                )

            if from_ is not None:
                # this is safeguarded by handlers (needs to satisfy
                # specific regex pattern)
                # escape single quotes anyway (for safety)
                f = from_.replace("'", "''")
                filters.append(
                    f"(datetime_started >= '{f}' OR datetime_ended >= '{f}')"
                )

            if to is not None:
                # this is safeguarded by handlers (needs to satisfy
                # specific regex pattern)
                # escape single quotes anyway (for safety)
                t = to.replace("'", "''")
                filters.append(
                    f"(datetime_started <= '{t}' OR datetime_ended <= '{t}')"
                )

            if success is not None:
                # properly format using internal methods
                # safeguarded by controlled vocab (see handlers)
                try:
                    filters.append(self.db.get_select_statement(
                        "jobs", success == "true", "success"
                    ).value.split("WHERE ")[1])
                # pylint: disable=broad-exception-caught
                except Exception as exc_info:
                    print(
                        "Failed to build job-query filter for 'success' "
                        + f"unexpectedly: {exc_info}",
                        file=sys.stderr,
                    )
                    return Response(
                        "FAILED", mimetype="text/plain", status=422
                    )

            if filters:
                base_statement.value += " WHERE " + " AND ".join(filters)

            query = self.db.execute(
                base_statement, clear_schema_cache=False
            )
            if query.error is not None:
                print(
                    f"Database query '{base_statement.value}' failed "
                    + f"unexpectedly: {query.error}",
                    file=sys.stderr,
                )
                return Response(
                    "FAILED", mimetype="text/plain", status=422
                )

            return jsonify([row[0] for row in query.data]), 200

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
            reason: Optional[str] = None,
        ):
            """Stop job associated with `token`."""
            try:
                self.adapter.abort(
                    None,
                    args=(token, {"origin": origin, "reason": reason})
                )
            # pylint: disable=broad-exception-caught
            except Exception as exc_info:
                return Response(
                    "Error while aborting job at "
                    + f"'{self.processor_abort_url}' using token '{token}': "
                    + str(exc_info),
                    mimetype="text/plain", status=502
                )
            return Response(
                f"successfully aborted '{token}'",
                mimetype="text/plain",
                status=200
            )

    def _get_records(self, bp: Blueprint):
        @bp.route(
            "/job/records", methods=["GET"], provide_automatic_options=False
        )
        @flask_handler(
            handler=handlers.get_records_handler,
            json=flask_args,
        )
        def get_records(
            token: Optional[str] = None,
            id_: Optional[str] = None,
            success: Optional[bool] = None,
        ):
            if token is not None and id_ is not None:
                return Response(
                    "Conflicting filters 'token' and 'id'.",
                    mimetype="text/plain",
                    status=409,
                )

            if token is not None:
                # due to the explicit `db.execute`, validate token/id
                # values before use
                try:
                    UUID(token)
                except ValueError as exc_info:
                    print(
                        f"Filtered dangerous value '{token}' while building "
                        + f"records-query filter for 'token': {exc_info}",
                        file=sys.stderr,
                    )
                    return Response(
                        "FAILED", mimetype="text/plain", status=422
                    )
                base_statement = self.db.get_select_statement(
                    "records",
                    token,
                    "job_token",
                    [
                        "job_token",
                        "report_id",
                        "success",
                        "external_id",
                        "origin_system_id",
                        "sip_id",
                        "ie_id",
                        "datetime_processed",
                    ],
                )
            elif id_ is not None:
                # due to the explicit `db.execute`, validate token/id
                # values before use
                try:
                    UUID(id_)
                except ValueError as exc_info:
                    print(
                        f"Filtered dangerous value '{id_}' while building "
                        + f"records-query filter for 'id': {exc_info}",
                        file=sys.stderr,
                    )
                    return Response(
                        "FAILED", mimetype="text/plain", status=422
                    )
                base_statement = _Statement(
                    f"""
                        SELECT
                            job_token, report_id, success, external_id,
                            origin_system_id, sip_id, ie_id, datetime_processed
                        FROM records
                        WHERE job_token in (
                            SELECT token FROM jobs WHERE job_config_id='{id_}'
                        )
                    """
                )
            else:
                return Response(
                    "At least one of the filters 'token' or 'id' is required.",
                    mimetype="text/plain",
                    status=400,
                )

            if success is not None:
                # properly format using internal methods
                # safeguarded by controlled vocab (see handlers)
                try:
                    base_statement.value += " AND " + (
                        self.db.get_select_statement(
                            "records", success == "true", "success"
                        ).value.split("WHERE ")[1]
                    )
                # pylint: disable=broad-exception-caught
                except Exception as exc_info:
                    print(
                        "Failed to build records-query filter for 'success' "
                        + f"unexpectedly: {exc_info}",
                        file=sys.stderr,
                    )
                    return Response(
                        "FAILED", mimetype="text/plain", status=422
                    )

            query = self.db.execute(
                base_statement, clear_schema_cache=False
            )
            if query.error is not None:
                print(
                    f"Database query '{base_statement.value}' failed "
                    + f"unexpectedly: {query.error}",
                    file=sys.stderr,
                )
                return Response(
                    "FAILED", mimetype="text/plain", status=422
                )

            return (
                jsonify(
                    [
                        Record(
                            token=r[0],
                            report_id=r[1],
                            success=bool(r[2]),
                            external_id=r[3],
                            origin_system_id=r[4],
                            sip_id=r[5],
                            ie_id=r[6],
                            datetime_processed=r[7],
                        ).json
                        for r in query.data
                    ]
                ),
                200,
            )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        self._get_info(bp)
        self._post_job(bp)
        self._post_test_job(bp)
        self._list_jobs(bp)
        self._delete_job(bp)
        self._get_records(bp)
