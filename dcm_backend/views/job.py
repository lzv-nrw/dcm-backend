"""
Job View-class definition
"""

from typing import Optional
import sys
from uuid import UUID, uuid4
import re

from flask import Blueprint, jsonify, Response
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common.logger import LoggingContext as Context
from dcm_common.db import SQLAdapter
from dcm_common import util, services
from dcm_common.services.views.interface import View

from dcm_backend.config import AppConfig
from dcm_backend.models import JobConfig, TriggerType, JobInfo
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
                        # collection_id is potentially needed to fetch
                        # collection-info
                        + (
                            ["collection_id"]
                            if "collection" in keys
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
            if info.job_config_id is not None:
                # * relations (only if not a test-job)
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

            if (keys is None or "collection" in keys) and query.get(
                "collection_id"
            ) is not None:
                info.collection = self.db.get_row(
                    "job_collections",
                    query["collection_id"],
                    cols=["completed"],
                ).eval()
                if info.collection["completed"] is None:
                    info.collection["completed"] = False
                info.collection["tokens"] = list(
                    map(
                        lambda job: job[0],
                        self.db.custom_cmd(
                            # pylint: disable=consider-using-f-string
                            """
                            SELECT token FROM jobs
                            WHERE collection_id={}
                            ORDER BY datetime_triggered ASC
                            """.format(
                                self.db.decode(query["collection_id"], "uuid")
                            ),
                            clear_schema_cache=False,
                        ).eval(),
                    )
                )
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
                {
                    "process": {
                        "id": id_,
                        "resume": True,
                        "testMode": False,
                    },
                    "context": {
                        "userTriggered": user_triggered,
                        "datetimeTriggered": util.now().isoformat(),
                        "triggerType": TriggerType.MANUAL.value,
                        "artifactsTTL": self.config.CLEANUP_ARTIFACT_TTL,
                        "notifyBackend": True,
                    }
                    | ({} if token is None else {"token": token})
                },
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
                {"id": id_, "latest_exec": token.value},
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
                {
                    "process": {
                        "resume": False,
                        "testConfig": {
                            "templateId": config.template_id,
                            "dataSelection": (
                                None
                                if config.data_selection is None
                                else config.data_selection.json
                            ),
                            "dataProcessing": (
                                None
                                if config.data_processing is None
                                else config.data_processing.json
                            ),
                        },
                        "testMode": True,
                    },
                    "context": {
                        "datetimeTriggered": util.now().isoformat(),
                        "triggerType": TriggerType.TEST.value,
                        "artifactsTTL": self.config.CLEANUP_ARTIFACT_TTL,
                    },
                },
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

    def _get_ies(self, bp: Blueprint):
        @bp.route(
            "/job/ies", methods=["GET"], provide_automatic_options=False
        )
        @flask_handler(
            handler=handlers.get_ies_handler,
            json=flask_args,
        )
        def get_ies(
            job_config_id: str,
            sort: str,
            filter_by_status: Optional[str] = None,
            filter_by_text: Optional[str] = None,
            range_: Optional[str] = None,
            count: Optional[str] = None,
        ):
            # validate job config id format
            try:
                UUID(job_config_id)
            except ValueError as exc_info:
                print(
                    f"Filtered dangerous value '{job_config_id}' while "
                    + f"building get-ies query with 'jobConfigId': {exc_info}",
                    file=sys.stderr,
                )
                return Response("FAILED", mimetype="text/plain", status=422)
            # process range
            if range_ is not None:
                # this is validated in the handler
                range_ = tuple(
                    map(
                        int,
                        re.fullmatch(r"([0-9]+)\.\.([0-9]+)", range_).groups(),
                    )
                )
                if range_[1] < range_[0]:
                    return Response(
                        "Bad range.", mimetype="text/plain", status=422
                    )
                range_filter = (
                    f"LIMIT {range_[1] - range_[0]} OFFSET {range_[0]}"
                )
            else:
                range_filter = ""

            # process sort
            match sort:
                case "datetimeChanged":
                    sort_ = "ORDER BY latest_record_datetime_changed DESC NULLS LAST"
                case "originSystemId":
                    sort_ = "ORDER BY origin_system_id ASC"
                case "externalId":
                    sort_ = "ORDER BY external_id ASC"
                case "archiveIeId":
                    sort_ = "ORDER BY latest_record_archive_ie_id ASC"
                case "archiveSipId":
                    sort_ = "ORDER BY latest_record_archive_sip_id ASC"
                case "status":
                    sort_ = """ORDER BY CASE
                                WHEN latest_record_status = 'import-error' then 1
                                WHEN latest_record_status = 'build-ip-error' then 2
                                WHEN latest_record_status = 'ip-val-error' then 3
                                WHEN latest_record_status = 'obj-val-error' then 4
                                WHEN latest_record_status = 'prepare-ip-error' then 5
                                WHEN latest_record_status = 'build-sip-error' then 6
                                WHEN latest_record_status = 'transfer-error' then 7
                                WHEN latest_record_status = 'ingest-error' then 8
                                WHEN latest_record_status = 'process-error' then 9
                                WHEN latest_record_status = 'in-process' then 100
                                WHEN latest_record_status = 'complete' then 101
                            END NULLS LAST"""
                case _:
                    return Response(
                        f"Unknown sort-id '{sort}'.",
                        mimetype="text/plain",
                        status=422,
                    )

            # process filters
            if filter_by_status is not None:
                filter_by_status_ = ""  # to satisfy pylint below
                match filter_by_status:
                    case "complete":
                        filter_by_status_ = (
                            "AND latest_record_status = 'complete'"
                        )
                    case "inProcess":
                        filter_by_status_ = (
                            "AND latest_record_status = 'in-process'"
                        )
                    case "validationError":
                        filter_by_status_ = "AND (latest_record_status = 'obj-val-error' OR latest_record_status = 'ip-val-error')"
                    case "error":
                        filter_by_status_ = "AND latest_record_status like '%error%'"
                    case "ignored":
                        filter_by_status_ = "AND latest_record_ignored"
                    case _:
                        return Response(
                            f"Unknown status filter-id '{filter_by_status}'.",
                            mimetype="text/plain",
                            status=422,
                        )
            else:
                filter_by_status_ = ""

            if filter_by_text is not None:
                filter_by_text_ = """
                AND (
                    id LIKE {pattern}
                    OR job_config_id LIKE {pattern}
                    OR source_organization LIKE {pattern}
                    OR origin_system_id LIKE {pattern}
                    OR external_id LIKE {pattern}
                    OR archive_id LIKE {pattern}
                    OR latest_record_status LIKE {pattern}
                    OR latest_record_oai_identifier LIKE {pattern}
                    OR latest_record_hotfolder_original_path LIKE {pattern}
                    OR latest_record_archive_ie_id LIKE {pattern}
                    OR latest_record_archive_sip_id LIKE {pattern}
                )""".format(
                    pattern=self.config.db.decode(
                        "%" + filter_by_text + "%", "text"
                    )
                )
            else:
                filter_by_text_ = ""

            # get row-count if needed
            if count == "true":
                count_ = self.config.db.encode(
                    self.config.db.custom_cmd(
                        f"""
                        SELECT COUNT(*)
                        FROM ies_with_latest_record
                        WHERE
                            job_config_id = {self.config.db.decode(job_config_id, 'text')}
                            {filter_by_status_}
                            {filter_by_text_}
                        """,
                        clear_schema_cache=False,
                    ).eval("collecting ie count")[0][0],
                    "integer",
                )
            else:
                count_ = None

            cols = [
                # id must be first in this list (requirement for parsing)
                ("id", "id"),
                ("job_config_id", "jobConfigId"),
                ("source_organization", "sourceOrganization"),
                ("origin_system_id", "originSystemId"),
                ("external_id", "externalId"),
                ("archive_id", "archiveId"),
                ("latest_record_id", "latestRecordId"),
                ("latest_record_baginfo_metadata", "bagInfoMetadata"),
            ]
            ies_query = self.config.db.custom_cmd(
                f"""
                SELECT {', '.join(map(lambda c: c[0], cols))}
                FROM ies_with_latest_record
                WHERE
                    job_config_id = {self.config.db.decode(job_config_id, 'text')}
                    {filter_by_status_}
                    {filter_by_text_}
                {sort_}
                {range_filter}
                """,
                clear_schema_cache=False,
            ).eval("collecting ies")

            # parse results
            ies = {}
            for ie in ies_query:
                ies[ie[0]] = {}
                for i, key in enumerate(map(lambda c: c[1], cols)):
                    if key == "bagInfoMetadata":
                        ies[ie[0]][key] = self.config.db.encode(ie[i], 'jsonb')
                    else:
                        ies[ie[0]][key] = ie[i]

            # iterate to collect all records
            for ie in ies.values():
                records_query = self.config.db.get_rows(
                    "records", ie["id"], col="ie_id"
                ).eval("collecting records")

                if len(records_query) == 0:
                    continue

                ie["records"] = {}
                for record in records_query:
                    ie["records"][record["id"]] = {
                        "id": record["id"],
                        "jobToken": record["job_token"],
                        "status": record["status"],
                        "importType": record["import_type"],
                        "datetimeChanged": record["datetime_changed"],
                        "ignored": record["ignored"],
                        "bitstream": record["bitstream"],
                        "skipObjectValidation": record[
                            "skip_object_validation"
                        ],
                        "oaiIdentifier": record["oai_identifier"],
                        "oaiDatestamp": record["oai_datestamp"],
                        "hotfolderOriginalPath": record[
                            "hotfolder_original_path"
                        ],
                        "archiveIeId": record["archive_ie_id"],
                        "archiveSipId": record["archive_sip_id"],
                    }
                # delete undefined/None properties in records
                # (this mostly simplifies automated tests)
                for record in ie["records"].values():
                    for key in record.copy():
                        if record[key] is None:
                            del record[key]

            return (
                jsonify(
                    {"IEs": list(ies.values())}
                    | ({} if count is None else {"count": count_})
                ),
                200,
            )

    def _get_ie(self, bp: Blueprint):
        @bp.route("/job/ie", methods=["GET"], provide_automatic_options=False)
        @flask_handler(
            handler=handlers.get_config_id_handler(),
            json=flask_args,
        )
        def get_ie(id_: str):
            # validate ie id format
            try:
                UUID(id_)
            except ValueError as exc_info:
                print(
                    f"Filtered dangerous value '{id_}' while "
                    + f"building get-ie query with 'id': {exc_info}",
                    file=sys.stderr,
                )
                return Response("FAILED", mimetype="text/plain", status=422)

            cols = [
                # id must be first in this list (requirement for parsing)
                ("id", "id"),
                ("job_config_id", "jobConfigId"),
                ("source_organization", "sourceOrganization"),
                ("origin_system_id", "originSystemId"),
                ("external_id", "externalId"),
                ("archive_id", "archiveId"),
                ("latest_record_id", "latestRecordId"),
                ("latest_record_baginfo_metadata", "bagInfoMetadata"),
            ]
            ie_query = self.config.db.custom_cmd(
                f"""
                SELECT {', '.join(map(lambda c: c[0], cols))}
                FROM ies_with_latest_record
                WHERE id = {self.config.db.decode(id_, 'text')}
                """,
                clear_schema_cache=False,
            ).eval("collecting ie")

            if len(ie_query) == 0:
                return Response(
                    f"Unknown IE '{id_}'.", mimetype="text/plain", status=404
                )

            # parse results
            ie = {}
            for i, key in enumerate(map(lambda c: c[1], cols)):
                if key == "bagInfoMetadata":
                    ie[key] = self.config.db.encode(ie_query[0][i], 'jsonb')
                else:
                    ie[key] = ie_query[0][i]

            # iterate to collect all records
            records_query = self.config.db.get_rows(
                "records", ie["id"], col="ie_id"
            ).eval("collecting records")

            if len(records_query) > 0:
                ie["records"] = {}
            for record in records_query:
                ie["records"][record["id"]] = {
                    "id": record["id"],
                    "jobToken": record["job_token"],
                    "status": record["status"],
                    "importType": record["import_type"],
                    "datetimeChanged": record["datetime_changed"],
                    "ignored": record["ignored"],
                    "bitstream": record["bitstream"],
                    "skipObjectValidation": record["skip_object_validation"],
                    "oaiIdentifier": record["oai_identifier"],
                    "oaiDatestamp": record["oai_datestamp"],
                    "hotfolderOriginalPath": record["hotfolder_original_path"],
                    "archiveIeId": record["archive_ie_id"],
                    "archiveSipId": record["archive_sip_id"],
                }
            # delete undefined/None properties in records
            # (this mostly simplifies automated tests)
            for record in ie["records"].values():
                for key in record.copy():
                    if record[key] is None:
                        del record[key]

            return jsonify(ie), 200

    def _post_ie_plan(self, bp: Blueprint):
        @bp.route(
            "/job/ie-plan", methods=["POST"], provide_automatic_options=False
        )
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(
            handler=handlers.post_ie_plan_handler,
            json=flask_json,
        )
        def plan_ie(
            id_: str,
            clear: Optional[bool] = None,
            ignore: Optional[bool] = None,
            plan_as_bitstream: Optional[bool] = None,
            plan_to_skip_object_validation: Optional[bool] = None,
        ):
            # validate job config id format
            try:
                UUID(id_)
            except ValueError as exc_info:
                print(
                    f"Filtered dangerous value '{id_}' while "
                    + f"building ie-plan query with 'id': {exc_info}",
                    file=sys.stderr,
                )
                return Response("FAILED", mimetype="text/plain", status=422)

            # validate
            if (
                sum(
                    1 if action else 0
                    for action in [
                        clear,
                        ignore,
                        plan_as_bitstream,
                        plan_to_skip_object_validation,
                    ]
                )
                > 1
            ):
                return Response(
                    "Conflicting actions.",
                    mimetype="text/plain",
                    status=422,
                )
            if not any(
                [
                    clear,
                    ignore,
                    plan_as_bitstream,
                    plan_to_skip_object_validation,
                ]
            ):
                return Response(
                    "Missing action.",
                    mimetype="text/plain",
                    status=422,
                )

            # fetch latest record
            record_query = self.config.db.custom_cmd(
                f"""
                SELECT latest_record_id, latest_record_status, latest_record_job_token
                FROM ies_with_latest_record
                WHERE id = {self.config.db.decode(id_, 'text')}
                """,
                clear_schema_cache=False,
            ).eval("getting latest record for ie")

            # validate again
            if len(record_query) == 0:
                return Response(
                    f"Could not find IE '{id_}'.",
                    mimetype="text/plain",
                    status=404,
                )

            record_id = record_query[0][0]
            if record_id is None:
                return Response(
                    f"Could not find record for IE '{id_}'.",
                    mimetype="text/plain",
                    status=404,
                )

            record_status = record_query[0][1]
            if record_status in ["complete"]:
                return Response(
                    "Cannot modify a record that is "
                    + f"{record_status}.",
                    mimetype="text/plain",
                    status=422,
                )

            if ignore and record_status == "in-process":
                return Response(
                    "Cannot ignore a record that is in-process.",
                    mimetype="text/plain",
                    status=422,
                )

            latest_job_token = record_query[0][2]
            if latest_job_token is not None:
                latest_job_status = self.config.db.get_row(
                    "jobs", latest_job_token, cols=["status"]
                ).eval("getting latest job-report")["status"]
                if latest_job_status not in ["completed", "aborted"]:
                    return Response(
                        "Record is currently being processed.",
                        mimetype="text/plain",
                        status=400,
                    )

            # apply plan
            self.config.db.update(
                "records",
                {
                    "id": record_id,
                    "status": record_status if ignore else "in-process",
                    "ignored": ignore,
                    "bitstream": plan_as_bitstream,
                    "skip_object_validation": plan_to_skip_object_validation,
                },
            ).eval("updating record")

            return Response(
                "OK",
                mimetype="text/plain",
                status=200,
            )

    def _post_job_completion_callback(self, bp: Blueprint):

        @bp.route(
            "/job/completion",
            methods=["POST"],
            provide_automatic_options=False,
        )
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(
            handler=handlers.post_job_completion_handler,
            json=flask_json,
        )
        def completion_callback(
            token: str,
            report: dict,
            job_config_id: Optional[str] = None,
        ):
            print(
                f"Received completion-callback for token '{token}' and job "
                + f"configuration '{job_config_id}'.",
                file=sys.stderr,
            )

            # check if schedule should be disabled
            if (
                job_config_id is not None
                and report.get("args", {})
                .get("context", {})
                .get("triggerType")
                == TriggerType.SCHEDULED.value
            ):
                stop_schedule = False

                records = report.get("data", {}).get("records", {})
                if len(records) == 0:
                    print(f"{token}: No records.", file=sys.stderr)
                else:
                    for record_id, record in records.items():
                        if record["status"] == "ip-val-error":
                            stop_schedule = True
                            print(
                                f"{token}: Encountered IP-validation error in "
                                + f"record '{record_id}'. Will stop schedule "
                                + f"for job configuration '{job_config_id}'.",
                                file=sys.stderr,
                            )
                            break

                if stop_schedule:
                    jc_query = self.db.get_row(
                        "job_configs", job_config_id
                    ).eval("loading job configuration")
                    if jc_query is None:
                        print(
                            f"{token}: Unable to read job configuration "
                            + f"'{job_config_id}' from database.",
                            file=sys.stderr,
                        )
                    else:
                        jc = JobConfig.from_row(jc_query)
                        if jc.schedule is not None and jc.schedule.active:
                            print(
                                f"{token}: Stopping schedule for job "
                                + f"configuration '{job_config_id}'.",
                                file=sys.stderr,
                            )
                            jc.schedule.active = False
                            self.db.update("job_configs", jc.row).eval(
                                "writing job configuration"
                            )
                            self.scheduler.clear_jobs(job_config_id)

            # manage batched execution
            if job_config_id is not None:
                collection_id = (
                    report.get("args", {})
                    .get("context", {})
                    .get("collectionId")
                )
                if report.get("data", {}).get("finalBatch") is False:
                    # * check for existing collection
                    if collection_id is None:
                        # * finalize existing collections (if needed; this
                        #   should only occur due to some error)
                        self.db.custom_cmd(
                            # pylint: disable=consider-using-f-string
                            """
                            UPDATE job_collections
                            SET completed={}
                            WHERE job_config_id={}
                            """.format(
                                self.db.decode(True, "boolean"),
                                self.db.decode(job_config_id, "uuid"),
                            ),
                            clear_schema_cache=False,
                        ).eval("finalizing job collections")
                        # * create new collection and link job
                        collection_id = self.db.insert(
                            "job_collections", {"job_config_id": job_config_id}
                        ).eval("create job collection")
                        self.db.update(
                            "jobs",
                            {"token": token, "collection_id": collection_id},
                        ).eval("linking job to collection")
                        print(
                            f"{token}: Created new collection "
                            + f"'{collection_id}' for batched job execution.",
                            file=sys.stderr,
                        )

                    # * update records to include existing/assigned
                    #   collection_id
                    stop_collection = False
                    stop_reason = ""
                    if report.get("progress", {}).get(
                        "status"
                    ) != "completed" or not report.get("data", {}).get(
                        "success"
                    ):
                        stop_collection = True
                        stop_reason = (
                            f"Batch '{token}' did not return successful"
                        )
                    for record_id, record in (
                        report.get("data", {}).get("records", {}).items()
                    ):
                        if (
                            not stop_collection
                            and record["status"] == "ip-val-error"
                        ):
                            stop_collection = True
                            stop_reason = (
                                f"IP-validation error for record '{record_id}'"
                            )
                        self.db.update(
                            "records",
                            {"id": record_id, "collection_id": collection_id},
                        ).eval("linking record to collection")

                    # * run next batch
                    if stop_collection:
                        print(
                            f"{token}: Stopping batched processing for "
                            + f"collection '{collection_id}': {stop_reason}",
                            file=sys.stderr,
                        )
                        failed_token = str(uuid4())
                        self.db.insert(
                            "jobs",
                            {
                                "token": failed_token,
                                "status": "aborted",
                                "job_config_id": job_config_id,
                                "user_triggered": report.get("args", {})
                                .get("context", {})
                                .get("userTriggered"),
                                "datetime_triggered": util.now().isoformat(),
                                "trigger_type": report.get("args", {})
                                .get("context", {})
                                .get("triggerType"),
                                "success": False,
                                "collection_id": collection_id,
                                "datetime_started": util.now(True).isoformat(),
                                "datetime_ended": util.now(True).isoformat(),
                                "report": {
                                    "host": report.get("host", "unknown"),
                                    "token": {
                                        "value": failed_token,
                                        "expires": False,
                                    },
                                    "args": report.get("args"),
                                    "progress": {
                                        "status": "aborted",
                                        "verbose": stop_reason,
                                        "numeric": 0,
                                    },
                                    "log": {
                                        Context.ERROR.name: [
                                            {
                                                "datetime": util.now().isoformat(),
                                                "origin": "Backend",
                                                "body": (
                                                    "Aborted batched "
                                                    + "processing due to "
                                                    + f"error: {stop_reason}"
                                                ),
                                            }
                                        ]
                                    },
                                },
                            },
                        ).eval("writing dummy job")
                        self.db.update(
                            "job_collections",
                            {"id": collection_id, "completed": True},
                        ).eval("finalizing job collection")
                    else:
                        print(
                            f"{token}: Submitting another job in collection "
                            + f"'{collection_id}'.",
                            file=sys.stderr,
                        )
                        args = report.get("args", {})
                        if "context" not in args:
                            args["context"] = {}
                        args["context"]["collectionId"] = collection_id
                        args["context"][
                            "datetime_triggered"
                        ] = util.now().isoformat()
                        if "token" in args:
                            del args["token"]
                        new_token = self.adapter.submit(
                            None,
                            args,
                            info := services.APIResult(),
                        )
                        if new_token is None:
                            # unknown problem
                            # -> stop execution + write dummy to database
                            print(
                                f"{token}: Failed to submit next batch for "
                                + f"configuration '{job_config_id}': "
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
                                file=sys.stderr
                            )
                            self.db.insert(
                                "jobs",
                                {
                                    "status": "aborted",
                                    "job_config_id": job_config_id,
                                    "user_triggered": report.get("args", {})
                                    .get("context", {})
                                    .get("userTriggered"),
                                    "datetime_triggered": util.now().isoformat(),
                                    "trigger_type": report.get("args", {})
                                    .get("context", {})
                                    .get("triggerType"),
                                    "success": False,
                                    "collection_id": collection_id,
                                    "datetime_started": util.now(True).isoformat(),
                                    "datetime_ended": util.now(True).isoformat(),
                                    "report": info.report,
                                },
                            ).eval("writing dummy job")
                            self.db.update(
                                "job_collections",
                                {"id": collection_id, "completed": True},
                            ).eval("finalizing job collection")
                        else:
                            print(
                                f"{token}: New job with token "
                                + f"'{new_token.value}' started in collection "
                                + f"'{collection_id}'.",
                                file=sys.stderr,
                            )
                            self.db.update(
                                "job_configs",
                                {
                                    "id": job_config_id,
                                    "latest_exec": new_token.value,
                                },
                            ).eval("update latest-execution")
                else:
                    # * flag collection as completed
                    if collection_id is not None:
                        self.db.update(
                            "job_collections",
                            {"id": collection_id, "completed": True},
                        ).eval("finalizing job collection")

            print(f"{token}: Done.", file=sys.stderr)

            return Response(
                "OK",
                mimetype="text/plain",
                status=200,
            )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        self._get_info(bp)
        self._post_job(bp)
        self._post_test_job(bp)
        self._list_jobs(bp)
        self._delete_job(bp)
        self._get_ies(bp)
        self._get_ie(bp)
        self._post_ie_plan(bp)
        self._post_job_completion_callback(bp)
