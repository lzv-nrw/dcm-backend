"""
This module defines the Job Processor `ServiceAdapter`.
"""

from typing import Any, Optional, Mapping, Iterable
from pathlib import Path
from copy import deepcopy

from dcm_common.db import SQLAdapter
from dcm_common.models.report import Status
from dcm_common.services import APIResult, ServiceAdapter
import dcm_job_processor_sdk

from dcm_backend.models import (
    JobConfig,
    TemplateConfig,
    ImportSource,
)


class JobProcessorAdapter(ServiceAdapter):
    """
    `ServiceAdapter` for the Job Processor service.

    Note that this adapter uses the `get_progress`-endpoint for polling.
    `APIResult`s managed by `run`/`poll` update only the `progress`-
    block in their `report` attribute instead of the entire report.
    Success is only evaluated based on whether
    `progress.status == "completed"`.
    """

    _SERVICE_NAME = "Job Processor"
    _SDK = dcm_job_processor_sdk

    def __init__(self, db: SQLAdapter, *args, **kwargs):
        self._db = db
        super().__init__(*args, **kwargs)

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ProcessApi(client)

    def _get_api_endpoint(self):
        return self._api_client.process

    @staticmethod
    def build_request_body_import_ies(
        import_ies: Mapping,
        template: Mapping,
        job_config: Mapping,
        test_mode: bool = False,
    ) -> None:
        """Build 'import_ies'-section of the request body in place."""
        if template.get("type") == "hotfolder":
            return

        if "import" not in import_ies:
            import_ies["import"] = {}

        if template.get("type") == "plugin":
            if "plugin" not in template.get("additionalInformation", {}):
                raise ValueError(
                    "Missing plugin-identifier while formatting job from "
                    + f"template '{template['id']}'."
                )
            import_ies["import"].update(
                {
                    "plugin": template["additionalInformation"]["plugin"],
                    "args": (
                        template["additionalInformation"].get("args", {})
                        | {"test": test_mode}
                    ),
                }
            )
            return

        if template.get("type") == "oai":
            import_ies["import"].update(
                {
                    "plugin": "oai_pmh_v2",
                    "args": {
                        "base_url": template.get(
                            "additionalInformation", {}
                        ).get("url"),
                        "metadata_prefix": template.get(
                            "additionalInformation", {}
                        ).get("metadataPrefix"),
                        "test": test_mode,
                    },
                }
            )

            transfer_url_info = template.get(
                "additionalInformation", {}
            ).get("transferUrlFilters")
            if transfer_url_info is not None:
                import_ies["import"]["args"][
                    "transfer_url_info"
                ] = transfer_url_info

            sets = job_config.get("dataSelection", {}).get("sets")
            if sets is not None:
                import_ies["import"]["args"]["set_spec"] = sets

            from_ = job_config.get("dataSelection", {}).get("from")
            if from_ is not None:
                import_ies["import"]["args"]["from_"] = from_

            until = job_config.get("dataSelection", {}).get("until")
            if until is not None:
                import_ies["import"]["args"]["until"] = until

            identifiers = job_config.get("dataSelection", {}).get(
                "identifiers"
            )
            if identifiers is not None:
                import_ies["import"]["args"]["identifiers"] = identifiers

            return

    @staticmethod
    def build_request_body_build_ip(
        build_ip: Mapping, template: Mapping, job_config: Mapping
    ) -> None:
        """Build 'build_ip'-section of the request body in place."""
        if template.get("type") == "hotfolder":
            return

        if "build" not in build_ip:
            build_ip["build"] = {}

        build_ip["build"]["validate"] = False

        type_ = (
            job_config.get("dataProcessing", {})
            .get("mapping", {})
            .get("type")
        )

        if type_ == "plugin":
            build_ip["build"]["mappingPlugin"] = (
                job_config.get("dataProcessing", {})
                .get("mapping", {})
                .get("data")
            )
            return

        if type_ == "python":
            build_ip["build"]["mappingPlugin"] = {
                "plugin": "generic-mapper-plugin-string",
                "args": {
                    "mapper": {
                        "string": (
                            job_config.get("dataProcessing", {})
                            .get("mapping", {})
                            .get("data", {})
                            .get("contents")
                        ),
                        "args": {},
                    }
                },
            }
            return

        if type_ == "xslt":
            build_ip["build"]["mappingPlugin"] = {
                "plugin": "xslt-plugin",
                "args": {
                    "xslt": (
                        job_config.get("dataProcessing", {})
                        .get("mapping", {})
                        .get("data", {})
                        .get("contents")
                    )
                },
            }
            return

    @staticmethod
    def build_request_body_import_ips(
        import_ips: Mapping,
        template: Mapping,
        job_config: Mapping,
        hotfolder_import_sources: Iterable[ImportSource],
        test_mode: bool = False,
    ) -> None:
        """Build 'import_ips'-section of the request body in place."""
        if template.get("type") != "hotfolder":
            return

        if "import" not in import_ips:
            import_ips["import"] = {}
        source_id = template.get("additionalInformation", {}).get(
            "sourceId"
        )
        import_source = (
            next(
                (s for s in hotfolder_import_sources if s.id_ == source_id),
                None,
            )
        )
        if import_source is None:
            raise ValueError(f"Unknown import source id '{source_id}'.")
        import_ips["import"]["target"] = {
            "path": str(
                Path(import_source.path)
                / job_config.get("dataSelection", {}).get("path", "")
            )
        }
        import_ips["import"]["test"] = test_mode

    @staticmethod
    def build_request_body_validate_payload(validate_payload: Mapping) -> None:
        """Build 'validate_payload'-section of the request body in place."""
        if "validation" not in validate_payload:
            validate_payload["validation"] = {}
        if "plugins" not in validate_payload["validation"]:
            validate_payload["validation"]["plugins"] = {}

        validate_payload["validation"]["plugins"]["integrity"] = {
            "plugin": "integrity-bagit",
            "args": {}
        }
        validate_payload["validation"]["plugins"]["format"] = {
            "plugin": "jhove-fido-mimetype-bagit",
            "args": {}
        }

    @staticmethod
    def build_request_body_prepare_ip(
        prepare_ip: Mapping, job_config: Mapping,
    ) -> None:
        """Build 'prepare_ip'-section of the request body in place."""
        if "preparation" not in prepare_ip:
            prepare_ip["preparation"] = {}
        rights_operations = (
            job_config.get("dataProcessing", {})
            .get("preparation", {})
            .get("rightsOperations", [])
        )
        preservation_operations = (
            job_config.get("dataProcessing", {})
            .get("preparation", {})
            .get("preservationOperations", [])
        )
        if (
            rights_operations is not None
            or preservation_operations is not None
        ):
            # Both 'rightsOperations' and 'preservationOperations' are
            # treated as 'bagInfoOperations' from the Preparation Module-API.
            # The two properties are separated in the backend-API to mirror
            # their separation in the client.
            prepare_ip["preparation"]["bagInfoOperations"] = (
                rights_operations if rights_operations is not None else []
            ) + (
                preservation_operations
                if preservation_operations is not None
                else []
            )
        sig_prop_operations = (
            job_config.get("dataProcessing", {})
            .get("preparation", {})
            .get("sigPropOperations", [])
        )
        if sig_prop_operations is not None:
            prepare_ip["preparation"][
                "sigPropOperations"
            ] = sig_prop_operations

    @staticmethod
    def build_request_body_ingest(ingest: Mapping) -> None:
        """Build 'ingest'-section of the request body in place."""
        if "ingest" not in ingest:
            ingest["ingest"] = {}

        ingest["ingest"]["archiveId"] = ""

        if "target" not in ingest["ingest"]:
            ingest["ingest"]["target"] = {}

    def build_request_body(
        self,
        job_config: JobConfig,
        base_request_body: Optional[Mapping] = None,
        test_mode: bool = False,
    ) -> dict:
        """
        Build request body from the base `base_request_body` and the job
        configuration `job_config`.

        Keyword arguments:
        job_config -- job configuration
        base_request_body -- base of the request body that should be
                             sent to the job processor
                             (default None)
        test_mode -- whether to run given job in test-mode
                     (default False)
        """
        # using json-representation for easier handling of incomplete data
        job_config_ = job_config.json

        if job_config_["status"] != "ok" and not test_mode:
            raise ValueError(
                f"Job configuration '{job_config_['id']}' is not marked as ready "
                + "for execution."
            )

        # using json-representation for easier handling of incomplete data
        template = TemplateConfig.from_row(
            self._db.get_row("templates", job_config_["templateId"]).eval(
                "formatting request for job processor"
            )
        ).json

        if template["status"] != "ok":
            raise ValueError(
                f"Template '{template['id']}' is not marked as ready for "
                + "execution."
            )

        # start building request
        request_body = deepcopy(base_request_body or {})
        if "process" not in request_body:
            request_body["process"] = {}
        if "args" not in request_body["process"]:
            request_body["process"]["args"] = {}
        args = request_body["process"]["args"]
        for s in [
            "import_ies",
            "import_ips",
            "build_ip",
            "validation_metadata",
            "validation_payload",
            "prepare_ip",
            "build_sip",
        ] + ([] if test_mode else ["transfer", "ingest"]):
            if s not in args:
                args[s] = {}

        # - stages
        if "from" not in request_body["process"]:
            if template.get("type") == "hotfolder":
                request_body["process"]["from"] = "import_ips"
            else:
                request_body["process"]["from"] = "import_ies"

        if test_mode:
            request_body["process"]["to"] = "build_sip"

        # - import_ies
        self.build_request_body_import_ies(
            args["import_ies"], template, job_config_, test_mode
        )

        # - build_ip
        self.build_request_body_build_ip(
            args["build_ip"], template, job_config_
        )

        # - import_ips
        self.build_request_body_import_ips(
            args["import_ips"],
            template,
            job_config_,
            [
                ImportSource.from_row(src)
                for src in self._db.get_rows("hotfolder_import_sources").eval()
            ],
            test_mode,
        )

        # - validation_metadata
        #   nothing to do

        # - validation_payload
        self.build_request_body_validate_payload(args["validation_payload"])

        # - prepare_ip
        self.build_request_body_prepare_ip(args["prepare_ip"], job_config_)

        # - build_sip
        #   nothing to do

        if test_mode:
            return request_body

        # - transfer
        #   TODO: template_config.destination

        # - ingest
        #   TODO: load data for destination from template/database instead
        #   of generating static values
        self.build_request_body_ingest(args["ingest"])

        return request_body

    def _build_request_body(self, base_request_body: dict, target: Any):
        if target:
            base_request_body["process"] = (
                base_request_body["process"] | target
            )
        return base_request_body

    def success(self, info: APIResult) -> bool:
        return (
            info.report.get("progress", {}).get("status", "not-completed")
            == Status.COMPLETED.value
        )

    def _get_progress_endpoint(self, api):
        return getattr(api, "get_progress")

    def _update_info_report(self, data: Any, info: APIResult) -> None:
        # writes only progress
        if "status" in data:
            if info.report is None:
                info.report = {}
            info.report["progress"] = data
        else:
            info.report = data
