"""
This module defines the Job Processor `ServiceAdapter`.
"""

from typing import Any

from dcm_common.orchestra import Status
from dcm_common.services import APIResult, ServiceAdapter
import dcm_job_processor_sdk


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

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ProcessApi(client)

    def _get_api_endpoint(self):
        return self._api_client.process

    def _get_abort_endpoint(self):
        return self._api_client.abort

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
