"""
This module defines the `ArchiveController` component of the dcm-backend-app.
"""

from typing import Optional
from pathlib import Path
import json
import re

import requests
from data_plumber.output import PipelineOutput
from data_plumber_http import Object, Property, String, Null
from dcm_common import Logger, LoggingContext as Context

from dcm_backend.models import Deposit


class ArchiveController:
    """
    An `ArchiveController` can be used to trigger a new deposit
    or retrieve information concerning a deposit activity (including status).

    Keyword arguments:
    auth -- authorization HTTP header file or string (sent in all
            requests); expected format 'Authorization: Basic <pass>'
    url -- url to the Rosetta instance
    proxies -- JSON object containing a mapping of protocol name and
               corresponding proxy-address
               (default None)
    timeout -- timeout duration for remote repository in seconds; None
               indicates not timing out
               (default 10)
    """
    _TAG: str = "Archive Controller"
    _RESPONSE_HANDLER = Object(
        model=Deposit,
        properties={
            Property("id", "id_", required=True): String(),
            Property("status", required=True): String(),
            Property("sip_reason", required=True): String() | Null(),
        },
        accept_only=[
            "subdirectory", "id", "creation_date", "submission_date",
            "update_date", "status", "title", "producer_agent", "producer",
            "material_flow", "sip_id", "sip_reason", "link",
        ]
    ).assemble(_loc="<API response body>")

    def __init__(
        self,
        auth: str | Path,
        url: str,
        proxies: Optional[dict] = None,
        timeout: Optional[float] = 10
    ) -> None:
        self._url = url
        self.proxies = proxies
        self._timeout = timeout
        if isinstance(auth, Path):
            _auth_header = auth.read_text(encoding="utf-8").strip().split(": ")
        else:
            _auth_header = auth.split(": ")
        if _auth_header[0] != "Authorization":
            raise ValueError(
                "Bad authorization header (expected format 'Authorization: "
                + "Basic <pass>')"
            )
        self._headers = {
            _auth_header[0]: _auth_header[1],
            "accept": "application/json"
        }

    @property
    def headers(self) -> dict[str, str]:
        """Returns a mapping of http-headers sent in all requests."""
        return self._headers.copy()

    def _process_exception(
        self, exc_info: requests.exceptions.RequestException
    ) -> str:
        """Process and log exception."""
        if isinstance(exc_info, requests.ConnectionError):
            return (
                f"Unable to establish connection to '{self._url}' "
                + f"({exc_info})."
            )
        if isinstance(exc_info, requests.Timeout):
            return f"Connection to '{self._url}' timed out ({exc_info})."
        return (
            f"Problem encountered while making a request to '{self._url}' "
            + f"({exc_info})."
        )

    def _process_error_response(self, text: str) -> str:
        """Parse error information in the response."""
        if "<!doctype html>" in text:
            return self._process_error_as_html(text)
        return self._process_error_as_json(text)

    def _process_error_as_html(self, text: str) -> str:
        """Parse error information in a response as HTML."""
        match = re.findall(
            r"<b>Message</b>(.*)</p>.*<b>Beschreibung<\/b>(.*)<\/p>",
            text
        )
        if match:
            return f"{match[0][0].strip()}: {match[0][1].strip()}"
        return text

    def _process_error_as_json(self, text: str) -> str:
        """Parse error information in a response as JSON."""
        try:
            _json = json.loads(text)
        except json.JSONDecodeError:
            return text
        try:
            return "; ".join(
                f"{error['errorCode']}: {error['errorMessage']}"
                for error in _json["errorList"]["error"]
            )
        except IndexError:
            return text
        return ""

    def _validate_response_body(
        self, _log: Logger, _json: dict
    ) -> PipelineOutput:
        """Run validation pipeline and log any error."""
        _validation = self._RESPONSE_HANDLER.run(json=_json)
        if _validation.last_status != 0:
            _log.log(
                Context.ERROR,
                body=(
                    "Received invalid response body: "
                    + _validation.last_message
                )
            )
        return _validation

    def get_deposit(self, id_: str) -> tuple[Optional[Deposit], Logger]:
        """
        Retrieve information concerning a deposit id.

        Returns a tuple containing:
        - on success, a `Deposit` object. Otherwise, None.
        - a `Logger` object.

        Keyword arguments:
        id_ -- id of the deposit activity
        """
        _log = Logger(default_origin=self._TAG)

        if not id_:
            # Return by logging an error message
            _log.log(
                Context.ERROR,
                body="The input argument 'id_' cannot be the empty string."
            )
            return None, _log

        # make request
        try:
            response = requests.get(
                f"{self._url}/rest/v0/deposits/{id_}",
                headers=self._headers,
                proxies=self.proxies,
                timeout=self._timeout
            )
        except requests.exceptions.RequestException as exc_info:
            _log.log(
                Context.ERROR,
                body=self._process_exception(exc_info)
            )
            return None, _log

        # log any errors
        if response.status_code == 204:
            _log.log(
                Context.ERROR,
                body=(
                    f"Getting deposit for id '{id_}' failed: "
                    + f"Expected status '200' but got '{response.status_code}'"
                    + " (No Content)."
                )
            )
            return None, _log
        if response.status_code != 200:
            _log.log(
                Context.ERROR,
                body=(
                    f"Getting deposit for id '{id_}' failed: "
                    + f"Expected status '200' but got '{response.status_code}'"
                    + f" ({self._process_error_response(response.text)})."
                )
            )
            return None, _log

        # Get response body
        _json = response.json()

        # Validate response body
        _validation = self._validate_response_body(_log, _json)

        # Validate id
        if _json.get("id") != id_:
            _log.log(
                Context.WARNING,
                body=(
                    "Received deposit-object with different id: Expected "
                    + f"'{id_}' but got '{_json.get('id')}'."
                )
            )

        return _validation.data.value, _log

    def post_deposit(
        self,
        subdirectory: str,
        producer: str,
        material_flow: str
    ) -> tuple[Optional[str], Logger]:
        """
        Trigger a deposit activity for a subdirectory.

        Returns a tuple containing:
        - on success, the id of the deposit activity. Otherwise, None.
        - a `Logger` object.

        Keyword arguments:
        subdirectory -- subdirectory of the load directory
                        for which a deposit activity will be triggered
        producer -- producer id of the deposit activity
        material_flow -- id of the material flow used for the deposit activity
        """
        _log = Logger(default_origin=self._TAG)

        # build request body
        _json = {
            "link": "string",
            "subdirectory": subdirectory,
            "producer": {
                "value": producer
            },
            "material_flow": {
                "value": material_flow
            },
        }

        # make request
        try:
            response = requests.post(
                f"{self._url}/rest/v0/deposits",
                json=_json,
                headers=self._headers,
                proxies=self.proxies,
                timeout=self._timeout
            )
        except requests.exceptions.RequestException as exc_info:
            _log.log(
                Context.ERROR,
                body=self._process_exception(exc_info)
            )
            return None, _log

        # log any errors
        if response.status_code != 200:
            _log.log(
                Context.ERROR,
                body=(
                    f"Posting deposit with body '{json.dumps(_json)}' failed: "
                    + f"Expected status '200' but got '{response.status_code}'"
                    + f" ({self._process_error_response(response.text)})."
                )
            )
            return None, _log

        # Get response body
        _json = response.json()

        # Validate response body
        self._validate_response_body(_log, _json)

        _id = _json.get("id")
        if isinstance(_id, str):
            return _id, _log
        return None, _log
