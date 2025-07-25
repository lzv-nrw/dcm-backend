"""
This module defines the `ArchiveController` component of the dcm-backend-app.
"""

from typing import Optional
from pathlib import Path

import requests
from dcm_common import Logger, LoggingContext as Context

from .common import ClientResponse


class RosettaAPIClient0:
    """
    A `RosettaAPIClient` can be used to trigger new deposits or retrieve
    information concerning deposit activities and associated sips.

    Implemented for API@v0 and Rosetta v8.2+

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

    _TAG: str = "Rosetta REST-API v0-Client"

    def __init__(
        self,
        auth: str | Path,
        url: str,
        proxies: Optional[dict] = None,
        timeout: Optional[float] = 10,
    ) -> None:
        self._url = url
        self.proxies = proxies
        self.timeout = timeout
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
            "accept": "application/json",
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

    def get_request(self, url: str) -> ClientResponse:
        """
        Returns a `ClientResponse` containing the deposit object or
        `None` on fail.

        Keyword arguments:
        url -- request url to process
        """
        log = Logger(default_origin=self._TAG)

        # make request
        try:
            response = requests.get(
                url,
                headers=self._headers,
                proxies=self.proxies,
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as exc_info:
            log.log(Context.ERROR, body=self._process_exception(exc_info))
            return ClientResponse(False, log, None)

        if response.status_code == 204:
            log.log(
                Context.ERROR,
                body=f"Getting resource '{url}' failed: No content.",
            )
            return ClientResponse(False, log, None)

        # log any errors
        if response.status_code >= 400:
            log.log(
                Context.ERROR,
                body=(
                    f"Getting resource '{url}' failed: Got error-code "
                    + f"'{response.status_code}': {response.text}."
                ),
            )
            return ClientResponse(False, log, None)

        return ClientResponse(True, log, response.json())

    def get_deposit(self, id_: str) -> ClientResponse:
        """
        GET-/rest/v0/deposits/{id_}

        Returns a `ClientResponse` containing the sip object or `None`
        on fail.

        Keyword arguments:
        id_ -- id of the deposit activity
        """
        return self.get_request(f"{self._url}/rest/v0/deposits/{id_}")

    def get_sip(self, id_: str) -> ClientResponse:
        """
        GET-/rest/v0/sips/{id_}

        Returns a `ClientResponse`.

        Keyword arguments:
        id_ -- id of sip to collect
        """
        url = f"{self._url}/rest/v0/sips/{id_}"
        response = self.get_request(url)
        # for some reason, the Rosetta API v0 returns an object filled
        # with null-fields (instead of null); this check brings the
        # client-behavior in line with the deposit-API
        if response.data is not None and all(
            v is None for v in response.data.values()
        ):
            response.success = False
            response.log.log(
                Context.ERROR,
                body=f"Response from fetching SIP via url '{url}' is empty.",
            )
            response.data = None
        return response

    def post_deposit(
        self, subdirectory: str, producer: str, material_flow: str
    ) -> ClientResponse:
        """
        POST-/rest/v0/deposits

        Trigger a deposit activity for a subdirectory.

        Returns a `ClientResponse` containing the deposit object or
        `None` on fail.

        Keyword arguments:
        subdirectory -- subdirectory of the load directory
                        for which a deposit activity will be triggered
        producer -- producer id of the deposit activity
        material_flow -- id of the material flow used for the deposit
                         activity
        """
        log = Logger(default_origin=self._TAG)

        # build request body
        json = {
            "subdirectory": subdirectory,
            "producer": {"value": producer},
            "material_flow": {"value": material_flow},
        }

        # make request
        url = f"{self._url}/rest/v0/deposits"
        try:
            response = requests.post(
                url,
                json=json,
                headers=self._headers,
                proxies=self.proxies,
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as exc_info:
            log.log(Context.ERROR, body=self._process_exception(exc_info))
            return ClientResponse(False, log, None)

        # log any errors
        if response.status_code >= 400:
            log.log(
                Context.ERROR,
                body=(
                    f"Getting resource '{url}' failed: Got error-code "
                    + f"'{response.status_code}': {response.text}."
                ),
            )
            return ClientResponse(False, log, None)

        return ClientResponse(True, log, response.json())
