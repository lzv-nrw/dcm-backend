"""ArchiveController-component test-module."""

import pytest

from dcm_common import Logger, LoggingContext as Context

from dcm_backend.components import ArchiveController
from dcm_backend.models import Deposit


@pytest.fixture(name="archive_controller")
def _archive_controller():
    return ArchiveController(
        auth="Authorization: Basic 1234567890",
        url="http://localhost:5050"
    )


@pytest.mark.parametrize(
    "auth",
    ["path", "file"]
)
def test_archive_controller_constructor_auth(auth, temp_folder):
    """
    Test constructor of `ArchiveController` with different values for
    `auth`.
    """
    header = "Authorization"
    header_value = "Basic <pass>"
    if auth == "path":
        (temp_folder / "test_header").write_text(f"{header}: {header_value}")
        ac = ArchiveController(temp_folder / "test_header", "")
    else:
        ac = ArchiveController(f"{header}: {header_value}", "")

    assert header in ac.headers
    assert ac.headers[header] == header_value


def test_archive_controller_constructor_bad_auth_format():
    """
    Test constructor of `ArchiveController` with bad value for `auth`.
    """
    with pytest.raises(ValueError):
        ArchiveController("SomeHeader: some value", "")


def test_archive_controller_get(
    archive_controller,
    deposit_response,
    run_rosetta_dummy
):
    """
    Test the `get` method of the ArchiveController with a local http-server.
    """

    deposit_id = "0000"

    # run dummy Rosetta instance
    dummy_response = deposit_response(deposit_id)
    run_rosetta_dummy(get_response=dummy_response)

    # make request
    response_deposit, response_log = \
        archive_controller.get_deposit(deposit_id)

    assert response_deposit == \
        Deposit(
            id_=deposit_id,
            status=dummy_response["status"],
            sip_reason=dummy_response["sip_reason"]
        )
    assert isinstance(response_log, Logger)
    assert response_log.json == {}


def test_archive_controller_post(
    archive_controller,
    subdirectory,
    producer,
    material_flow,
    deposit_response,
    run_rosetta_dummy
):
    """
    Test the `post` method of the ArchiveController with a local http-server.
    """

    deposit_id = "1111"

    # run  Rosetta instance
    dummy_response = deposit_response(deposit_id)
    run_rosetta_dummy(dummy_response)

    # make request
    response_id, response_log = \
        archive_controller.post_deposit(
            subdirectory=subdirectory,
            producer=producer,
            material_flow=material_flow
        )

    assert response_id == deposit_id
    assert isinstance(response_log, Logger)
    assert response_log.json == {}


@pytest.mark.parametrize(
    ("error_msg", "target_string"),
    [
        (
            """<!doctype html><html lang="de"><head><title>HTTP Status 401 – Unautorisiert</title><style type="text/css">body {font-family:Tahoma,Arial,sans-serif;} h1, h2, h3, b {color:white;background-color:#525D76;} h1 {font-size:22px;} h2 {font-size:16px;} h3 {font-size:14px;} p {font-size:12px;} a {color:black;} .line {height:1px;background-color:#525D76;border:none;}</style></head><body><h1>HTTP Status 401 – Unautorisiert</h1><hr class="line" /><p><b>Type</b> Status Report</p><p><b>Message</b> Unauthorized</p><p><b>Beschreibung</b> The request has not been applied to the target resource because it lacks valid authentication credentials for that resource.</p><hr class="line" /><h3></h3></body></html>
""",
            ""
        ),
        (
            '{"errorsExist":true,"errorList":{"error":[{"errorCode":"2001","errorMessage":"Deposit ID wef does not exist."}]},"result":null}',
            "Deposit ID wef does not exist"
        ),
    ],
    ids=["html", "body"]
)
def test_archive_controller_get_error(
    archive_controller,
    run_rosetta_dummy,
    error_msg, target_string
):
    """
    Test the `get` method of the ArchiveController with a requests error.
    """

    deposit_id = "0000"

    # run dummy Rosetta instance
    run_rosetta_dummy(get_response=error_msg, get_error_code=503)

    # make request
    response_deposit, response_log = \
        archive_controller.get_deposit(deposit_id)

    assert response_deposit is None
    assert Context.ERROR in response_log
    assert any(
        target_string in msg["body"]
        for msg in response_log.json[Context.ERROR.name]
    )


def test_archive_controller_post_error(
    archive_controller,
    subdirectory,
    producer,
    material_flow,
    run_rosetta_dummy
):
    """
    Test the `post` method of the ArchiveController with a requests error.
    """

    # run dummy Rosetta instance
    error_msg = "No Connection"
    run_rosetta_dummy(post_response=error_msg, post_error_code=503)

    # make request
    response_id, response_log = archive_controller.post_deposit(
        subdirectory=subdirectory,
        producer=producer,
        material_flow=material_flow
    )

    assert response_id is None
    assert Context.ERROR in response_log
    assert any(
        error_msg in msg["body"]
        for msg in response_log.json[Context.ERROR.name]
    )


def test_archive_controller_get_empty_string(archive_controller):
    """
    Test the `get` method of the ArchiveController
    with an empty string as input.
    """

    deposit_id = ""
    error_msg = "The input argument 'id_' cannot be the empty string."

    response_deposit, response_log = archive_controller.get_deposit(deposit_id)

    assert response_deposit is None
    assert Context.ERROR in response_log
    assert any(
        error_msg in msg["body"]
        for msg in response_log.json[Context.ERROR.name]
    )


def test_archive_controller_get_false_id(
    archive_controller,
    deposit_response,
    run_rosetta_dummy
):
    """
    Test the `get` method of the ArchiveController when the response body
    contains a false id.
    """

    deposit_id = "0000"
    response_id = "1111"
    expected_error_msg = "Received deposit-object with different id"

    # run dummy Rosetta instance
    dummy_response = deposit_response(response_id)
    run_rosetta_dummy(dummy_response, request_id=deposit_id)

    # make request
    response_deposit, response_log = \
        archive_controller.get_deposit(deposit_id)

    assert isinstance(response_deposit, Deposit)
    assert Context.WARNING in response_log
    assert any(
        expected_error_msg in msg["body"]
        for msg in response_log.json[Context.WARNING.name]
    )


@pytest.mark.parametrize(
    ("reason"),
    [
        "type",
        "unknown",
    ]
)
@pytest.mark.parametrize(
    ("method"),
    [
        "GET",
        "POST",
    ]
)
def test_archive_controller_invalid_response(
    archive_controller,
    run_rosetta_dummy,
    deposit_response,
    reason,
    method,
    subdirectory,
    producer,
    material_flow,
):
    """
    Test the `get` and `post `methods of the ArchiveController
    when the response body is invalid due to
    * bad type for the argument 'id'
    * unknown argument
    """

    deposit_id = "0000"
    if reason == "type":
        expected_error_msg = (
            "Argument 'id' in '<API response body>' has bad type."
        )
        dummy_response = deposit_response(1111)
    else:
        dummy_response = deposit_response("1111")
        dummy_response["another-key"] = "another-value"
        expected_error_msg = (
            "Argument 'another-key' in '<API response body>' not allowed"
        )

    # run dummy Rosetta instance
    run_rosetta_dummy(
        post_response=dummy_response,
        get_response=dummy_response,
        request_id=deposit_id
    )

    if method == "GET":
        response_deposit, response_log = \
            archive_controller.get_deposit(deposit_id)

        assert response_deposit is None
    else:
        response_id, response_log = archive_controller.post_deposit(
            subdirectory=subdirectory,
            producer=producer,
            material_flow=material_flow
        )
        if reason == "type":
            assert response_id is None
        else:
            assert Context.ERROR in response_log
            assert response_id == "1111"

    assert Context.ERROR in response_log
    assert any(
        expected_error_msg in msg["body"]
        for msg in response_log.json[Context.ERROR.name]
    )


def test_archive_controller_get_connection_error(
    archive_controller,
):
    """
    Test the `get` method of the ArchiveController without running
    http-server.
    """

    # make request
    response_deposit, log = archive_controller.get_deposit("0000")

    assert Context.ERROR in log
    assert len(log[Context.ERROR]) == 1
    assert "Unable to establish" in log[Context.ERROR][0].body
    assert response_deposit is None


def test_archive_controller_post_connection_error(
    archive_controller,
):
    """
    Test the `get` method of the ArchiveController without running
    http-server.
    """

    # make request
    id_, log = archive_controller.post_deposit(
        "dir", "producer", "material_flow"
    )

    assert Context.ERROR in log
    assert len(log[Context.ERROR]) == 1
    assert "Unable to establish" in log[Context.ERROR][0].body
    assert id_ is None
