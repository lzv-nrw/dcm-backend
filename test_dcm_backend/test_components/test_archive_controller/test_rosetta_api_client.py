"""ArchiveController-component test-module."""

from time import sleep
from pathlib import Path

import pytest
from dcm_common import LoggingContext as Context

from dcm_backend.components import RosettaAPIClient0


@pytest.fixture(name="client")
def _client():
    return RosettaAPIClient0(
        auth="Authorization: Basic 1234567890",
        url="http://localhost:5050"
    )


@pytest.mark.parametrize(
    "auth",
    ["path", "file"]
)
def test_constructor_auth(auth, file_storage):
    """
    Test constructor of `RosettaAPIClient0` with different values for
    `auth`.
    """
    header = "Authorization"
    header_value = "Basic <pass>"
    if auth == "path":
        (file_storage / "test_header").write_text(f"{header}: {header_value}")
        ac = RosettaAPIClient0(file_storage / "test_header", "")
    else:
        ac = RosettaAPIClient0(f"{header}: {header_value}", "")

    assert header in ac.headers
    assert ac.headers[header] == header_value


def test_constructor_bad_auth_format():
    """
    Test constructor of `RosettaAPIClient0` with bad value for `auth`.
    """
    with pytest.raises(ValueError):
        RosettaAPIClient0("SomeHeader: some value", "")


def test_minimal(
    client: RosettaAPIClient0, rosetta_stub, run_service
):
    """Test a minimal `RosettaAPIClient0`-workflow."""

    # run dummy Rosetta instance
    run_service(rosetta_stub, port=5050)

    # attempt get deposit
    deposit = client.get_deposit("0000")
    assert deposit.data is None
    assert Context.ERROR in deposit.log
    print(deposit.log.fancy())

    # attempt get sip
    sip = client.get_sip("SIP0000")
    assert sip.data is None
    assert Context.ERROR in sip.log
    print(sip.log.fancy())

    # post new deposit
    deposit = client.post_deposit(
        subdirectory="/", producer="a", material_flow="0"
    )
    assert deposit.data is not None
    assert Context.ERROR not in deposit.log
    assert "id" in deposit.data
    assert "sip_id" in deposit.data

    initial_deposit = deposit.data

    # attempt another get deposit
    deposit = client.get_deposit(initial_deposit["id"])
    assert deposit.data is not None
    assert Context.ERROR not in deposit.log
    assert initial_deposit == deposit.data

    # attempt another get sip
    sip = client.get_sip(initial_deposit["sip_id"])
    assert sip.data is not None
    assert Context.ERROR not in sip.log
    assert "id" in sip.data
    assert sip.data["id"] == initial_deposit["sip_id"]
    assert "iePids" in sip.data


@pytest.mark.skip(reason="manual test")
def test_manual(rosetta_stub, run_service):
    """
    This can be unskipped and used for manual testing with an actual
    implementation of the Rosetta REST-API.
    """

    # run dummy Rosetta instance
    run_service(rosetta_stub, port=5050)
    client = RosettaAPIClient0(
        Path.home() / ".rosetta" / "rosetta_auth",
        "https://lzv-test.hbz-nrw.de/",
    )

    sip = client.get_sip("0000")
    print(sip.data)
    print(sip.log.fancy())


def test_generic_http_error(client: RosettaAPIClient0, run_service):
    """
    Test method `RosettaAPIClient0.get_deposit` with an http error code.
    """

    # run dummy Rosetta instance
    run_service(
        routes=[
            (
                "/rest/v0/deposits/<id_>",
                lambda id_: ("deposits-error", 400),
                ["GET"]
            ),
            (
                "/rest/v0/deposits",
                lambda id_: ("deposit-error", 400),
                ["POST"]
            ),
            (
                "/rest/v0/sip/<id_>",
                lambda id_: ("sip-error", 400),
                ["GET"]
            ),
        ],
        port=5050
    )

    # GET-/deposits/<id_>
    deposit = client.get_deposit("0000")
    assert deposit.data is None
    assert Context.ERROR in deposit.log
    print(deposit.log.fancy())

    # POST-/deposits
    deposit = client.post_deposit("/", "a", "0")
    assert deposit.data is None
    assert Context.ERROR in deposit.log
    print(deposit.log.fancy())

    # GET-/sips/<id_>
    sip = client.get_sip("0000")
    assert sip.data is None
    assert Context.ERROR in sip.log
    print(sip.log.fancy())


def test_get_deposit_connection_error(
    client: RosettaAPIClient0,
):
    """
    Test method `RosettaAPIClient0.get_deposit` without running
    http-server.
    """

    # make request
    deposit = client.get_deposit("0000")

    assert Context.ERROR in deposit.log
    assert len(deposit.log[Context.ERROR]) == 1
    assert "Unable to establish" in deposit.log[Context.ERROR][0].body
    assert deposit.data is None


def test_post_deposit_connection_error(
    client: RosettaAPIClient0,
):
    """
    Test method `RosettaAPIClient0.post_deposit` without running
    http-server.
    """

    # make request
    deposit = client.post_deposit(
        "dir", "producer", "material_flow"
    )

    assert Context.ERROR in deposit.log
    assert len(deposit.log[Context.ERROR]) == 1
    assert "Unable to establish" in deposit.log[Context.ERROR][0].body
    assert deposit.data is None


def test_get_sip_connection_error(
    client: RosettaAPIClient0,
):
    """
    Test method `RosettaAPIClient0.get_sip` without running http-server.
    """

    # make request
    sip = client.get_sip("SIP0000")

    assert Context.ERROR in sip.log
    assert len(sip.log[Context.ERROR]) == 1
    assert "Unable to establish" in sip.log[Context.ERROR][0].body
    assert sip.data is None


def test_get_deposit_timeout_error(
    client: RosettaAPIClient0, run_service,
):
    """
    Test method `RosettaAPIClient0.get_deposit` with timeout.
    """
    client.timeout = 0.001

    # run dummy Rosetta instance
    # workaround to force timeout; did not work for some reason with
    # rosetta_stub and timeout <1e-10 in python 3.12
    run_service(
        routes=[
            (
                "/rest/v0/deposits/<id_>",
                lambda id_: sleep(0.01),
                ["GET"]
            ),
        ],
        port=5050
    )

    # make request
    deposit = client.get_deposit("0000")

    assert Context.ERROR in deposit.log
    assert len(deposit.log[Context.ERROR]) == 1
    assert "timed out" in deposit.log[Context.ERROR][0].body
    assert deposit.data is None


def test_post_deposit_timeout_error(
    client: RosettaAPIClient0, run_service,
):
    """
    Test method `RosettaAPIClient0.post_deposit` with timeout.
    """
    client.timeout = 0.001

    # run dummy Rosetta instance
    # workaround to force timeout; did not work for some reason with
    # rosetta_stub and timeout <1e-10 in python 3.12
    run_service(
        routes=[
            (
                "/rest/v0/deposits",
                lambda: sleep(0.01),
                ["POST"]
            ),
        ],
        port=5050
    )

    # make request
    deposit = client.post_deposit(
        "dir", "producer", "material_flow"
    )

    assert Context.ERROR in deposit.log
    assert len(deposit.log[Context.ERROR]) == 1
    assert "timed out" in deposit.log[Context.ERROR][0].body
    assert deposit.data is None


def test_get_sip_timeout_error(
    client: RosettaAPIClient0, run_service,
):
    """
    Test method `RosettaAPIClient0.get_sip` with timeout.
    """
    client.timeout = 0.001

    # run dummy Rosetta instance
    # workaround to force timeout; did not work for some reason with
    # rosetta_stub and timeout <1e-10 in python 3.12
    run_service(
        routes=[
            (
                "/rest/v0/sips/<id_>",
                lambda id_: sleep(0.01),
                ["GET"]
            ),
        ],
        port=5050
    )

    # make request
    sip = client.get_sip("SIP0000")

    assert Context.ERROR in sip.log
    assert len(sip.log[Context.ERROR]) == 1
    assert "timed out" in sip.log[Context.ERROR][0].body
    assert sip.data is None
