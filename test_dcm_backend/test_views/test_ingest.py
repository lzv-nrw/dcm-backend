"""Test-module for ingest-endpoint."""

from flask import jsonify
from dcm_common import LoggingContext as Context

from dcm_backend import app_factory


def test_post_ingest_minimal(
    minimal_request_body,
    rosetta_stub,
    run_service,
    testing_config,
):
    """Test basic functionality of /ingest-POST endpoint."""

    app = app_factory(testing_config())
    client = app.test_client()

    # run dummy Rosetta instance
    run_service(rosetta_stub, port=5050)

    # submit job
    response = client.post("/ingest", json=minimal_request_body)
    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/report?token={token}").json

    assert Context.ERROR.name not in json["log"]
    assert json["data"]["success"]
    assert json["data"]["details"]["archiveApi"] is not None
    assert json["data"]["details"]["deposit"] is not None
    assert json["data"]["details"]["sip"] is not None


def test_post_ingest_fail_post_error(
    minimal_request_body, run_service, testing_config
):
    """
    Test the /ingest-POST endpoint with a requests error
    during the post request.
    """

    app = app_factory(testing_config())
    client = app.test_client()

    # run dummy Rosetta instance
    run_service(
        routes=[
            (
                "/rest/v0/deposits",
                lambda *args, **kwargs: ("deposit-error", 400),
                ["POST"],
            ),
        ],
        port=5050,
    )

    # submit job
    response = client.post("/ingest", json=minimal_request_body)

    assert response.status_code == 201
    assert response.mimetype == "application/json"

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/report?token={response.json['value']}").json

    assert Context.ERROR.name in json["log"]
    assert not json["data"]["success"]


def test_post_ingest_success_get_error(
    minimal_request_body, run_service, testing_config
):
    """
    Test the /ingest-POST endpoint, when the ingest is successfully triggered,
    but there is an error during the GET-request in the archive system.
    """

    app = app_factory(testing_config())
    client = app.test_client()

    # run dummy Rosetta instance
    fake_deposit = {"id": "x", "sip_id": "y"}
    run_service(
        routes=[
            (
                "/rest/v0/deposits",
                lambda: (jsonify(fake_deposit), 200),
                ["POST"],
            ),
            ("/rest/v0/sips/<id_>", lambda id_: (jsonify(None), 400), ["GET"]),
        ],
        port=5050,
    )

    # submit job
    response = client.post("/ingest", json=minimal_request_body)

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/report?token={response.json['value']}").json

    assert Context.ERROR.name in json["log"]
    assert json["data"]["success"]
    assert json["data"]["details"]["deposit"] == fake_deposit
    assert "sip" not in json["data"]["details"]


def test_get_status_minimal(
    minimal_request_body, run_service, rosetta_stub, testing_config
):
    """Test basic functionality of /ingest-GET endpoint."""

    app = app_factory(testing_config())
    client = app.test_client()

    # run dummy Rosetta instance
    run_service(rosetta_stub, port=5050)

    # submit job
    token = client.post("/ingest", json=minimal_request_body).json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/report?token={token}").json

    response = client.get(
        f"/ingest?archiveId=a&depositId={json['data']['details']['deposit']['id']}"
    )
    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json["success"]
    assert "details" in response.json
    assert response.json["details"].get("archiveApi") is not None
    assert (
        response.json["details"].get("deposit")
        == json["data"]["details"]["deposit"]
    )
    assert response.json["details"].get("sip") is not None


def test_get_status_error(testing_config, run_service):
    """Test error-handling of /ingest-GET endpoint."""

    app = app_factory(testing_config())
    client = app.test_client()
    app.extensions["orchestra"].stop(stop_on_idle=True)

    # run dummy Rosetta instance
    run_service(
        routes=[
            (
                "/rest/v0/deposits/<id_>",
                lambda id_: (
                    (jsonify({"id": "x0", "sip_id": "y" + id_[1]}), 200)
                    if id_.startswith("x")
                    else (jsonify(None), 400)
                ),
                ["GET"],
            ),
            (
                "/rest/v0/sips/<id_>",
                lambda id_: (
                    (jsonify({"id": "y1"}), 200)
                    if id_ == "y1"
                    else (jsonify(None), 400)
                ),
                ["GET"],
            ),
        ],
        port=5050,
    )

    response = client.get("/ingest?archiveId=a&depositId=0000")
    print(response.data)
    assert response.status_code == 502

    response = client.get("/ingest?archiveId=a&depositId=x0")
    print(response.data)
    assert response.status_code == 502

    response = client.get("/ingest?archiveId=a&depositId=x1")
    assert response.json["success"]
    assert response.json["details"].get("deposit") is not None
    assert response.json["details"].get("sip") is not None
