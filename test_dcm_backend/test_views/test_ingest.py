"""Test-module for ingest-endpoint."""

from dcm_common import LoggingContext as Context


def test_ingest_minimal(
    client, minimal_request_body, wait_for_report,
    temp_folder, test_subdir,
    run_rosetta_dummy
):
    """Test basic functionality of /ingest-POST endpoint."""

    # run dummy Rosetta instance
    run_rosetta_dummy()

    assert (
        temp_folder / test_subdir
    ).is_dir()

    # submit job
    response = client.post(
        "/ingest",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    json = wait_for_report(client, token)

    assert Context.ERROR.name not in json["log"]
    assert json["data"]["success"]
    assert "deposit" in json["data"]
    assert "id" in json["data"]["deposit"]
    assert isinstance(json["data"]["deposit"]["id"], str)
    assert "status" in json["data"]["deposit"]
    assert json["data"]["deposit"]["status"] == "INPROCESS"


def test_ingest_fail_post_error(
    client, minimal_request_body, wait_for_report,
    temp_folder, test_subdir,
    run_rosetta_dummy
):
    """
    Test the /ingest-POST endpoint with a requests error
    during the post request.
    """

    # run dummy Rosetta instance
    run_rosetta_dummy(
        post_response="No Connection",
        post_error_code=503
    )

    assert (
        temp_folder / test_subdir
    ).is_dir()

    # submit job
    response = client.post(
        "/ingest",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    assert response.status_code == 201
    assert response.mimetype == "application/json"

    # wait until job is completed
    json = wait_for_report(client, response.json["value"])

    assert Context.ERROR.name in json["log"]
    assert not json["data"]["success"]


def test_ingest_success_get_error(
    client, minimal_request_body, wait_for_report,
    run_rosetta_dummy
):
    """
    Test the /ingest-POST endpoint, when the ingest is successfully triggered,
    but there is an error during the GET-request in the archive system.
    """

    # run dummy Rosetta instance
    run_rosetta_dummy(get_error_code=503)

    # submit job
    response = client.post(
        "/ingest",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert Context.ERROR.name in json["log"]
    assert json["data"]["success"]
    assert json["data"]["deposit"]["status"] == "TRIGGERED"


def test_get_status_minimal(
    client, minimal_request_body, wait_for_report, run_rosetta_dummy
):
    """Test basic functionality of /ingest-GET endpoint."""

    # run dummy Rosetta instance
    run_rosetta_dummy()

    response = client.get("/ingest?id=")
    assert response.status_code == 422
    assert response.mimetype == "text/plain"

    response = client.get("/ingest?id=abc")
    assert response.status_code == 502
    assert response.mimetype == "text/plain"

    # submit job
    token = client.post("/ingest", json=minimal_request_body).json["value"]
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is completed
    json = wait_for_report(client, token)

    response = client.get(f"/ingest?id={json['data']['deposit']['id']}")
    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json["deposit"] == json["data"]["deposit"]
