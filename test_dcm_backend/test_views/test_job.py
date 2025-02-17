"""Test-module for job-endpoint."""

from uuid import uuid4
import json
from copy import deepcopy

from flask import jsonify, request, Response
import pytest
from dcm_common.db import NativeKeyValueStoreAdapter, MemoryStore
from dcm_common.models import Token

from dcm_backend import app_factory


@pytest.fixture(name="minimal_config")
def _minimal_config():
    return {
        "id": "dab3e1bf-f655-4e57-938d-d6953612552b",
        "name": "-",
        "last_modified": "2024-01-01T00:00:00+01:00",
        "job": {"from": "import_ies", "args": {}},
        "schedule": {
            "active": True,
            "repeat": {"unit": "week", "interval": 1}
        }
    }


@pytest.fixture(name="minimal_info")
def _minimal_info():
    token = {"value": "abcdef-123", "expires": False}
    return {
        "report": {
            "host": "job-processor",
            "token": token,
            "progress": {"status": "queued"},
        },
        "token": token,
        "metadata": {
            "produced": {
                "by": "job-processor",
                "datetime": "2024-01-01T00:00:00.000000+01:00"
            }
        }
    }


@pytest.fixture(name="client_and_db")
def _client_and_dbs(testing_config):
    config_db = NativeKeyValueStoreAdapter(MemoryStore())
    report_db = NativeKeyValueStoreAdapter(MemoryStore())
    return (
        app_factory(
            testing_config(), job_config_db=config_db, report_db=report_db
        ).test_client(),
        config_db,
        report_db
    )


def test_get(client_and_db, minimal_info):
    """Test endpoint `GET-/job` of job-API."""
    client, _, report_db = client_and_db
    report_db.write(minimal_info["token"]["value"], minimal_info)
    response = client.get(f"/job?token={minimal_info['token']['value']}")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == minimal_info


def test_get_unknown(client_and_db):
    """Test endpoint `GET-/job` of job-API for unknown token."""
    client, _, _ = client_and_db
    response = client.get("/job?token=unknown")

    assert response.status_code == 404
    assert response.mimetype == "text/plain"


def test_post(
    client_and_db, minimal_config, minimal_info, temp_folder, run_service
):
    """Test endpoint `POST-/job` of job-API."""

    token = minimal_info["token"]
    file = str(uuid4())
    assert not (temp_folder / file).exists()

    def _process():
        (temp_folder / file).write_text(
            json.dumps(request.json), encoding="utf-8"
        )
        return jsonify(token), 201
    run_service(
        routes=[
            ("/process", _process, ["POST"]),
        ],
        port=8086
    )

    client, config_db, _ = client_and_db
    config_db.write(minimal_config["id"], minimal_config)
    response = client.post("/job", json={"id": minimal_config["id"]})

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    assert response.json == token

    assert (temp_folder / file).exists()
    json_ = json.loads((temp_folder / file).read_text(encoding="utf-8"))
    assert json_["process"] == minimal_config["job"]
    assert json_["id"] == minimal_config["id"]


def test_post_unknown_config(client_and_db, minimal_config):
    """Test endpoint `POST-/job` of job-API for unknown config id."""
    client, _, _ = client_and_db
    response = client.post("/job", json={"id": minimal_config["id"]})

    assert response.status_code == 404
    assert response.mimetype == "text/plain"
    assert minimal_config["id"] in response.text


def test_post_failed_submission(client_and_db, minimal_config):
    """
    Test endpoint `POST-/job` of job-API for unavailable Job Processor
    service.
    """
    client, config_db, _ = client_and_db
    config_db.write(minimal_config["id"], minimal_config)
    response = client.post("/job", json={"id": minimal_config["id"]})

    assert response.status_code == 502
    assert response.mimetype == "text/plain"
    assert "Error during submission" in response.text


def test_options(client_and_db, minimal_config, minimal_info):
    """Test endpoint `OPTIONS-/job` of job-API."""
    client, _, report_db = client_and_db

    # prepare scheduling and report_db
    client.post("/job/configure", json=minimal_config)
    minimal_info["config"] = {"original_body": {"process": {}}}
    minimal_info_queued = deepcopy(minimal_info)
    minimal_info_running = deepcopy(minimal_info)
    minimal_info_completed = deepcopy(minimal_info)
    minimal_info_aborted = deepcopy(minimal_info)
    for _json, status in [
        (minimal_info_queued, "queued"),
        (minimal_info_running, "running"),
        (minimal_info_completed, "completed"),
        (minimal_info_aborted, "aborted"),
    ]:
        _json["report"]["progress"]["status"] = status
        _json["report"]["token"] = Token().json
        _json["config"]["original_body"]["id"] = minimal_config["id"]
        _json["token"] = _json["report"]["token"]
        report_db.write(_json["token"]["value"], _json)

    del minimal_info_completed["config"]["original_body"]["id"]
    minimal_info_aborted["config"]["original_body"]["id"] = "another-id"

    # test cases
    # basics & single scheduled
    response = client.options("/job?status=scheduled")
    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == [
        {"status": "scheduled", "id": minimal_config["id"]}
    ]
    assert client.options("/job?status=scheduled&id=123").json == []

    # single running
    assert client.options("/job?status=running").json == [
        {
            "status": "running",
            "id": minimal_config["id"],
            "token": minimal_info_running["token"]
        }
    ]
    assert client.options("/job?status=running&id=123").json == []

    # queued or running
    assert all(
        x in client.options("/job?status=queued,running").json for x in [
            {
                "status": "queued",
                "id": minimal_config["id"],
                "token": minimal_info_queued["token"]
            },
            {
                "status": "running",
                "id": minimal_config["id"],
                "token": minimal_info_running["token"]
            }
        ]
    )
    assert client.options("/job?status=queued,running&id=123").json == []

    # scheduled or running
    assert all(
        x in client.options("/job?status=scheduled,running").json for x in [
            {
                "status": "scheduled",
                "id": minimal_config["id"]
            },
            {
                "status": "running",
                "id": minimal_config["id"],
                "token": minimal_info_running["token"]
            }
        ]
    )
    assert client.options("/job?status=scheduled,running&id=123").json == []

    # no status, by id
    assert client.options("/job?id=another-id").json == [
        {
            "status": "aborted",
            "id": "another-id",
            "token": minimal_info_aborted["token"]
        }
    ]

    # missing id in original request
    assert client.options("/job?status=completed").json == [
        {
            "status": "completed",
            "token": minimal_info_completed["token"]
        }
    ]

    # no filter
    assert len(client.options("/job").json) == 5


@pytest.mark.parametrize(
    "response",
    [
        Response("OK", mimetype="text/plain", status=200),
        Response("not OK", mimetype="text/plain", status=502)
    ],
    ids=["ok", "not-ok"]
)
def test_abort(
    response, client_and_db, minimal_info, run_service
):
    """Test endpoint `DELETE-/job` of job-API."""

    token = minimal_info["token"]
    run_service(
        routes=[
            ("/process", lambda: response, ["DELETE"]),
        ],
        port=8086
    )

    client, _, _ = client_and_db
    response = client.delete(
        f"/job?token={token['value']}",
        json={"origin": "test-runner", "reason": "test abort"}
    )

    assert response.status_code == response.status_code
    assert response.mimetype == "text/plain"
    assert token["value"] in response.text
