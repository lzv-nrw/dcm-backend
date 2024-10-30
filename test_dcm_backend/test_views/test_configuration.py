"""Test-module for configuration-endpoint."""

from uuid import uuid4
from time import time, sleep

from flask import jsonify
import pytest
from dcm_common.db import NativeKeyValueStoreAdapter, MemoryStore

from dcm_backend import app_factory


@pytest.fixture(name="minimal_config")
def _minimal_config():
    return {
        "id": "dab3e1bf-f655-4e57-938d-d6953612552b",
        "name": "-",
        "last_modified": "2024-01-01T00:00:00+01:00",
        "job": {}
    }


@pytest.fixture(name="client_and_db")
def _client_and_db(testing_config):
    db = NativeKeyValueStoreAdapter(MemoryStore())
    return app_factory(testing_config(), config_db=db).test_client(), db


def test_get(client_and_db, minimal_config):
    """Test endpoint `GET-/configure` of config-API."""
    client, db = client_and_db
    db.write(minimal_config["id"], minimal_config)
    response = client.get(f"/configure?id={minimal_config['id']}")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == minimal_config


def test_get_unknown(client_and_db, minimal_config):
    """Test endpoint `GET-/configure` of config-API with unknown id."""
    client, _ = client_and_db
    response = client.get(f"/configure?id={minimal_config['id']}")

    assert response.status_code == 404
    assert response.mimetype == "text/plain"


def test_post_config(client_and_db, minimal_config):
    """Test endpoint `POST-/configure` of config-API."""
    client, db = client_and_db
    response = client.post("/configure", json=minimal_config)

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == {
        "id": minimal_config["id"],
        "name": minimal_config["name"]
    }

    assert db.read(minimal_config["id"]) == minimal_config


def test_post_config_incomplete(client_and_db):
    """
    Test endpoint `POST-/configure` of config-API with incomplete data
    (id, name).
    """
    client, db = client_and_db
    json = client.post("/configure", json={"job": {}}).json

    db_json = db.read(json["id"])
    assert db_json["id"] == json["id"]
    assert db_json["name"] == json["name"]
    assert json["name"].endswith(json["id"][0:8])


def test_post_with_scheduling(client_and_db):
    """
    Test endpoint `POST-/configure` of config-API with scheduled job.
    """
    client, _ = client_and_db
    json = client.post(
        "/configure", json={
            "job": {},
            "schedule": {
                "active": True,
                "repeat": {"unit": "week", "interval": 1}
            }
        }
    ).json

    # add
    scheduled = client.get("/schedule").json["scheduler"]["scheduled"]
    assert len(scheduled) == 1
    assert json["id"] in scheduled

    # remove
    client.post(
        "/configure", json={
            "id": json["id"],
            "job": {},
            "schedule": {
                "active": False,
                "repeat": {"unit": "week", "interval": 1}
            }
        }
    )
    assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 0


def test_post_with_scheduling_and_execution(
    client_and_db, run_service, temp_folder
):
    """
    Test endpoint `POST-/configure` of config-API with scheduled job
    and wait for execution.
    """

    token = {"value": "abcdef-123", "expires": False}
    file = str(uuid4())
    assert not (temp_folder / file).exists()

    def _progress():
        (temp_folder / file).touch()
        return jsonify(
            {"status": "completed", "verbose": "-", "numeric": 100}
        ), 200
    run_service(
        routes=[
            ("/process", lambda: (jsonify(token), 201), ["POST"]),
            ("/progress", _progress, ["GET"]),
        ],
        port=8086
    )

    client, _ = client_and_db
    client.post(
        "/configure", json={
            "job": {"from": "import_ies", "args": {}},
            "schedule": {
                "active": True,
                "repeat": {"unit": "second", "interval": 2}
            }
        }
    )
    client.put("/schedule", json={})

    time0 = time()
    while not (temp_folder / file).exists() and time() - time0 < 3:
        sleep(0.01)
    client.delete("/schedule")

    assert (temp_folder / file).exists()


def test_options(client_and_db):
    """Test endpoint `OPTIONS-/configure` of config-API."""
    client, db = client_and_db
    response = client.options("/configure")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == []

    keys = ["key1", "key2"]
    for key in keys:
        db.write(key, {})
    response = client.options("/configure")
    assert response.json == keys


def test_delete_config(client_and_db):
    """Test endpoint `DELETE-/configure` of config-API."""
    client, db = client_and_db
    db.write("key", {})
    response = client.delete("/configure?id=key")

    assert response.status_code == 200
    assert response.mimetype == "text/plain"
    assert db.keys() == ()


def test_delete_config_unknown(client_and_db):
    """
    Test endpoint `DELETE-/configure` of config-API for non-existing
    config.
    """
    client, _ = client_and_db
    response = client.delete("/configure?id=key")

    assert response.status_code == 200
    assert response.mimetype == "text/plain"


def test_delete_config_scheduled(client_and_db):
    """
    Test endpoint `DELETE-/configure` of config-API with scheduled job.
    """
    client, _ = client_and_db
    id_ = client.post(
        "/configure", json={
            "job": {},
            "schedule": {
                "active": True,
                "repeat": {"unit": "week", "interval": 1}
            }
        }
    ).json["id"]

    assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 1
    client.delete(f"/configure?id={id_}")
    assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 0
