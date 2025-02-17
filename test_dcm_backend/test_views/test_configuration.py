"""Test-module for configuration-endpoint."""

from uuid import uuid4
from time import time, sleep

from flask import jsonify
import pytest
from dcm_common.db import NativeKeyValueStoreAdapter, MemoryStore

from dcm_backend import app_factory


@pytest.fixture(name="minimal_job_config")
def _minimal_job_config():
    return {
        "id": "dab3e1bf-f655-4e57-938d-d6953612552b",
        "name": "-",
        "last_modified": "2024-01-01T00:00:00+01:00",
        "job": {},
    }


@pytest.fixture(name="minimal_user_config")
def _minimal_user_config():
    return {
        "userId": "user0",
        "externalId": "-",
        "roles": []
    }


@pytest.fixture(name="client_and_db")
def _client_and_db(testing_config):
    db = NativeKeyValueStoreAdapter(MemoryStore())
    return (
        app_factory(
            testing_config(), job_config_db=db, user_config_db=db
        ).test_client(),
        db,
    )


def test_get_job(client_and_db, minimal_job_config):
    """Test endpoint `GET-/job/configure` of config-API."""
    client, db = client_and_db
    db.write(minimal_job_config["id"], minimal_job_config)
    response = client.get(f"/job/configure?id={minimal_job_config['id']}")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == minimal_job_config


def test_get_job_unknown(client_and_db, minimal_job_config):
    """Test endpoint `GET-/job/configure` of config-API with unknown id."""
    client, _ = client_and_db
    response = client.get(f"/job/configure?id={minimal_job_config['id']}")

    assert response.status_code == 404
    assert response.mimetype == "text/plain"


def test_post_job_config(client_and_db, minimal_job_config):
    """Test endpoint `POST-/job/configure` of config-API."""
    client, db = client_and_db
    response = client.post("/job/configure", json=minimal_job_config)

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == {
        "id": minimal_job_config["id"],
        "name": minimal_job_config["name"],
    }

    assert db.read(minimal_job_config["id"]) == minimal_job_config


def test_post_job_config_incomplete(client_and_db):
    """
    Test endpoint `POST-/job/configure` of config-API with incomplete data
    (id, name).
    """
    client, db = client_and_db
    json = client.post("/job/configure", json={"job": {}}).json

    db_json = db.read(json["id"])
    assert db_json["id"] == json["id"]
    assert db_json["name"] == json["name"]
    assert json["name"].endswith(json["id"][0:8])


def test_post_job_with_scheduling(client_and_db):
    """
    Test endpoint `POST-/job/configure` of config-API with scheduled job.
    """
    client, _ = client_and_db
    json = client.post(
        "/job/configure",
        json={
            "job": {},
            "schedule": {
                "active": True,
                "repeat": {"unit": "week", "interval": 1},
            },
        },
    ).json

    # add
    scheduled = client.get("/schedule").json["scheduler"]["scheduled"]
    assert len(scheduled) == 1
    assert json["id"] in scheduled

    # remove
    client.post(
        "/job/configure",
        json={
            "id": json["id"],
            "job": {},
            "schedule": {
                "active": False,
                "repeat": {"unit": "week", "interval": 1},
            },
        },
    )
    assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 0


def test_post_job_with_scheduling_and_execution(
    client_and_db, run_service, temp_folder
):
    """
    Test endpoint `POST-/job/configure` of config-API with scheduled job
    and wait for execution.
    """

    token = {"value": "abcdef-123", "expires": False}
    file = str(uuid4())
    assert not (temp_folder / file).exists()

    def _progress():
        (temp_folder / file).touch()
        return (
            jsonify({"status": "completed", "verbose": "-", "numeric": 100}),
            200,
        )

    run_service(
        routes=[
            ("/process", lambda: (jsonify(token), 201), ["POST"]),
            ("/progress", _progress, ["GET"]),
        ],
        port=8086,
    )

    client, _ = client_and_db
    client.post(
        "/job/configure",
        json={
            "job": {"from": "import_ies", "args": {}},
            "schedule": {
                "active": True,
                "repeat": {"unit": "second", "interval": 2},
            },
        },
    )
    client.put("/schedule", json={})

    time0 = time()
    while not (temp_folder / file).exists() and time() - time0 < 3:
        sleep(0.01)
    client.delete("/schedule")

    assert (temp_folder / file).exists()


def test_job_options(client_and_db):
    """Test endpoint `OPTIONS-/job/configure` of config-API."""
    client, db = client_and_db
    response = client.options("/job/configure")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == []

    keys = ["key1", "key2"]
    for key in keys:
        db.write(key, {})
    response = client.options("/job/configure")
    assert response.json == keys


def test_delete_job_config(client_and_db):
    """Test endpoint `DELETE-/job/configure` of config-API."""
    client, db = client_and_db
    db.write("key", {})
    response = client.delete("/job/configure?id=key")

    assert response.status_code == 200
    assert response.mimetype == "text/plain"
    assert db.keys() == ()


def test_delete_job_config_unknown(client_and_db):
    """
    Test endpoint `DELETE-/job/configure` of config-API for non-existing
    config.
    """
    client, _ = client_and_db
    response = client.delete("/job/configure?id=key")

    assert response.status_code == 200
    assert response.mimetype == "text/plain"


def test_delete_job_config_scheduled(client_and_db):
    """
    Test endpoint `DELETE-/job/configure` of config-API with scheduled job.
    """
    client, _ = client_and_db
    id_ = client.post(
        "/job/configure",
        json={
            "job": {},
            "schedule": {
                "active": True,
                "repeat": {"unit": "week", "interval": 1},
            },
        },
    ).json["id"]

    assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 1
    client.delete(f"/job/configure?id={id_}")
    assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 0


def test_user_options(client_and_db):
    """Test endpoint `OPTIONS-/user/configure` of config-API."""
    client, db = client_and_db
    response = client.options("/user/configure")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == []

    keys = ["key1", "key2"]
    for key in keys:
        db.write(key, {})
    response = client.options("/user/configure")
    assert response.json == keys


def test_get_user(client_and_db, minimal_user_config):
    """Test endpoint `GET-/user/configure` of config-API."""
    client, db = client_and_db
    db.write(minimal_user_config["userId"], minimal_user_config)
    response = client.get(
        f"/user/configure?id={minimal_user_config['userId']}"
    )

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == minimal_user_config


def test_get_user_secrets(client_and_db, minimal_user_config):
    """
    Test endpoint `GET-/user/configure` of config-API regarding secrets
    not being included in response.
    """
    client, db = client_and_db
    db.write(
        minimal_user_config["userId"], minimal_user_config | {"password": "a"}
    )
    response = client.get(
        f"/user/configure?id={minimal_user_config['userId']}"
    )

    assert "password" not in response.json
    assert "active" not in response.json


def test_post_user(client_and_db, minimal_user_config):
    """Test endpoint `POST-/user/configure` of config-API."""
    client, db = client_and_db
    response = client.post("/user/configure", json=minimal_user_config)

    assert response.status_code == 200
    assert response.mimetype == "text/plain"

    written_config = db.read(minimal_user_config["userId"])
    assert "password" in written_config
    assert "active" in written_config
    assert {
        k: v
        for k, v in written_config.items()
        if k not in ["password", "active"]
    } == minimal_user_config


def test_post_user_conflict(client_and_db, minimal_user_config):
    """
    Test endpoint `POST-/user/configure` of config-API for existing
    user.
    """
    client, db = client_and_db
    db.write(minimal_user_config["userId"], minimal_user_config)
    response = client.post("/user/configure", json=minimal_user_config)

    assert response.status_code == 409
    assert response.mimetype == "text/plain"


def test_put_user_selective_changes(client_and_db, minimal_user_config):
    """
    Test endpoint `PUT-/user/configure` of config-API for selective
    changes in the config.
    """
    client, db = client_and_db

    # prerequisites for this test
    assert "firstname" not in minimal_user_config

    # write config with firstname
    client.post(
        "/user/configure", json=minimal_user_config | {"firstname": "Pete"}
    )
    assert db.read(minimal_user_config["userId"])["firstname"] == "Pete"

    # write config with lastname
    client.put(
        "/user/configure",
        json=minimal_user_config | {"lastname": "Programmer"},
    )
    assert db.read(minimal_user_config["userId"])["firstname"] == "Pete"
    assert db.read(minimal_user_config["userId"])["lastname"] == "Programmer"


def test_put_user_does_not_change_password(
    client_and_db, minimal_user_config
):
    """
    Test endpoint `PUT-/user/configure` of config-API not
    changing/deleting password.
    """
    client, db = client_and_db
    password = "password"
    db.write(
        minimal_user_config["userId"],
        minimal_user_config | {"password": password},
    )
    client.put(
        "/user/configure", json=minimal_user_config | {"firstname": "Pete"}
    )
    new_config = db.read(minimal_user_config["userId"])

    assert new_config["firstname"] == "Pete"
    assert new_config["password"] == password


def test_delete_user(client_and_db):
    """Test endpoint `DELETE-/user/configure` of config-API."""
    client, db = client_and_db
    db.write("key", {})
    response = client.delete("/user/configure?id=key")

    assert response.status_code == 200
    assert response.mimetype == "text/plain"
    assert db.keys() == ()
