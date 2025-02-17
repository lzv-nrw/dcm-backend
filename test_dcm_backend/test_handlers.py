"""
Test module for the `dcm_backend/handlers.py`.
"""

import pytest
from data_plumber_http.settings import Responses

from dcm_backend.models import (
    IngestConfig,
    JobConfig,
    UserConfig,
    UserCredentials,
)
from dcm_backend import handlers


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {"no-ingest": None},
            400
        ),
        (  # missing both properties
            {"ingest": {}},
            400
        ),
        (  # missing archive_identifier
            {"ingest": {"rosetta": {"subdir": "subdir"}}},
            400
        ),
        (  # missing subdir
            {"ingest": {
                "archive_identifier": "rosetta",
                "rosetta": {}
                }},
            400
        ),
        (
            {"ingest": {
                "archive_identifier": "rosetta",
                "rosetta": {"subdir": "subdir"}
                }},
            Responses.GOOD.status
        ),
        (  # unknown archive_identifier
            {"ingest": {
                "archive_identifier": "some archive identifier",
                "rosetta": {"subdir": "subdir"}
                }},
            422
        ),
        (
            {
                "ingest": {
                    "archive_identifier": "rosetta",
                    "rosetta": {"subdir": "subdir"}
                },
                "callbackUrl": None
            },
            422
        ),
        (
            {
                "ingest": {
                    "archive_identifier": "rosetta",
                    "rosetta": {"subdir": "subdir"}
                },
                "callbackUrl": "no.scheme/path"
            },
            422
        ),
        (
            {
                "ingest": {
                    "archive_identifier": "rosetta",
                    "rosetta": {"subdir": "subdir"}
                },
                "callbackUrl": "https://lzv.nrw/callback"
            },
            Responses.GOOD.status
        ),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_ingest_handler(
    json, status, testing_config
):
    "Test `ingest_handler`."

    output = handlers.get_ingest_handler(
        default_producer=testing_config.ROSETTA_PRODUCER,
        default_material_flow=testing_config.ROSETTA_MATERIAL_FLOW
    ).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["ingest"], IngestConfig)
        assert output.data.value["ingest"].rosetta.material_flow ==\
            testing_config.ROSETTA_MATERIAL_FLOW


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {"no-id": None},
            400
        ),
        (
            {"id": None},
            422
        ),
        (
            {"id": "value"},
            Responses.GOOD.status
        ),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_config_id_handler_required(json, status):
    "Test `config_id_handler`."
    config_id_handler = handlers.get_config_id_handler(True)
    output = config_id_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["id_"], str)


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {},
            Responses.GOOD.status
        ),
        (
            {"id": None},
            422
        ),
        (
            {"id": "value"},
            Responses.GOOD.status
        ),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_config_id_handler_not_required(json, status):
    "Test `config_id_handler`."
    config_id_handler = handlers.get_config_id_handler(False)
    output = config_id_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert (
            "id_" not in output.data.value
            or isinstance(output.data.value["id_"], str)
        )


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {"no-job": None},
            400
        ),
        (
            {"id": ""},
            400
        ),
        (
            {"job": None},
            422
        ),
        (
            {"job": {}},
            Responses.GOOD.status
        ),
        (
            {"job": {}, "name": None},
            422
        ),
        (
            {"job": {}, "id": None},
            422
        ),
        (
            {"job": {}, "last_modified": None},
            422
        ),
        (
            {"job": {}, "last_modified": "no-ISO"},
            422
        ),
        (
            {"job": {}, "schedule": None},
            422
        ),
        (
            {"job": {}, "schedule": {}},
            400
        ),
        (
            {"job": {}, "schedule": {"active": None}},
            422
        ),
        (
            {"job": {}, "schedule": {"active": True}},
            Responses.GOOD.status
        ),
        (
            {"job": {}, "schedule": {"active": True, "start": None}},
            422
        ),
        (
            {"job": {}, "schedule": {"active": True, "start": "no-ISO"}},
            422
        ),
        (
            {"job": {}, "schedule": {"active": True, "end": None}},
            422
        ),
        (
            {"job": {}, "schedule": {"active": True, "end": "no-ISO"}},
            422
        ),
        (
            {"job": {}, "schedule": {"active": True, "repeat": None}},
            422
        ),
        (
            {"job": {}, "schedule": {"active": True, "repeat": {}}},
            400
        ),
        (
            {
                "job": {},
                "schedule": {
                    "active": True,
                    "repeat": {"unit": "day", "interval": 1}
                }
            },
            Responses.GOOD.status
        ),
        (
            {
                "job": {},
                "schedule": {
                    "active": True,
                    "repeat": {"unit": None, "interval": 1}
                }
            },
            422
        ),
        (
            {
                "job": {},
                "schedule": {
                    "active": True,
                    "repeat": {"unit": "no-unit", "interval": 1}
                }
            },
            422
        ),
        (
            {
                "job": {},
                "schedule": {
                    "active": True,
                    "repeat": {"unit": "day", "interval": -1}
                }
            },
            422
        ),
        (
            {
                "job": {},
                "schedule": {
                    "active": True,
                    "repeat": {"unit": "day", "interval": 1000000}
                }
            },
            422
        ),
        (
            {
                "schedule": {
                    "active": True,
                    "start": "2024-01-01T00:00:00+01:00",
                    "end": "2024-01-01T00:00:00+01:00",
                    "repeat": {
                        "unit": "day",
                        "interval": 1
                    }
                },
                "id": "dab3e1bf-f655-4e57-938d-d6953612552b",
                "name": "daily OAI-harvest",
                "last_modified": "2024-01-01T00:00:00+01:00",
                "job": {}
            },
            Responses.GOOD.status
        ),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_job_config_post_handler(
    json, status
):
    "Test `job_config_post_handler`."

    output = handlers.job_config_post_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], JobConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        ({}, Responses.GOOD.status),
        ({"status": None}, 422),
        ({"status": ""}, Responses.GOOD.status),
        ({"status": "scheduled"}, Responses.GOOD.status),
        ({"status": "queued"}, Responses.GOOD.status),
        ({"status": "running"}, Responses.GOOD.status),
        ({"status": "completed"}, Responses.GOOD.status),
        ({"status": "scheduled,unknown"}, 422),
        ({"status": "scheduled,queued"}, Responses.GOOD.status),
        ({"status": "scheduled,queued,running,completed,aborted"}, Responses.GOOD.status),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_job_status_filter_handler(json, status):
    "Test `job_status_filter_handler`."
    output = handlers.job_status_filter_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        if json.get("status") is not None:
            assert (
                isinstance(output.data.value["status"], list)
                and all(
                    x in ["scheduled", "queued", "running", "completed", "aborted"]
                    for x in output.data.value["status"]
                )
            )


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {"no-id": None},
            400
        ),
        (
            {"userId": None},
            422
        ),
        (
            {"userId": None, "unkown": None},
            400
        ),
        (
            {"userId": "a"},
            Responses().GOOD.status
        ),
        (
            {"userId": "a", "externalId": True},
            422
        ),
        (
            {"userId": "a", "externalId": "b"},
            Responses().GOOD.status
        ),
        (
            {"userId": "a", "firstname": 0},
            422
        ),
        (
            {"userId": "a", "firstname": "b"},
            Responses().GOOD.status
        ),
        (
            {"userId": "a", "lastname": 0},
            422
        ),
        (
            {"userId": "a", "lastname": "b"},
            Responses().GOOD.status
        ),
        (
            {"userId": "a", "email": 0},
            422
        ),
        (
            {"userId": "a", "email": "b"},
            422
        ),
        (
            {"userId": "a", "email": "pete@lzv.nrw"},
            Responses().GOOD.status
        ),
        (
            {"userId": "a", "roles": None},
            422
        ),
        (
            {"userId": "a", "roles": []},
            Responses().GOOD.status
        ),
        (
            {"userId": "a", "roles": ["1", "2"]},
            Responses().GOOD.status
        ),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_user_config_post_handler(
    json, status
):
    "Test `user_config_post_handler`."

    output = handlers.user_config_post_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], UserConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {"no-userId": None},
            400
        ),
        (
            {"password": "p"},
            400
        ),
        (
            {"userId": "u"},
            400
        ),
        (
            {"userId": None, "password": "p"},
            422
        ),
        (
            {"userId": "u", "password": None},
            422
        ),
        (
            {"userId": "u", "password": "p"},
            Responses().GOOD.status
        ),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_user_login_handler(json, status):
    "Test `user_login_handler`."

    output = handlers.user_login_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["credentials"], UserCredentials)


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {"no-userId": None},
            400
        ),
        (
            {"password": "p", "newPassword": "p"},
            400
        ),
        (
            {"userId": "u", "newPassword": "p"},
            400
        ),
        (
            {"userId": "u", "password": "p"},
            400
        ),
        (
            {"userId": None, "password": "p", "newPassword": "p"},
            422
        ),
        (
            {"userId": "u", "password": None, "newPassword": "p"},
            422
        ),
        (
            {"userId": "u", "password": "p", "newPassword": None},
            422
        ),
        (
            {"userId": "u", "password": "p", "newPassword": "p"},
            Responses().GOOD.status
        ),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_user_change_password_handler(json, status):
    "Test `user_change_password_handler`."

    output = handlers.user_change_password_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["credentials"], UserCredentials)
        assert "new_password" in output.data.value
