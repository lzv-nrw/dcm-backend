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
    WorkspaceConfig,
    TemplateConfig,
)
from dcm_backend import handlers


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"no-ingest": None}, 400),
            ({"ingest": {}}, 400),  # missing both properties
            (  # missing archiveId
                {"ingest": {"target": {"subdirectory": "subdir"}}},
                400,
            ),
            (  # missing target
                {"ingest": {"archiveId": "a"}},
                400,
            ),
            (
                {
                    "ingest": {
                        "archiveId": "a",
                        "target": {
                            "subdirectory": "subdir",
                            "producer": "0",
                            "material_flow": "1",
                        },
                    },
                    "callbackUrl": "https://lzv.nrw/callback",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "ingest": {
                        "archiveId": "a",
                        "target": {
                            "subdirectory": "subdir",
                            "producer": "0",
                            "material_flow": "1",
                        },
                    },
                    "token": None,
                },
                422,
            ),
            (
                {
                    "ingest": {
                        "archiveId": "a",
                        "target": {
                            "subdirectory": "subdir",
                            "producer": "0",
                            "material_flow": "1",
                        },
                    },
                    "token": "non-uuid",
                },
                422,
            ),
            (
                {
                    "ingest": {
                        "archiveId": "a",
                        "target": {
                            "subdirectory": "subdir",
                            "producer": "0",
                            "material_flow": "1",
                        },
                    },
                    "token": "37ee72d6-80ab-4dcd-a68d-f8d32766c80d",
                },
                Responses.GOOD.status,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_post_ingest_handler(json, status):
    "Test `post_ingest_handler`."

    output = handlers.post_ingest_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["ingest"], IngestConfig)


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
    (
        pytest_args := [
            ({"no-id": None}, 400),
            ({"id": None}, 422),
            ({"id": "value"}, Responses.GOOD.status),
            ({"id": "value", "userTriggered": None}, 422),
            ({"id": "value", "userTriggered": "value"}, Responses.GOOD.status),
            ({"id": "value", "token": None}, 422),
            ({"id": "value", "token": "non-uuid"}, 422),
            (
                {
                    "id": "value",
                    "token": "37ee72d6-80ab-4dcd-a68d-f8d32766c80d",
                },
                Responses.GOOD.status,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_post_job_handler(json, status):
    "Test `post_job_handler`."
    output = handlers.post_job_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)


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


def get_partial_json(json: dict, skip: list[str]) -> dict:
    """Helper for generating a subset of a given json."""
    return {
        k: v for k, v in json.items() if k not in skip
    }


job_config_json_ok = {
    "templateId": "d",
    "status": "ok",
    "id": "dab3e1bf-f655-4e57-938d-d6953612552b",
    "name": "daily OAI-harvest",
    "description": "daily OAI-harvest",
    "contactInfo": "some contact",
    "dataSelection": {},
    "dataProcessing": {
        "mapping": {"type": "plugin", "data": {"plugin": "demo", "args": {}}}
    },
    "schedule": {
        "active": True,
        "repeat": {"unit": "day", "interval": 1},
    },
}


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"no-status": None}, 400),
            ({"id": ""}, 400),
            ({"id": "i", "status": None, "templateId": "t"}, 422),
            ({"id": "i", "status": "ok", "templateId": None}, 422),
            ({"id": "i", "status": "some status", "templateId": "t"}, 422),
            (
                get_partial_json(job_config_json_ok, ["description"])
                | {"description": None},
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["name"]),
                400,
            ),
            (
                get_partial_json(job_config_json_ok, ["name"])
                | {"name": None},
                422,
            ),
            (
                job_config_json_ok,
                Responses.GOOD.status,
            ),
            (
                {
                    "id": "i",
                    "status": "draft",
                    "templateId": "t",
                    "name": None,
                },
                422,
            ),
            ({"id": None, "status": "draft", "templateId": "t"}, 422),
            (  # dataSelection
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": None,
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {},
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"unknown": None},
                },
                400,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"path": None},
                },
                400,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"path": "dir"},
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"identifiers": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"identifiers": [None]},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"identifiers": ["a"]},
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"sets": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"sets": [None]},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"sets": ["a"]},
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"from": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"from": "04-08-2023"},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"from": "2023-08-04"},
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"until": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"until": "04-08-2023"},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataSelection"])
                | {
                    "dataSelection": {"until": "2023-08-04"},
                },
                Responses.GOOD.status,
            ),
            (  # dataProcessing
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": None,
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {},
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {"unknown": None},
                },
                400,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {"mapping": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {"mapping": {}},
                },
                400,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "mapping": {"type": "python", "data": {}}
                    },
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {"mapping": {"type": None, "data": {}}},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "mapping": {"type": "python", "data": None}
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "mapping": {
                            "type": "python",
                            "data": {
                                "contents": "-",
                                "name": "-",
                                "datetimeUploaded": "-",
                            },
                        }
                    },
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "mapping": {
                            "type": "python",
                            "data": {
                                "unknown": "-",
                            },
                        }
                    },
                },
                400,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "mapping": {
                            "type": "plugin",
                            "data": {
                                "plugin": "-",
                                "args": {},
                            },
                        }
                    },
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "mapping": {
                            "type": "plugin",
                            "data": {
                                "unknown": "-",
                            },
                        }
                    },
                },
                400,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {"preparation": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {"preparation": {}},
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "preparation": {"rightsOperations": None}
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "preparation": {"rightsOperations": [None]}
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "preparation": {"rightsOperations": [{}]}
                    },
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "preparation": {"sigPropOperations": None}
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "preparation": {"sigPropOperations": []}
                    },
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "preparation": {"preservationOperations": None}
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "preparation": {"preservationOperations": [None]}
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["dataProcessing"])
                | {
                    "dataProcessing": {
                        "preparation": {"preservationOperations": [{}]}
                    },
                },
                Responses.GOOD.status,
            ),
            (  # schedule
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": None,
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {},
                },
                400,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {"active": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {"active": True},
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {"active": True, "start": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {"active": True, "start": "no-ISO"},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {"active": True, "end": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {"active": True, "end": "no-ISO"},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {"active": True, "repeat": None},
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {"active": True, "repeat": {}},
                },
                400,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {
                        "active": True,
                        "start": "2024-01-01T00:00:00+01:00",
                        "end": "2024-01-01T00:00:00+01:00",
                        "repeat": {"unit": "day", "interval": 1},
                    },
                },
                Responses.GOOD.status,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {
                        "active": True,
                        "repeat": {"unit": None, "interval": 1},
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {
                        "active": True,
                        "repeat": {"unit": "no-unit", "interval": 1},
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {
                        "active": True,
                        "repeat": {"unit": "day", "interval": -1},
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {
                        "active": True,
                        "repeat": {"unit": "day", "interval": 1000000},
                    },
                },
                422,
            ),
            (
                get_partial_json(job_config_json_ok, ["schedule"])
                | {
                    "schedule": {
                        "active": True,
                        "repeat": {"unit": "day", "interval": 1},
                    },
                },
                Responses.GOOD.status,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_job_config_handler_true(json, status):
    """Test `get_job_config_handler`."""

    output = handlers.get_job_config_handler(True).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], JobConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {
                "templateId": "d",
                "status": "draft",
            },
            Responses.GOOD.status
        ),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_get_job_config_handler_false(json, status):
    """Test `get_job_config_handler`."""

    output = handlers.get_job_config_handler(False).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], JobConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            (
                {
                    "templateId": "d",
                    "status": "draft",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "templateId": "d",
                    "status": "draft",
                    "userCreated": "a",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "templateId": "d",
                    "status": "draft",
                    "datetimeCreated": "a",
                },
                422,
            ),
            (
                {
                    "templateId": "d",
                    "status": "draft",
                    "datetimeCreated": "2024-01-01T00:00:00+01:00",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "templateId": "d",
                    "status": "draft",
                    "userModified": "a",
                },
                400,
            ),
            (
                {
                    "templateId": "d",
                    "status": "draft",
                    "datetimeModified": "2024-01-01T00:00:00+01:00",
                },
                400,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_job_config_handler_created_metadata_true(json, status):
    """Test `get_job_config_handler`."""

    output = handlers.get_job_config_handler(
        False, accept_creation_md=True
    ).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], JobConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            (
                {
                    "templateId": "d",
                    "status": "draft",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "templateId": "d",
                    "status": "draft",
                    "userCreated": "a",
                },
                400,
            ),
            (
                {
                    "templateId": "d",
                    "status": "draft",
                    "datetimeCreated": "a",
                },
                400,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_job_config_handler_created_metadata_false(json, status):
    """Test `get_job_config_handler`."""

    output = handlers.get_job_config_handler(
        False, accept_creation_md=False
    ).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], JobConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({}, 400),
            ({"token": None}, 422),
            ({"token": "t"}, Responses.GOOD.status),
            ({"token": "t", "keys": None}, 422),
            ({"token": "t", "keys": "123"}, 422),
            ({"token": "t", "keys": "abc'"}, 422),
            ({"token": "t", "keys": "abc"}, Responses.GOOD.status),
            ({"token": "t", "keys": "abc,def"}, Responses.GOOD.status),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_job_handler(json, status):
    """Test `get_job_handler`."""

    output = handlers.get_job_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            # bad types do not need testing since this is a handler for
            # query-params
            ({}, Responses.GOOD.status),
            ({"unknown": ""}, 400),
            ({"group": "ab c"}, 422),
            ({"group": "abc"}, Responses.GOOD.status),
            ({"group": "abc,def"}, Responses.GOOD.status),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_list_users_handler(json, status):
    """Test `list_users_handler`."""

    output = handlers.list_users_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            # bad types do not need testing since this is a handler for
            # query-params
            ({}, Responses.GOOD.status),
            ({"unknown": ""}, 400),
            ({"templateId": "abc"}, Responses.GOOD.status),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_list_job_configs_handler(json, status):
    """Test `list_job_configs_handler`."""

    output = handlers.list_job_configs_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            # bad types do not need testing since this is a handler for
            # query-params
            ({}, Responses.GOOD.status),
            ({"unknown": ""}, 400),
            ({"id": "value"}, Responses.GOOD.status),
            ({"status": "123"}, 422),
            ({"status": "abc'"}, 422),
            ({"status": "abc"}, Responses.GOOD.status),
            ({"status": "abc,def"}, Responses.GOOD.status),
            ({"from": "a"}, 422),
            ({"from": "2025"}, Responses.GOOD.status),
            ({"from": "2025'"}, 422),
            ({"from": "2025-01-01T12:00:00.000000+00:00"}, Responses.GOOD.status),
            ({"to": "a"}, 422),
            ({"to": "2025"}, Responses.GOOD.status),
            ({"to": "2025'"}, 422),
            ({"to": "2025-01-01T12:00:00.000000+00:00"}, Responses.GOOD.status),
            ({"success": "true'"}, 422),
            ({"success": "abc"}, 422),
            ({"success": "true"}, Responses.GOOD.status),
            ({"success": "false"}, Responses.GOOD.status),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_list_jobs_handler(json, status):
    """Test `list_jobs_handler`."""

    output = handlers.list_jobs_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            # bad types do not need testing since this is a handler for
            # query-params
            ({}, Responses.GOOD.status),
            ({"unknown": ""}, 400),
            ({"id": "value"}, Responses.GOOD.status),
            ({"token": "value"}, Responses.GOOD.status),
            ({"success": "true'"}, 422),
            ({"success": "abc"}, 422),
            ({"success": "true"}, Responses.GOOD.status),
            ({"success": "false"}, Responses.GOOD.status),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_records_handler(json, status):
    """Test `get_records_handler`."""

    output = handlers.get_records_handler.run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"no-id": None}, 400),
            ({"id": None}, 422),
            ({"id": None, "unkown": None}, 400),
            ({"id": "a"}, 400),
            ({"id": "a", "email": "a@b.c", "username": None}, 422),
            ({"username": "a"}, 400),
            ({"username": "a", "id": "a"}, 400),
            ({"email": "a"}, 400),
            ({"id": "a", "email": "a"}, 400),
            ({"username": "a", "email": "a"}, 400),
            (
                {"id": "a", "username": "a", "email": "a@b.c"},
                Responses().GOOD.status,
            ),
            ({"id": "a", "username": "a", "externalId": None}, 422),
            (
                {
                    "id": "a",
                    "username": "a",
                    "externalId": "b",
                    "email": "a@b.c",
                },
                Responses().GOOD.status,
            ),
            ({"id": "a", "username": "a", "status": None}, 422),
            ({"id": "a", "username": "a", "status": "b"}, 422),
            (
                {"id": "a", "username": "a", "status": "ok", "email": "a@b.c"},
                Responses().GOOD.status,
            ),
            (
                {"id": "a", "username": "a", "email": "a@b.c", "firstname": 0},
                422,
            ),
            (
                {
                    "id": "a",
                    "username": "a",
                    "firstname": "b",
                    "email": "a@b.c",
                },
                Responses().GOOD.status,
            ),
            (
                {"id": "a", "username": "a", "email": "a@b.c", "lastname": 0},
                422,
            ),
            (
                {
                    "id": "a",
                    "username": "a",
                    "lastname": "b",
                    "email": "a@b.c",
                },
                Responses().GOOD.status,
            ),
            ({"id": "a", "username": "a", "email": 0}, 422),
            ({"id": "a", "username": "a", "email": "b"}, 422),
            (
                {"id": "a", "username": "a", "email": "a@b.c", "groups": None},
                422,
            ),
            (
                {
                    "id": "a",
                    "username": "a",
                    "email": "a@b.c",
                    "groups": [
                        {"id": "admin"},
                        {"id": "curator", "workspace": "ws0"},
                    ],
                },
                Responses().GOOD.status,
            ),
            (
                {
                    "id": "a",
                    "status": "ok",
                    "username": "a",
                },
                400,
            ),
            (
                {
                    "id": "a",
                    "status": "inactive",
                    "username": "a",
                },
                400,
            ),
            (
                {
                    "id": "a",
                    "status": "deleted",
                    "username": "a",
                    "email": "a@b.c",
                },
                400,
            ),
            (
                {
                    "id": "a",
                    "status": "deleted",
                    "username": "a",
                },
                Responses().GOOD.status,
            ),
            (
                {
                    "id": "a",
                    "status": "deleted",
                    "username": "a",
                    "firstname": "b",
                    "lastname": "c",
                },
                Responses().GOOD.status,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_user_config_handler_true(
    json, status
):
    "Test `get_user_config_handler`."

    output = handlers.get_user_config_handler(True).run(json=json)

    print(output.last_message)
    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], UserConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"username": "a", "email": "a@b.c"}, Responses().GOOD.status),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_user_config_handler_false(json, status):
    "Test `get_user_config_handler`."

    output = handlers.get_user_config_handler(False).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], UserConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            (
                {
                    "username": "a",
                    "email": "a@b.c",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "username": "a",
                    "email": "a@b.c",
                    "userCreated": "a",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "username": "a",
                    "email": "a@b.c",
                    "datetimeCreated": "a",
                },
                422,
            ),
            (
                {
                    "username": "a",
                    "email": "a@b.c",
                    "datetimeCreated": "2024-01-01T00:00:00+01:00",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "username": "a",
                    "email": "a@b.c",
                    "userModified": "a",
                },
                400,
            ),
            (
                {
                    "username": "a",
                    "email": "a@b.c",
                    "datetimeModified": "2024-01-01T00:00:00+01:00",
                },
                400,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_user_config_handler_created_metadata_true(json, status):
    """Test `get_user_config_handler`."""

    output = handlers.get_user_config_handler(
        False, accept_creation_md=True
    ).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], UserConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            (
                {
                    "username": "a",
                    "email": "a@b.c",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "username": "a",
                    "email": "a@b.c",
                    "userCreated": "a",
                },
                400,
            ),
            (
                {
                    "username": "a",
                    "email": "a@b.c",
                    "datetimeCreated": "a",
                },
                400,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_user_config_handler_created_metadata_false(json, status):
    """Test `get_user_config_handler`."""

    output = handlers.get_user_config_handler(
        False, accept_creation_md=False
    ).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], UserConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {"no-username": None},
            400
        ),
        (
            {"password": "p"},
            400
        ),
        (
            {"username": "u"},
            400
        ),
        (
            {"username": None, "password": "p"},
            422
        ),
        (
            {"username": "u", "password": None},
            422
        ),
        (
            {"username": "u", "password": "p"},
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
            {"no-username": None},
            400
        ),
        (
            {"password": "p", "newPassword": "p"},
            400
        ),
        (
            {"username": "u", "newPassword": "p"},
            400
        ),
        (
            {"username": "u", "password": "p"},
            400
        ),
        (
            {"username": None, "password": "p", "newPassword": "p"},
            422
        ),
        (
            {"username": "u", "password": None, "newPassword": "p"},
            422
        ),
        (
            {"username": "u", "password": "p", "newPassword": None},
            422
        ),
        (
            {"username": "u", "password": "p", "newPassword": "p"},
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


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"no-name": None}, 400),
            ({"id": "b", "name": None}, 422),
            ({"id": "b", "name": None, "unknown": None}, 400),
            ({"name": "a", "id": True}, 422),
            ({"name": "a", "id": 0}, 422),
            ({"name": "a", "id": "b"}, Responses().GOOD.status),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_workspace_config_handler_true(
    json, status
):
    "Test `get_workspace_config_handler`."

    output = handlers.get_workspace_config_handler(True).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], WorkspaceConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"name": "a"}, Responses().GOOD.status),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_workspace_config_handler_false(
    json, status
):
    "Test `get_workspace_config_handler`."

    output = handlers.get_workspace_config_handler(False).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], WorkspaceConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            (
                {
                    "name": "a",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "name": "a",
                    "userCreated": "a",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "name": "a",
                    "datetimeCreated": "a",
                },
                422,
            ),
            (
                {
                    "name": "a",
                    "datetimeCreated": "2024-01-01T00:00:00+01:00",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "name": "a",
                    "userModified": "a",
                },
                400,
            ),
            (
                {
                    "name": "a",
                    "datetimeModified": "2024-01-01T00:00:00+01:00",
                },
                400,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_workspace_config_handler_created_metadata_true(json, status):
    """Test `get_workspace_config_handler`."""

    output = handlers.get_workspace_config_handler(
        False, accept_creation_md=True
    ).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], WorkspaceConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            (
                {
                    "name": "a",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "name": "a",
                    "userCreated": "a",
                },
                400,
            ),
            (
                {
                    "name": "a",
                    "datetimeCreated": "a",
                },
                400,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_workspace_config_handler_created_metadata_false(json, status):
    """Test `get_workspace_config_handler`."""

    output = handlers.get_workspace_config_handler(
        False, accept_creation_md=False
    ).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], WorkspaceConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"no-required": None}, 400),
            ({"id": None}, 400),
            ({"id": None, "unknown": None}, 400),
            ({"id": "a", "status": None}, 422),
            ({"id": "a", "status": "not-ok"}, 422),
            ({"id": "a", "status": "ok"}, 400),
            ({"id": "a", "status": "ok", "name": True}, 422),
            ({"id": "a", "status": "ok", "name": 0}, 422),
            (
                {
                    "id": "a",
                    "status": "ok",
                    "workspaceId": "d",
                    "name": "b",
                    "description": "c",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                },
                Responses().GOOD.status,
            ),
            (
                {
                    "id": "a",
                    "status": "ok",
                    "name": "b",
                    "type": "oai",
                    "additionalInformation": {
                        "url": "https://address.to.OAI-server",
                        "metadataPrefix": "oai_dc",
                        "transferUrlFilters": [
                            {
                                "regex": r"(https://lzv\.nrw/oai/transfer=[a-z0-9]+)",
                                "path": "./",
                            }
                        ],
                    },
                },
                Responses().GOOD.status,
            ),
            (
                {
                    "id": "a",
                    "status": "ok",
                    "name": "b",
                    "type": "hotfolder",
                    "additionalInformation": {"sourceId": "some-id"},
                },
                Responses().GOOD.status,
            ),
            (
                {
                    "id": "a",
                    "status": "draft",
                    "type": "hotfolder",
                    "additionalInformation": None,
                },
                422,
            ),
            (
                {
                    "id": "a",
                    "status": "draft",
                    "type": "unknown",
                    "additionalInformation": {"unknown": "."},
                },
                422,
            ),
            (
                {
                    "id": "a",
                    "status": "draft",
                    "type": "hotfolder",
                    "additionalInformation": {"no-path": "."},
                },
                Responses().GOOD.status,
            ),
            (
                {
                    "id": "a",
                    "status": "draft",
                    "type": "hotfolder",
                    "additionalInformation": {"sourceId": None},
                },
                Responses().GOOD.status,
            ),
            (
                {
                    "id": "a",
                    "status": "draft",
                },
                Responses().GOOD.status,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_template_config_handler_true(
    json, status
):
    "Test `get_template_config_handler`."

    output = handlers.get_template_config_handler(True).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], TemplateConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        (
            {
                "status": "ok",
                "name": "b",
                "type": "plugin",
                "additionalInformation": {"plugin": "demo", "args": {}}
            },
            Responses().GOOD.status
        ),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_get_template_config_handler_false(
    json, status
):
    "Test `get_template_config_handler`."

    output = handlers.get_template_config_handler(False).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], TemplateConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            (
                {
                    "status": "ok",
                    "name": "b",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "status": "ok",
                    "name": "b",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                    "userCreated": "a",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "status": "ok",
                    "name": "b",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                    "datetimeCreated": "a",
                },
                422,
            ),
            (
                {
                    "status": "ok",
                    "name": "b",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                    "datetimeCreated": "2024-01-01T00:00:00+01:00",
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "status": "ok",
                    "name": "b",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                    "userModified": "a",
                },
                400,
            ),
            (
                {
                    "status": "ok",
                    "name": "b",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                    "datetimeModified": "2024-01-01T00:00:00+01:00",
                },
                400,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_template_config_handler_created_metadata_true(json, status):
    """Test `get_template_config_handler`."""

    output = handlers.get_template_config_handler(
        False, accept_creation_md=True
    ).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], TemplateConfig)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            (
                {
                    "status": "ok",
                    "name": "b",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                },
                Responses.GOOD.status,
            ),
            (
                {
                    "status": "ok",
                    "name": "b",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                    "userCreated": "a",
                },
                400,
            ),
            (
                {
                    "status": "ok",
                    "name": "b",
                    "type": "plugin",
                    "additionalInformation": {"plugin": "demo", "args": {}},
                    "datetimeCreated": "a",
                },
                400,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_get_template_config_handler_created_metadata_false(json, status):
    """Test `get_template_config_handler`."""

    output = handlers.get_template_config_handler(
        False, accept_creation_md=False
    ).run(json=json)

    assert output.last_status == status
    if status != Responses.GOOD.status:
        print(output.last_message)
    else:
        assert isinstance(output.data.value["config"], TemplateConfig)
