"""Test-module for job-endpoint."""

from uuid import uuid3, uuid4
import json

from flask import jsonify, request, Response
import pytest
from dcm_common.util import now

from dcm_backend import app_factory, util
from dcm_backend.models import TriggerType


@pytest.fixture(name="minimal_config")
def _minimal_config():
    return {
        "id": "dab3e1bf-f655-4e57-938d-d6953612552b",
        "templateId": util.DemoData.template1,
        "status": "ok",
        "schedule": {
            "active": True,
            "start": now().isoformat(),
            "repeat": {"unit": "week", "interval": 1},
        },
    }


@pytest.fixture(name="minimal_info")
def _minimal_info():
    token = {"value": str(uuid4()), "expires": False}
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
                "datetime": "2024-01-01T00:00:00.000000+01:00",
            }
        },
    }


def test_get_minimal(no_orchestra_testing_config):
    """Minimal test endpoint `GET-/job` of job-API."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()
    config.db.insert(
        "jobs",
        {
            "token": util.DemoData.token1,
            "trigger_type": TriggerType.MANUAL.value,
        },
    )
    response = client.get(f"/job?token={util.DemoData.token1}")

    assert response.status_code == 200
    assert response.mimetype == "application/json"


def test_get_keys(no_orchestra_testing_config):
    """
    Test endpoint `GET-/job` of job-API with `keys` query parameter.
    """
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()
    config.db.insert(
        "jobs",
        {
            "token": util.DemoData.token1,
            "job_config_id": util.DemoData.job_config1,
            "user_triggered": util.DemoData.user0,
            "datetime_triggered": now().isoformat(),
            "trigger_type": TriggerType.MANUAL.value,
            "status": "ok",
            "success": True,
            "datetime_started": now().isoformat(),
            "datetime_ended": now().isoformat(),
            "report": {},
        },
    )

    job_info = client.get(f"/job?token={util.DemoData.token1}").json
    db_query = config.db.get_row("jobs", util.DemoData.token1).eval()

    # check general contents
    key_map = {
        "job_config_id": "jobConfigId",
        "user_triggered": "userTriggered",
        "datetime_triggered": "datetimeTriggered",
        "trigger_type": "triggerType",
        "datetime_started": "datetimeStarted",
        "datetime_ended": "datetimeEnded",
    }
    for col, value in db_query.items():
        key = key_map.get(col, col)
        if key in job_info:
            assert job_info[key] == value
        else:
            assert value is None
    assert all(key in job_info for key in ["templateId", "workspaceId"])

    # test individual keys
    for key in [
        "jobConfigId",
        "status",
        "success",
        "triggerType",
        "datetimeTriggered",
        "datetimeStarted",
        "datetimeEnded",
        "report",
        "templateId",
        "workspaceId",
    ]:
        partial_job_info = client.get(
            f"/job?token={util.DemoData.token1}&keys={key}"
        ).json
        assert len(partial_job_info) == 2, f"missing '{key}'"
        assert "token" in partial_job_info
        assert key in partial_job_info
        assert partial_job_info[key] == job_info[key]

    # test multiple and unknown keys
    partial_job_info = client.get(
        f"/job?token={util.DemoData.token1}&keys=jobConfigId,status,success,unknown"
    ).json
    assert set(partial_job_info.keys()) == {
        "token",
        "jobConfigId",
        "status",
        "success",
    }


def test_get_bad(no_orchestra_testing_config):
    """Test endpoint `GET-/job` of job-API for bad token."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()
    response = client.get("/job?token=bad")

    assert response.status_code == 422
    assert response.mimetype == "text/plain"


def test_get_unknown(no_orchestra_testing_config):
    """Test endpoint `GET-/job` of job-API for unknown token."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()
    response = client.get(f"/job?token={uuid4()}")

    assert response.status_code == 404
    assert response.mimetype == "text/plain"


def test_post(
    no_orchestra_testing_config,
    minimal_config,
    minimal_info,
    file_storage,
    run_service,
):
    """Test endpoint `POST-/job` of job-API."""

    token = minimal_info["token"]
    file = str(uuid4())
    assert not (file_storage / file).exists()

    def _process():
        (file_storage / file).write_text(
            json.dumps(request.json), encoding="utf-8"
        )
        return jsonify(token), 201

    run_service(
        routes=[
            ("/process", _process, ["POST"]),
        ],
        port=8087,
    )

    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()
    minimal_config["template_id"] = minimal_config.pop("templateId")
    config.db.insert("job_configs", minimal_config).eval()
    response = client.post(
        "/job",
        json={
            "id": minimal_config["id"],
            "userTriggered": util.DemoData.user0,
        },
    )

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    assert response.json == token

    assert (file_storage / file).exists()
    request_json = json.loads(
        (file_storage / file).read_text(encoding="utf-8")
    )
    assert request_json["process"]["id"] == minimal_config["id"]
    assert request_json["context"]["userTriggered"] == util.DemoData.user0
    assert "datetimeTriggered" in request_json["context"]
    assert request_json["context"]["triggerType"] == TriggerType.MANUAL.value

    assert (
        config.db.get_row("job_configs", minimal_config["id"]).eval()[
            "latest_exec"
        ]
        == token["value"]
    )


# FIXME: enable after test-jobs are fixed
@pytest.mark.skip(reason="currently not supported")
def test_post_test(
    no_orchestra_testing_config,
    minimal_info,
    file_storage,
    minimal_config,
    run_service,
):
    """
    Test endpoint `POST-/job-test` of job-API.
    """
    token = minimal_info["token"]
    file = str(uuid4())
    assert not (file_storage / file).exists()

    def _process():
        (file_storage / file).write_text(
            json.dumps(request.json), encoding="utf-8"
        )
        return jsonify(token), 201

    run_service(
        routes=[
            ("/process", _process, ["POST"]),
        ],
        port=8087,
    )

    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()
    response = client.post(
        "/job-test", json=minimal_config | {"name": "test-job"}
    )

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    assert response.json == token

    assert (file_storage / file).exists()
    assert json.loads((file_storage / file).read_text(encoding="utf-8"))[
        "process"
    ]["testMode"]


def test_post_unknown_config(no_orchestra_testing_config, minimal_config):
    """Test endpoint `POST-/job` of job-API for unknown config id."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()
    response = client.post("/job", json={"id": minimal_config["id"]})

    assert response.status_code == 404
    assert response.mimetype == "text/plain"
    assert minimal_config["id"] in response.text


def test_post_failed_submission(no_orchestra_testing_config, minimal_config):
    """
    Test endpoint `POST-/job` of job-API for unavailable Job Processor
    service.
    """
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()
    minimal_config["template_id"] = minimal_config.pop("templateId")
    config.db.insert("job_configs", minimal_config).eval()
    response = client.post("/job", json={"id": minimal_config["id"]})

    assert response.status_code == 502
    assert response.mimetype == "text/plain"
    assert "Error during submission" in response.text


@pytest.mark.parametrize(
    ("init_cmds", "query", "expected", "status"),
    (
        pytest_args := [
            (  # query without params
                [
                    f"INSERT INTO jobs (token) VALUES ('{util.DemoData.token1}')"
                ],
                "",
                [util.DemoData.token1],
                200,
            ),
            (  # query for job config id empty
                [
                    f"INSERT INTO jobs (token, job_config_id) VALUES ('{util.DemoData.token1}', '{util.DemoData.job_config1}')"
                ],
                f"?id={uuid4()}",
                [],
                200,
            ),
            (  # query for job config id non-empty
                [
                    f"INSERT INTO jobs (token, job_config_id) VALUES ('{util.DemoData.token1}', '{util.DemoData.job_config1}')"
                ],
                f"?id={util.DemoData.job_config1}",
                [util.DemoData.token1],
                200,
            ),
            (  # sql injection via id
                [
                    f"INSERT INTO jobs (token, job_config_id) VALUES ('{util.DemoData.token1}', '{util.DemoData.job_config1}')"
                ],
                f"?id={util.DemoData.job_config1}'",
                [],
                422,
            ),
            (  # query for status empty
                [
                    f"INSERT INTO jobs (token, status) VALUES ('{util.DemoData.token1}', 'running')"
                ],
                "?status=queued",
                [],
                200,
            ),
            (  # query for status non-empty
                [
                    f"INSERT INTO jobs (token, status) VALUES ('{util.DemoData.token1}', 'running')"
                ],
                "?status=running",
                [util.DemoData.token1],
                200,
            ),
            (  # query for status non-empty (multiple)
                [
                    f"INSERT INTO jobs (token, status) VALUES ('{util.DemoData.token1}', 'running')"
                ],
                "?status=queued,running",
                [util.DemoData.token1],
                200,
            ),
            (  # query for status non-empty (multiple)
                [
                    f"INSERT INTO jobs (token, status) VALUES ('{util.DemoData.token1}', 'queued')",
                    "INSERT INTO jobs (token, status) VALUES ('a', 'running')",
                ],
                "?status=queued,running",
                [util.DemoData.token1, "a"],
                200,
            ),
            (  # sql injection via status (ignored)
                [
                    f"INSERT INTO jobs (token, status) VALUES ('{util.DemoData.token1}', 'queued')",
                ],
                "?status=queued'",
                [],
                422,
            ),
            (  # query with from empty
                [
                    f"INSERT INTO jobs (token, datetime_started) VALUES ('{util.DemoData.token1}', '2025')",
                ],
                "?from=2026",
                [],
                200,
            ),
            (  # query with from non-empty
                [
                    f"INSERT INTO jobs (token, datetime_started) VALUES ('{util.DemoData.token1}', '2025')",
                ],
                "?from=2025",
                [util.DemoData.token1],
                200,
            ),
            (  # sql injection via from (caught by handler)
                [
                    f"INSERT INTO jobs (token, datetime_started) VALUES ('{util.DemoData.token1}', '2025')",
                ],
                "?from=2025'",
                [],
                422,
            ),
            (  # query with to empty
                [
                    f"INSERT INTO jobs (token, datetime_started) VALUES ('{util.DemoData.token1}', '2027')",
                ],
                "?to=2026",
                [],
                200,
            ),
            (  # query with to non-empty
                [
                    f"INSERT INTO jobs (token, datetime_started) VALUES ('{util.DemoData.token1}', '2027')",
                ],
                "?to=2027",
                [util.DemoData.token1],
                200,
            ),
            (  # sql injection via to (caught by handler)
                [
                    f"INSERT INTO jobs (token, datetime_started) VALUES ('{util.DemoData.token1}', '2027')",
                ],
                "?to=2027'",
                [],
                422,
            ),
            (  # query by success empty
                [
                    f"INSERT INTO jobs (token, success) VALUES ('{util.DemoData.token1}', true)",
                ],
                "?success=false",
                [],
                200,
            ),
            (  # query by success non-empty
                [
                    f"INSERT INTO jobs (token, success) VALUES ('{util.DemoData.token1}', true)",
                ],
                "?success=true",
                [util.DemoData.token1],
                200,
            ),
            (  # sql injection via success (caught by handler)
                [
                    f"INSERT INTO jobs (token, success) VALUES ('{util.DemoData.token1}', true)",
                ],
                "?success=true'",
                [],
                422,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_options(
    no_orchestra_testing_config, init_cmds, query, expected, status
):
    """Test endpoint `OPTIONS-/job` of job-API."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()

    config.db.custom_cmd(
        "DELETE FROM records", clear_schema_cache=False
    ).eval()
    config.db.custom_cmd("DELETE FROM jobs", clear_schema_cache=False).eval()
    for cmd in init_cmds:
        config.db.custom_cmd(cmd, clear_schema_cache=False).eval()

    response = client.options(f"/job{query}")
    assert response.status_code == status
    if status == 200:
        assert response.json == expected


@pytest.mark.parametrize(
    "response",
    [
        Response("OK", mimetype="text/plain", status=200),
        Response("not OK", mimetype="text/plain", status=502),
    ],
    ids=["ok", "not-ok"],
)
def test_abort(
    response, no_orchestra_testing_config, minimal_info, run_service
):
    """Test endpoint `DELETE-/job` of job-API."""

    token = minimal_info["token"]
    run_service(
        routes=[
            ("/process", lambda: response, ["DELETE"]),
        ],
        port=8087,
    )

    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()
    response = client.delete(
        f"/job?token={token['value']}",
        json={"origin": "test-runner", "reason": "test abort"},
    )

    assert response.status_code == response.status_code
    assert response.mimetype == "text/plain"
    assert token["value"] in response.text


class ExtDemoData(util.DemoData):
    job_config2 = str(uuid3(util.uuid_namespace, name="job_config2"))
    job_config3 = str(uuid3(util.uuid_namespace, name="job_config3"))
    ie0 = str(uuid3(util.uuid_namespace, name="ie0"))
    ie1 = str(uuid3(util.uuid_namespace, name="ie1"))
    ie2 = str(uuid3(util.uuid_namespace, name="ie2"))
    ie3 = str(uuid3(util.uuid_namespace, name="ie3"))
    record0 = str(uuid3(util.uuid_namespace, name="record0"))
    record1 = str(uuid3(util.uuid_namespace, name="record1"))
    record2 = str(uuid3(util.uuid_namespace, name="record2"))


def test_get_ies(no_orchestra_testing_config):
    """Test endpoint `GET-/job/ies` of job-API."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()

    config.db.insert(
        "jobs",
        {
            "token": util.DemoData.token1,
            "trigger_type": TriggerType.MANUAL.value,
        },
    )
    for cmd in [
        f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-0', 'archive-0')",
        f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
        f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'complete')",
        f"INSERT INTO job_configs (id, template_id) VALUES ('{ExtDemoData.job_config2}', '{ExtDemoData.template2}')",
        f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config2}', 'a', 'b', 'ext-a', 'archive-0')",
        f"INSERT INTO job_configs (id, template_id) VALUES ('{ExtDemoData.job_config3}', '{ExtDemoData.template3}')",
    ]:
        config.db.custom_cmd(cmd, clear_schema_cache=False).eval()

    response = client.get(
        f"/job/ies?jobConfigId={ExtDemoData.job_config1}&count=true"
    )
    assert response.json == {
        "count": 2,
        "IEs": [
            {
                "id": ExtDemoData.ie0,
                "jobConfigId": ExtDemoData.job_config1,
                "sourceOrganization": "a",
                "originSystemId": "b",
                "externalId": "ext-0",
                "archiveId": "archive-0",
                "latestRecordId": ExtDemoData.record0,
                "records": {
                    ExtDemoData.record0: {
                        "id": ExtDemoData.record0,
                        "jobToken": ExtDemoData.token1,
                        "status": "complete",
                    }
                },
            },
            {
                "id": ExtDemoData.ie1,
                "jobConfigId": ExtDemoData.job_config1,
                "sourceOrganization": "a",
                "originSystemId": "b",
                "externalId": "ext-1",
                "archiveId": "archive-0",
                "latestRecordId": None,
            },
        ],
    }
    response = client.get(
        f"/job/ies?jobConfigId={ExtDemoData.job_config2}&count=true"
    )
    assert response.json == {
        "count": 1,
        "IEs": [
            {
                "id": ExtDemoData.ie2,
                "jobConfigId": ExtDemoData.job_config2,
                "sourceOrganization": "a",
                "originSystemId": "b",
                "externalId": "ext-a",
                "archiveId": "archive-0",
                "latestRecordId": None,
            }
        ],
    }
    response = client.get(
        f"/job/ies?jobConfigId={ExtDemoData.job_config3}&count=true"
    )
    assert response.json == {"count": 0, "IEs": []}


@pytest.mark.parametrize(
    ("init_cmds", "query", "expected"),
    [
        (  # simple query without default filter
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-0', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, datetime_changed) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'complete', '9999')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, datetime_changed) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'complete', '1111')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}",
            [ExtDemoData.ie0, ExtDemoData.ie1],
        ),
        (  # sort by datetimeChanged
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-0', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, datetime_changed) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'complete', '1111')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, datetime_changed) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'complete', '9999')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, datetime_changed) VALUES ('{ExtDemoData.record2}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie2}', 'complete', '4444')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&sort=datetimeChanged",
            [ExtDemoData.ie1, ExtDemoData.ie2, ExtDemoData.ie0],
        ),
        (  # sort by originSystemId
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', '3', 'ext-0', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', '1', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', '2', 'ext-2', 'archive-0')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&sort=originSystemId",
            [ExtDemoData.ie1, ExtDemoData.ie2, ExtDemoData.ie0],
        ),
        (  # sort by externalId
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&sort=externalId",
            [ExtDemoData.ie1, ExtDemoData.ie2, ExtDemoData.ie0],
        ),
        (  # sort by archiveIeId
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, archive_ie_id) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'complete', '3')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, archive_ie_id) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'complete', '1')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, archive_ie_id) VALUES ('{ExtDemoData.record2}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie2}', 'complete', '2')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&sort=archiveIeId",
            [ExtDemoData.ie1, ExtDemoData.ie2, ExtDemoData.ie0],
        ),
        (  # sort by archiveSipId
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, archive_sip_id) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'complete', '3')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, archive_sip_id) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'complete', '1')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, archive_sip_id) VALUES ('{ExtDemoData.record2}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie2}', 'complete', '2')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&sort=archiveSipId",
            [ExtDemoData.ie1, ExtDemoData.ie2, ExtDemoData.ie0],
        ),
        (  # sort by status
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie3}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-4', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'complete')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'process-error')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record2}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie2}', 'in-process')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&sort=status",
            [
                ExtDemoData.ie1,
                ExtDemoData.ie2,
                ExtDemoData.ie0,
                ExtDemoData.ie3,
            ],
        ),
        (  # filter by status: complete
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'complete')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'process-error')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&filterByStatus=complete",
            [ExtDemoData.ie0],
        ),
        (  # filter by status: in-process
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'in-process')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'process-error')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&filterByStatus=inProcess",
            [ExtDemoData.ie0],
        ),
        (  # filter by status: validationError
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'obj-val-error')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'process-error')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record2}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie2}', 'ip-val-error')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&filterByStatus=validationError",
            [ExtDemoData.ie0, ExtDemoData.ie2],
        ),
        (  # filter by status: error
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'transfer-error')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'in-process')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record2}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie2}', 'import-error')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&filterByStatus=error",
            [ExtDemoData.ie0, ExtDemoData.ie2],
        ),
        (  # filter by status: ignored
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'transfer-error')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status, ignored) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'in-process', 1)",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record2}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie2}', 'import-error')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&filterByStatus=ignored",
            [ExtDemoData.ie1],
        ),
        (  # filter by text
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-1')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&filterByText=archive-0",
            [ExtDemoData.ie0, ExtDemoData.ie1],
        ),
        (  # filter by text; case insensitive
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'Ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&filterByText=ext-1",
            [ExtDemoData.ie0, ExtDemoData.ie1],
        ),
        (  # filter by text; whitespace
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext 1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&filterByText=ext+1",
            [ExtDemoData.ie0],
        ),
        (  # filter by status and text
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-1')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'transfer-error')",
                f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record1}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie1}', 'in-process')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&filterByStatus=inProcess&filterByText=archive-0&",
            [ExtDemoData.ie1],
        ),
        (  # range
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie3}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-4', 'archive-0')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&range=0..1",
            [ExtDemoData.ie0],
        ),
        (
            [
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-1', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie1}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-2', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie2}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-3', 'archive-0')",
                f"INSERT INTO ies VALUES ('{ExtDemoData.ie3}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-4', 'archive-0')",
            ],
            f"?jobConfigId={ExtDemoData.job_config1}&range=1..3",
            [ExtDemoData.ie1, ExtDemoData.ie2],
        ),
    ],
    ids=[
        "single-no-filter",
        "sort-datetime",
        "sort-osid",
        "sort-exid",
        "sort-sip",
        "sort-ie",
        "sort-status",
        "filter-complete",
        "filter-in-process",
        "filter-valerr",
        "filter-err",
        "filter-ignored",
        "filter-text",
        "filter-text-case",
        "filter-text-whitespace",
        "filter-status&text",
        "range-01",
        "range-13",
    ],
)
def test_get_ies_filter_sort_and_range(
    no_orchestra_testing_config, init_cmds, query, expected
):
    """Test endpoint `GET-/job/ies` of job-API."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()

    config.db.insert(
        "jobs",
        {
            "token": util.DemoData.token1,
            "trigger_type": TriggerType.MANUAL.value,
        },
    )
    for cmd in init_cmds:
        config.db.custom_cmd(cmd, clear_schema_cache=False).eval()

    response = client.get(f"/job/ies{query}")
    assert response.status_code == 200
    assert list(map(lambda ie: ie["id"], response.json["IEs"])) == expected


def test_get_ie(no_orchestra_testing_config):
    """Test endpoint `GET-/job/ie` of job-API."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()

    config.db.insert(
        "jobs",
        {
            "token": util.DemoData.token1,
            "trigger_type": TriggerType.MANUAL.value,
        },
    )
    for cmd in [
        f"INSERT INTO ies VALUES ('{ExtDemoData.ie0}', '{ExtDemoData.job_config1}', 'a', 'b', 'ext-0', 'archive-0')",
        f"INSERT INTO records (id, job_config_id, job_token, ie_id, status) VALUES ('{ExtDemoData.record0}', '{ExtDemoData.job_config1}', '{ExtDemoData.token1}', '{ExtDemoData.ie0}', 'complete')",
    ]:
        config.db.custom_cmd(cmd, clear_schema_cache=False).eval()

    response = client.get(f"/job/ie?id={ExtDemoData.ie0}")
    assert response.json == {
        "id": ExtDemoData.ie0,
        "jobConfigId": ExtDemoData.job_config1,
        "sourceOrganization": "a",
        "originSystemId": "b",
        "externalId": "ext-0",
        "archiveId": "archive-0",
        "latestRecordId": ExtDemoData.record0,
        "records": {
            ExtDemoData.record0: {
                "id": ExtDemoData.record0,
                "jobToken": ExtDemoData.token1,
                "status": "complete",
            }
        },
    }

    assert client.get(f"/job/ie?id={ExtDemoData.ie1}").status_code == 404


def test_post_ie_plan(no_orchestra_testing_config):
    """Test endpoint `POST-/job/ie-plan` of job-API."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()

    # bad plans
    response = client.post(
        "/job/ie-plan",
        json={"id": ExtDemoData.ie0},
    )
    print(response.data)
    assert response.status_code == 422
    response = client.post(
        "/job/ie-plan",
        json={"id": ExtDemoData.ie0, "ignore": True, "planAsBitstream": True},
    )
    print(response.data)
    assert response.status_code == 422

    config.db.insert(
        "jobs",
        {
            "token": util.DemoData.token1,
            "status": "completed",
            "trigger_type": TriggerType.MANUAL.value,
        },
    ).eval()

    # unknown ie
    response = client.post(
        "/job/ie-plan", json={"id": ExtDemoData.ie0, "ignore": True}
    )
    print(response.data)
    assert response.status_code == 404

    config.db.insert(
        "ies",
        {
            "id": ExtDemoData.ie0,
            "job_config_id": ExtDemoData.job_config1,
            "origin_system_id": "a",
            "external_id": "0",
            "archive_id": "b",
        },
    ).eval()

    # no record
    response = client.post(
        "/job/ie-plan", json={"id": ExtDemoData.ie0, "ignore": True}
    )
    print(response.data)
    assert response.status_code == 404

    config.db.insert(
        "records",
        {
            "id": ExtDemoData.record0,
            "job_config_id": ExtDemoData.job_config1,
            "job_token": ExtDemoData.token1,
            "ie_id": ExtDemoData.ie0,
            "status": "complete",
        },
    ).eval()

    # record is already complete
    response = client.post(
        "/job/ie-plan", json={"id": ExtDemoData.ie0, "ignore": True}
    )
    print(response.data)
    assert response.status_code == 422

    config.db.update(
        "records",
        {"id": ExtDemoData.record0, "status": "process-error"},
    ).eval()

    # ok
    assert (
        client.post(
            "/job/ie-plan", json={"id": ExtDemoData.ie0, "ignore": True}
        ).status_code
        == 200
    )
    assert config.db.get_row(
        "records", ExtDemoData.record0, cols=["status", "ignored"]
    ).eval() == {"status": "process-error", "ignored": True}

    config.db.update(
        "records",
        {"id": ExtDemoData.record0, "ignored": None},
    ).eval()

    # also ok
    assert (
        client.post(
            "/job/ie-plan",
            json={"id": ExtDemoData.ie0, "planAsBitstream": True},
        ).status_code
        == 200
    )
    assert config.db.get_row(
        "records",
        ExtDemoData.record0,
        cols=["status", "bitstream", "skip_object_validation"],
    ).eval() == {
        "status": "in-process",
        "bitstream": True,
        "skip_object_validation": None,
    }

    config.db.update(
        "records",
        {"id": ExtDemoData.record0, "status": "process-error"},
    ).eval()

    # also ok
    assert (
        client.post(
            "/job/ie-plan",
            json={"id": ExtDemoData.ie0, "clear": True},
        ).status_code
        == 200
    )
    assert config.db.get_row(
        "records",
        ExtDemoData.record0,
        cols=["status", "bitstream", "skip_object_validation"],
    ).eval() == {
        "status": "in-process",
        "bitstream": None,
        "skip_object_validation": None,
    }

    config.db.update(
        "records",
        {
            "id": ExtDemoData.record0,
            "status": "process-error",
            "bitstream": None,
        },
    ).eval()

    # also ok
    assert (
        client.post(
            "/job/ie-plan",
            json={"id": ExtDemoData.ie0, "planToSkipObjectValidation": True},
        ).status_code
        == 200
    )
    assert config.db.get_row(
        "records",
        ExtDemoData.record0,
        cols=["status", "bitstream", "skip_object_validation"],
    ).eval() == {
        "status": "in-process",
        "bitstream": None,
        "skip_object_validation": True,
    }

    # cannot ignore if in-process
    config.db.update(
        "jobs", {"token": util.DemoData.token1, "status": "running"}
    ).eval()
    response = client.post(
        "/job/ie-plan", json={"id": ExtDemoData.ie0, "ignore": True}
    )
    print(response.data)

    # cannot run actions if job is running
    assert response.status_code == 422
    config.db.update(
        "records", {"id": ExtDemoData.record0, "status": "process-error"}
    ).eval()
    response = client.post(
        "/job/ie-plan", json={"id": ExtDemoData.ie0, "ignore": True}
    )
    print(response.data)
    assert response.status_code == 400
