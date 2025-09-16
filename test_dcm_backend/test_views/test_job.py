"""Test-module for job-endpoint."""

from uuid import uuid4
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
    response = client.get(f"/job?token={util.DemoData.token1}")

    assert response.status_code == 200
    assert response.mimetype == "application/json"


def test_get_keys(no_orchestra_testing_config):
    """
    Test endpoint `GET-/job` of job-API with `keys` query parameter.
    """
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()

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
    assert all(
        key in job_info for key in ["templateId", "workspaceId", "records"]
    )

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
        "records",
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
    temp_folder,
    run_service,
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

    assert (temp_folder / file).exists()
    context = json.loads((temp_folder / file).read_text(encoding="utf-8"))[
        "context"
    ]
    assert context["jobConfigId"] == minimal_config["id"]
    assert context["userTriggered"] == util.DemoData.user0
    assert "datetimeTriggered" in context
    assert context["triggerType"] == TriggerType.MANUAL.value

    assert (
        config.db.get_row("job_configs", minimal_config["id"]).eval()[
            "latest_exec"
        ]
        == token["value"]
    )


def test_post_test(
    no_orchestra_testing_config,
    minimal_info,
    temp_folder,
    minimal_config,
    run_service,
):
    """
    Test endpoint `POST-/job/configure/test` of job-API.
    """
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

    assert (temp_folder / file).exists()
    assert (
        json.loads((temp_folder / file).read_text(encoding="utf-8"))[
            "process"
        ]["to"]
        == "build_sip"
    )


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
    [
        (  # query without params
            [f"INSERT INTO jobs (token) VALUES ('{util.DemoData.token1}')"],
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
    ],
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


@pytest.mark.parametrize(
    ("init_cmds", "query", "nexpected", "status"),
    [
        (  # query without params
            [],
            "",
            None,
            400,
        ),
        (  # query for job config id empty
            [
                f"INSERT INTO records (id, job_token, success, report_id) VALUES ('{util.DemoData.record1}', '{util.DemoData.token1}', true, '<report-id>')"
            ],
            f"?id={uuid4()}",
            0,
            200,
        ),
        (  # query for job config id non-empty
            [
                f"INSERT INTO records (id, job_token, success, report_id) VALUES ('{util.DemoData.record1}', '{util.DemoData.token1}', true, '<report-id>')"
            ],
            f"?id={util.DemoData.job_config1}",
            1,
            200,
        ),
        (  # sql injection via id
            [],
            f"?id={util.DemoData.job_config1}'",
            None,
            422,
        ),
        (  # query for job token empty
            [
                f"INSERT INTO records (id, job_token, success, report_id) VALUES ('{util.DemoData.record1}', '{util.DemoData.token1}', true, '<report-id>')"
            ],
            f"?token={str(uuid4())}",
            0,
            200,
        ),
        (  # query for job token non-empty
            [
                f"INSERT INTO records (id, job_token, success, report_id) VALUES ('{util.DemoData.record1}', '{util.DemoData.token1}', true, '<report-id>')"
            ],
            f"?token={util.DemoData.token1}",
            1,
            200,
        ),
        (  # sql injection via token
            [],
            f"?token={util.DemoData.token1}'",
            None,
            422,
        ),
        (  # query by success empty
            [
                f"INSERT INTO records (id, job_token, success, report_id) VALUES ('{util.DemoData.record1}', '{util.DemoData.token1}', true, '<report-id>')"
            ],
            f"?token={util.DemoData.token1}&success=false",
            0,
            200,
        ),
        (  # query by success non-empty
            [
                f"INSERT INTO records (id, job_token, success, report_id) VALUES ('{util.DemoData.record1}', '{util.DemoData.token1}', true, '<report-id>')"
            ],
            f"?token={util.DemoData.token1}&success=true",
            1,
            200,
        ),
        (  # sql injection via success (caught by handler)
            [],
            "?success=true'",
            None,
            422,
        ),
    ],
)
def test_get_records(
    no_orchestra_testing_config, init_cmds, query, nexpected, status
):
    """Test endpoint `GET-/job/records` of job-API."""
    config = no_orchestra_testing_config()
    client = app_factory(config, block=True).test_client()

    config.db.custom_cmd(
        "DELETE FROM records", clear_schema_cache=False
    ).eval()
    for cmd in init_cmds:
        config.db.custom_cmd(cmd, clear_schema_cache=False).eval()

    response = client.get(f"/job/records{query}")
    assert response.status_code == status
    if status == 200:
        assert len(response.json) == nexpected
