"""
Test module for the package `dcm-backend-sdk`.
"""

from time import sleep
from uuid import uuid4
from hashlib import md5
from datetime import timedelta

from flask import jsonify
import pytest
import dcm_backend_sdk
from dcm_common.util import now

from dcm_backend import app_factory, util
from dcm_backend.models import JobConfig


@pytest.fixture(name="sdk_testing_config")
def _sdk_testing_config(testing_config, temp_folder):
    testing_config.ORCHESTRATION_AT_STARTUP = True
    testing_config.SCHEDULING_AT_STARTUP = True
    testing_config.SQLITE_DB_FILE = temp_folder / str(uuid4())
    testing_config.DB_ADAPTER_STARTUP_IMMEDIATELY = True

    return testing_config


@pytest.fixture(name="app_and_db")
def _app_and_db(sdk_testing_config):
    config = sdk_testing_config()
    return {"app": app_factory(config, as_process=True), "db": config.db}


@pytest.fixture(name="default_sdk", scope="module")
def _default_sdk():
    return dcm_backend_sdk.DefaultApi(
        dcm_backend_sdk.ApiClient(
            dcm_backend_sdk.Configuration(
                host="http://localhost:8080"
            )
        )
    )


@pytest.fixture(name="ingest_sdk", scope="module")
def _ingest_sdk():
    return dcm_backend_sdk.IngestApi(
        dcm_backend_sdk.ApiClient(
            dcm_backend_sdk.Configuration(
                host="http://localhost:8080"
            )
        )
    )


@pytest.fixture(name="config_sdk", scope="module")
def _config_sdk():
    return dcm_backend_sdk.ConfigApi(
        dcm_backend_sdk.ApiClient(
            dcm_backend_sdk.Configuration(
                host="http://localhost:8080"
            )
        )
    )


@pytest.fixture(name="job_sdk", scope="module")
def _job_sdk():
    return dcm_backend_sdk.JobApi(
        dcm_backend_sdk.ApiClient(
            dcm_backend_sdk.Configuration(
                host="http://localhost:8080"
            )
        )
    )


@pytest.fixture(name="user_sdk", scope="module")
def _user_sdk():
    return dcm_backend_sdk.UserApi(
        dcm_backend_sdk.ApiClient(
            dcm_backend_sdk.Configuration(
                host="http://localhost:8080"
            )
        )
    )


def test_default_ping(
    default_sdk: dcm_backend_sdk.DefaultApi, app_and_db, run_service
):
    """Test default endpoint `/ping-GET`."""

    run_service(app_and_db["app"], probing_path="ready")

    response = default_sdk.ping()

    assert response == "pong"


def test_default_status(
    default_sdk: dcm_backend_sdk.DefaultApi, app_and_db, run_service
):
    """Test default endpoint `/status-GET`."""

    run_service(app_and_db["app"], probing_path="ready")

    response = default_sdk.get_status()

    assert response.ready


def test_default_identify(
    default_sdk: dcm_backend_sdk.DefaultApi, app_and_db, run_service,
    sdk_testing_config
):
    """Test default endpoint `/identify-GET`."""

    run_service(app_and_db["app"], probing_path="ready")

    response = default_sdk.identify()

    assert (
        response.to_dict() == sdk_testing_config().CONTAINER_SELF_DESCRIPTION
    )


def test_ingest_report(
    ingest_sdk: dcm_backend_sdk.IngestApi,
    app_and_db,
    run_service,
    minimal_request_body,
    rosetta_stub,
):
    """Test endpoints `/ingest-POST` and `/report-GET`."""

    # run dummy Rosetta instance
    run_service(rosetta_stub, port=5050)
    # run backend
    run_service(app_and_db["app"], port=8080, probing_path="ready")

    submission = ingest_sdk.ingest(minimal_request_body)

    while True:
        try:
            report = ingest_sdk.get_report(token=submission.value)
            break
        except dcm_backend_sdk.exceptions.ApiException as e:
            assert e.status == 503
            sleep(0.1)

    report = ingest_sdk.get_report(token=submission.value)
    assert report.data.success
    assert isinstance(
        report.data.details.deposit.get("id"), str
    )
    assert (
        report.data.details.deposit.get("status")
        == "INPROCESS"
    )

    status = ingest_sdk.ingest_status(
        archive_id=minimal_request_body["ingest"]["archiveId"],
        deposit_id=report.data.details.deposit.get("id"),
    )
    assert status.success
    assert status.details.deposit.get(
        "id"
    ) == report.data.details.deposit.get("id")
    assert status.details.sip.get(
        "id"
    ) == report.data.details.deposit.get("sip_id")


def test_ingest_report_404(
    ingest_sdk: dcm_backend_sdk.IngestApi, app_and_db, run_service
):
    """Test ingest endpoint `/report-GET` without previous submission."""

    run_service(app_and_db["app"], probing_path="ready")

    with pytest.raises(dcm_backend_sdk.rest.ApiException) as exc_info:
        ingest_sdk.get_report(token="some-token")
    assert exc_info.value.status == 404


def test_job_configure(config_sdk: dcm_backend_sdk.ConfigApi, app_and_db, run_service):
    """Test `/job/configure`-endpoints."""

    run_service(app_and_db["app"], probing_path="ready")

    token = {"value": "abcdef-123", "expires": False}
    run_service(
        routes=[("/process", lambda: (jsonify(token), 201), ["POST"]),],
        port=8087
    )

    assert config_sdk.list_job_configs(
        template_id=util.DemoData.template1
    ) == [util.DemoData.job_config1]

    job_config = {
        "status": "ok",
        "templateId": util.DemoData.template1,
        "name": "Demo-Job 1",
        "contactInfo": "einstein@lzv.nrw",
        "dataProcessing": {
            "mapping": {
                "type": "plugin",
                "data": {"plugin": "demo", "args": {}},
            },
        },
        "userCreated": util.DemoData.user0,
        "datetimeCreated": now().isoformat(),
    }
    config_info = config_sdk.set_job_config(job_config)
    config = config_sdk.get_job_config(config_info.id).to_dict()
    assert config == (
        job_config
        | config_info.to_dict()
        | {
            "workspaceId": util.DemoData.workspace1,
        }
    )

    assert sorted(config_sdk.list_job_configs()) == sorted(
        [v for k, v in util.DemoData.__dict__.items() if "job" in k]
        + [config_info.id]
    )

    # update job
    new_description = "Job Test"
    assert (
        "description"
        not in config_sdk.get_job_config(config_info.id).to_dict()
    )
    config_sdk.update_job_config(
        job_config
        | {
            "id": config["id"],
            "description": new_description,
            "userModified": util.DemoData.user1,
            "datetimeModified": now().isoformat(),
        }
    )
    assert (
        config_sdk.get_job_config(config_info.id).to_dict()["description"]
        == new_description
    )

    assert sorted(config_sdk.list_job_configs()) == sorted(
        [v for k, v in util.DemoData.__dict__.items() if "job" in k]
        + [config_info.id]
    )

    config_sdk.delete_job_config(config_info.id)
    assert sorted(config_sdk.list_job_configs()) == sorted(
        [v for k, v in util.DemoData.__dict__.items() if "job" in k]
    )


def test_job(
    config_sdk: dcm_backend_sdk.ConfigApi, job_sdk: dcm_backend_sdk.JobApi,
    app_and_db, run_service
):
    """Test `/job`-endpoints."""

    run_service(app_and_db["app"], probing_path="ready")

    minimal_config = {
        "id": str(uuid4()),
        "templateId": util.DemoData.template1,
        "status": "ok",
        "name": "some config",
    }
    token = {"value": str(uuid4()), "expires": False}
    report = {
        "host": "job-processor",
        "token": token,
        "args": {},
        "progress": {
            "status": "completed", "verbose": "-", "numeric": 100
        },
    }
    minimal_job = {
        "token": token["value"],
        "job_config_id": minimal_config["id"],
        "datetime_triggered": "2024-01-01T00:00:00+01:00",
        "trigger_type": "manual",
        "status": report["progress"]["status"],
        "success": True,
        "datetime_started": "2024-01-01T00:00:00.000000+01:00",
        "datetime_ended": "2024-01-01T00:01:00.000000+01:00",
        "report": report,
    }
    def _process():
        app_and_db["db"].insert("jobs", minimal_job).eval()
        return jsonify(token), 201
    run_service(
        routes=[("/process", _process, ["POST"]),], port=8087
    )

    # post job
    config_info = config_sdk.set_job_config(minimal_config)
    assert config_info.id == minimal_config["id"]
    assert config_info.id in job_sdk.list_job_configs()
    assert (
        JobConfig.from_row(
            app_and_db["db"].get_row("job_configs", config_info.id).eval()
        ).json
        == minimal_config
    )

    job_token = job_sdk.run({"id": minimal_config["id"]})
    assert job_token.value == token["value"]
    assert any(
        job_token.value in job["token"]
        for job in app_and_db["db"].get_rows("jobs").eval()
    )

    # list jobs
    tokens = job_sdk.list_jobs()
    assert len(tokens) == 3
    assert minimal_job["token"] in tokens

    # get job
    info = job_sdk.get_job_info(minimal_job["token"])
    assert info.token == token["value"]

    # run test-job
    app_and_db["db"].delete("jobs", token["value"])
    job_token = job_sdk.run_test_job(minimal_config)
    assert any(
        job_token.value in job["token"]
        for job in app_and_db["db"].get_rows("jobs").eval()
    )


def test_configure_user(
    config_sdk: dcm_backend_sdk.ConfigApi, app_and_db, run_service
):
    """Test `/user/configure`-endpoints."""

    run_service(app_and_db["app"], probing_path="ready")

    assert config_sdk.list_users(
        group="admin"
    ) == [util.DemoData.user0]

    minimal_config = {
        "username": "new-user",
        "email": "a@b.c",
        "groups": [],
        "userCreated": util.DemoData.user0,
        "datetimeCreated": now().isoformat(),
    }
    user_id = config_sdk.create_user(minimal_config).id
    config = config_sdk.get_user_config(user_id).to_dict()
    assert config == minimal_config | {"id": user_id, "status": "inactive"}

    # update user
    config_sdk.update_user(
        minimal_config
        | {
            "id": user_id,
            "externalId": "new-user",
            "userModified": util.DemoData.user1,
            "datetimeModified": now().isoformat(),
        }
    )
    assert (
        config_sdk.get_user_config(user_id).to_dict()["externalId"]
        == "new-user"
    )

    # list users
    users = config_sdk.list_users()
    assert len(users) == 1 + len(
        [v for k, v in util.DemoData.__dict__.items() if "user" in k]
    )
    assert user_id in users

    # delete user
    config_sdk.delete_user_config(user_id)
    with pytest.raises(dcm_backend_sdk.ApiException):
        config_sdk.get_user_config(user_id)

    # get fully configured user
    config_sdk.get_user_config(util.DemoData.user0)


def test_user_login(
    user_sdk: dcm_backend_sdk.UserApi,
    app_and_db,
    run_service,
):
    """Test `POST-/user`-endpoint."""

    run_service(app_and_db["app"], probing_path="ready")

    # successful login
    assert user_sdk.login(
        {
            "username": "einstein",
            "password": md5(b"relativity").hexdigest(),
        }
    ).to_dict() == {
        "id": util.DemoData.user1,
        "externalId": "albert",
        "status": "ok",
        "username": "einstein",
        "firstname": "Albert",
        "lastname": "Einstein",
        "email": "einstein@lzv.nrw",
        "groups": [{"id": "curator", "workspace": util.DemoData.workspace1}],
    }
    # failed login
    try:
        user_sdk.login(
            {"username": "einstein", "password": "bad-pw"}
        )
    except dcm_backend_sdk.exceptions.ApiException as exc_info:
        assert exc_info.status == 401


def test_user_change_password(
    user_sdk: dcm_backend_sdk.UserApi,
    app_and_db,
    run_service,
):
    """Test `POST-/user/password`-endpoint."""

    run_service(app_and_db["app"], probing_path="ready")

    # change password
    new_password = md5(b"password").hexdigest()
    assert (
        user_sdk.change_user_password(
            {
                "username": "einstein",
                "password": md5(b"relativity").hexdigest(),
                "newPassword": new_password,
            }
        )
        == "OK"
    )
    # login with new password
    user_sdk.login({"username": "einstein", "password": new_password})


def test_configure_workspace(
    config_sdk: dcm_backend_sdk.ConfigApi, app_and_db, run_service
):
    """Test `/workspace/configure`-endpoints."""

    run_service(app_and_db["app"], probing_path="ready")

    minimal_config = {
        "name": "New Workspace",
        "userCreated": util.DemoData.user0,
        "datetimeCreated": now().isoformat(),
    }
    workspace_id = config_sdk.create_workspace(minimal_config).id
    config = config_sdk.get_workspace(workspace_id).to_dict()
    assert config == minimal_config | {
        "id": workspace_id,
        "users": [],
        "templates": [],
    }

    # update workspace
    config_sdk.update_workspace(
        minimal_config
        | {
            "id": workspace_id,
            "name": "Workspace Test",
            "userModified": util.DemoData.user1,
            "datetimeModified": now().isoformat(),
        }
    )
    assert (
        config_sdk.get_workspace(workspace_id).to_dict()["name"]
        == "Workspace Test"
    )

    # list workspaces
    workspaces = config_sdk.list_workspaces()
    assert len(workspaces) == 3
    assert workspace_id in workspaces

    # delete workspace
    config_sdk.delete_workspace(workspace_id)
    with pytest.raises(dcm_backend_sdk.ApiException):
        config_sdk.get_workspace(workspace_id)

    # get fully configured workspace
    config_sdk.get_workspace(util.DemoData.workspace1)


def test_configure_template(
    config_sdk: dcm_backend_sdk.ConfigApi, app_and_db, run_service
):
    """Test `/template/configure`-endpoints."""

    run_service(app_and_db["app"], probing_path="ready")

    minimal_config = {
        "status": "ok",
        "name": "New Template",
        "type": "hotfolder",
        "additionalInformation": {"sourceId": "some-id"},
        "userCreated": util.DemoData.user0,
        "datetimeCreated": now().isoformat(),
    }
    template_id = config_sdk.create_template(minimal_config).id
    config = config_sdk.get_template(template_id).to_dict()
    assert config == minimal_config | {"id": template_id}

    # update template
    config_sdk.update_template(
        minimal_config
        | {
            "id": template_id,
            "name": "Template Test",
            "userModified": util.DemoData.user1,
            "datetimeModified": now().isoformat(),
        }
    )
    assert (
        config_sdk.get_template(template_id).to_dict()["name"]
        == "Template Test"
    )

    # list templates
    templates = config_sdk.list_templates()
    assert len(templates) == 4
    assert template_id in templates

    # delete template
    config_sdk.delete_template(template_id)
    with pytest.raises(dcm_backend_sdk.ApiException):
        config_sdk.get_template(template_id)

    # get fully configured workspace
    config_sdk.get_template(util.DemoData.template1)


def test_hotfolder_sources(
    config_sdk: dcm_backend_sdk.ConfigApi,
    run_service,
    app_and_db,
):
    """Test `/template/hotfolder-sources`-endpoint."""

    run_service(app_and_db["app"], probing_path="ready")
    response = config_sdk.get_hotfolder_sources()
    assert sorted(
        [src.model_dump(exclude_none=True)["id"] for src in response]
    ) == sorted(
        [v for k, v in util.DemoData.__dict__.items() if "hotfolder" in k]
    )
