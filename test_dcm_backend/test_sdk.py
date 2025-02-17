"""
Test module for the package `dcm-backend-sdk`.
"""

from time import sleep
from uuid import uuid4
from hashlib import md5

from flask import jsonify
import pytest
from dcm_common.db import JSONFileStore
import dcm_backend_sdk

from dcm_backend import app_factory


@pytest.fixture(name="sdk_job_tests_folder", scope="module")
def _sdk_job_tests_folder(temp_folder):
    return temp_folder / str(uuid4())


@pytest.fixture(name="sdk_testing_config")
def _sdk_testing_config(testing_config, sdk_job_tests_folder):
    testing_config.ORCHESTRATION_AT_STARTUP = True
    testing_config.SCHEDULING_AT_STARTUP = True
    testing_config.REPORT_DATABASE_ADAPTER = "native"
    testing_config.REPORT_DATABASE_SETTINGS = {
        "backend": "disk", "dir": str(sdk_job_tests_folder)
    }
    testing_config.CREATE_DEMO_USERS = True
    return testing_config


@pytest.fixture(name="app")
def _app(sdk_testing_config):
    return app_factory(sdk_testing_config(), as_process=True)


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
    default_sdk: dcm_backend_sdk.DefaultApi, app, run_service
):
    """Test default endpoint `/ping-GET`."""

    run_service(app)

    response = default_sdk.ping()

    assert response == "pong"


def test_default_status(
    default_sdk: dcm_backend_sdk.DefaultApi, app, run_service
):
    """Test default endpoint `/status-GET`."""

    run_service(app)

    response = default_sdk.get_status()

    assert response.ready


def test_default_identify(
    default_sdk: dcm_backend_sdk.DefaultApi, app, run_service,
    sdk_testing_config
):
    """Test default endpoint `/identify-GET`."""

    run_service(app)

    response = default_sdk.identify()

    assert (
        response.to_dict() == sdk_testing_config().CONTAINER_SELF_DESCRIPTION
    )


def test_ingest_report(
    ingest_sdk: dcm_backend_sdk.IngestApi,
    app, run_service,
    minimal_request_body,
    run_rosetta_dummy
):
    """Test endpoints `/ingest-POST` and `/report-GET`."""

    # run dummy Rosetta instance
    run_rosetta_dummy()

    run_service(app, port=8080)

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
    assert isinstance(report.data.deposit.id, str)
    assert report.data.deposit.status == "INPROCESS"

    status = ingest_sdk.deposit_status(report.data.deposit.id)
    assert status.deposit.id == report.data.deposit.id


def test_ingest_report_404(
    ingest_sdk: dcm_backend_sdk.IngestApi, app, run_service
):
    """Test ingest endpoint `/report-GET` without previous submission."""

    run_service(app)

    with pytest.raises(dcm_backend_sdk.rest.ApiException) as exc_info:
        ingest_sdk.get_report(token="some-token")
    assert exc_info.value.status == 404


def test_configure(config_sdk: dcm_backend_sdk.ConfigApi, app, run_service):
    """Test `/job/configure`-endpoints."""

    run_service(app)

    token = {"value": "abcdef-123", "expires": False}
    run_service(
        routes=[("/process", lambda: (jsonify(token), 201), ["POST"]),],
        port=8086
    )

    minimal_config = {
        "job": {"from": "import_ies", "args": {}},
        "schedule": {
            "active": True,
            "repeat": {"unit": "second", "interval": 1}
        }
    }
    config_info = config_sdk.set_job_config(minimal_config)
    config = config_sdk.get_job_config(config_info.id).to_dict()
    assert config == (
        minimal_config
        | config_info.to_dict()
        | {"last_modified": config["last_modified"]}
    )

    assert config_sdk.list_job_configs() == [config_info.id]

    config_sdk.delete_job_config(config_info.id)
    assert config_sdk.list_job_configs() == []


def test_job(
    config_sdk: dcm_backend_sdk.ConfigApi, job_sdk: dcm_backend_sdk.JobApi,
    sdk_job_tests_folder, app, run_service
):
    """Test `/job`-endpoints."""

    run_service(app)

    token = {"value": "abcdef-123", "expires": False}
    minimal_config = {
        "id": str(uuid4()),
        "job": {"from": "import_ies", "args": {}},
    }
    db = JSONFileStore(sdk_job_tests_folder)
    def _process():
        db.write(
            token["value"], {
                "config": {
                    "original_body": {"process": {}, "id": minimal_config["id"]}
                },
                "report": {
                    "host": "job-processor",
                    "token": token,
                    "args": {},
                    "progress": {
                        "status": "completed", "verbose": "-", "numeric": 100
                    },
                },
                "token": token,
                "metadata": {
                    "produced": {
                        "by": "job-processor",
                        "datetime": "2024-01-01T00:00:00.000000+01:00"
                    }
                }
            }
        )
        return jsonify(token), 201
    run_service(
        routes=[("/process", _process, ["POST"]),], port=8086
    )

    # post job
    config_info = config_sdk.set_job_config(minimal_config)
    assert config_info.id == minimal_config["id"]
    job_token = job_sdk.run({"id": minimal_config["id"]})
    assert job_token.value == token["value"]
    assert db.keys() == (job_token.value,)

    # list jobs
    jobs = job_sdk.list_jobs()
    assert len(jobs) == 1
    assert jobs[0].id == minimal_config["id"]
    assert jobs[0].token.to_dict() == token

    # get job
    info = job_sdk.get_job_info(token["value"])
    assert info.token.to_dict() == token
    assert info.report.token.to_dict() == token


def test_configure_user(
    config_sdk: dcm_backend_sdk.ConfigApi, app, run_service
):
    """Test `/user/configure`-endpoints."""

    run_service(app)

    minimal_config = {
        "userId": "user0",
        "externalId": "-",
        "roles": []
    }
    config_sdk.create_user(minimal_config)
    config = config_sdk.get_user_config("user0").to_dict()
    assert config == minimal_config

    # update user
    config_sdk.update_user(minimal_config | {"externalId": "user1"})
    assert config_sdk.get_user_config("user0").to_dict()["externalId"] == "user1"

    # list users
    users = config_sdk.list_users()
    assert len(users) == 3
    assert minimal_config["userId"] in users

    # delete user
    config_sdk.delete_user_config("user0")
    with pytest.raises(dcm_backend_sdk.ApiException):
        config_sdk.get_user_config("user0")


def test_user_login(
    user_sdk: dcm_backend_sdk.UserApi,
    app,
    run_service,
):
    """Test `POST-/user`-endpoint."""

    run_service(app)

    # successful login
    assert (
        user_sdk.login(
            {
                "userId": "Einstein",
                "password": md5(b"relativity").hexdigest(),
            }
        )
        == "OK"
    )
    # failed login
    try:
        user_sdk.login(
            {"userId": "Einstein", "password": "bad-pw"}
        )
    except dcm_backend_sdk.exceptions.ApiException as exc_info:
        assert exc_info.status == 401


def test_user_change_password(
    user_sdk: dcm_backend_sdk.UserApi,
    app,
    run_service,
):
    """Test `POST-/user/password`-endpoint."""

    run_service(app)

    # change password
    new_password = md5(b"password").hexdigest()
    assert (
        user_sdk.change_user_password(
            {
                "userId": "Einstein",
                "password": md5(b"relativity").hexdigest(),
                "newPassword": new_password,
            }
        )
        == "OK"
    )
    # login with new password
    user_sdk.login({"userId": "Einstein", "password": new_password})
