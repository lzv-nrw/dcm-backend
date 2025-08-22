"""Test-module for configuration-endpoint."""

from uuid import uuid4
from time import time, sleep
from datetime import datetime, timedelta
from copy import deepcopy

from flask import jsonify
import pytest
from dcm_common.util import now

from dcm_backend import app_factory, util
from dcm_backend.models import (
    UserConfig,
    WorkspaceConfig,
    TemplateConfig,
    JobConfig,
)


@pytest.fixture(name="minimal_job_config")
def _minimal_job_config():
    return {
        "id": "dab3e1bf-f655-4e57-938d-d6953612552b",
        "templateId": util.DemoData.template1,
        "status": "ok",
        "name": "some config",
    }


@pytest.fixture(name="minimal_user_config")
def _minimal_user_config():
    return {
        "username": "new-user",
        "email": "a@b.c",
    }


@pytest.fixture(name="minimal_workspace_config")
def _minimal_workspace_config():
    return {
        "name": "New Workspace",
    }


@pytest.fixture(name="minimal_template_config")
def _minimal_template_config():
    return {
        "status": "ok",
        "name": "New Template",
        "type": "hotfolder",
        "additionalInformation": {"sourceId": "some-id"}
    }


@pytest.fixture(name="client_and_db")
def _client_and_db(testing_config):
    config = testing_config()
    return (
        app_factory(config, block=True).test_client(),
        config.db,
    )


def test_get_job(client_and_db):
    """Test endpoint `GET-/job/configure` of config-API."""
    client, db = client_and_db
    response = client.get(f"/job/configure?id={util.DemoData.job_config1}")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == JobConfig.from_row(
        db.get_row("job_configs", util.DemoData.job_config1).eval()
    ).json | {"workspaceId": util.DemoData.workspace1}


def test_get_job_no_workspace(client_and_db):
    """
    Test endpoint `GET-/job/configure` of config-API for a job config
    associated to a template with no associated workspace.
    """
    client, db = client_and_db
    response = client.get(f"/job/configure?id={util.DemoData.job_config3}")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert "workspace_id" not in response.json
    assert response.json == JobConfig.from_row(
        db.get_row("job_configs", util.DemoData.job_config3).eval()
    ).json


def test_get_job_unknown(client_and_db):
    """Test endpoint `GET-/job/configure` of config-API with unknown id."""
    client, db = client_and_db
    unknown_id = str(uuid4())
    assert unknown_id not in [
        row["id"]
        for row in db.get_rows(table="job_configs", cols=["id"]).eval()
    ]
    response = client.get(f"/job/configure?id={unknown_id}")

    assert response.status_code == 404
    assert response.mimetype == "text/plain"


def test_post_job_config(client_and_db, minimal_job_config):
    """Test endpoint `POST-/job/configure` of config-API."""
    client, db = client_and_db
    response = client.post("/job/configure", json=minimal_job_config)

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == {"id": minimal_job_config["id"]}

    config_db = JobConfig.from_row(
        db.get_row("job_configs", minimal_job_config["id"]).eval()
    ).json
    assert minimal_job_config == config_db


def test_post_job_config_no_id(client_and_db, minimal_job_config):
    """
    Test endpoint `POST-/job/configure` of config-API missing `id`.
    """
    client, db = client_and_db

    job_config_no_id = deepcopy(minimal_job_config)
    del job_config_no_id["id"]
    json = client.post(
        "/job/configure",
        json=job_config_no_id,
    ).json

    db_json = db.get_row("job_configs", json["id"]).eval()
    assert db_json["id"] == json["id"]


def test_post_job_config_conflict(client_and_db, minimal_job_config):
    """
    Test endpoint `POST-/job/configure` of config-API with an existing `id`.
    """
    client, db = client_and_db
    assert util.DemoData.job_config1 in db.get_column("job_configs", "id").eval()

    job_config_existing_id = deepcopy(minimal_job_config)
    job_config_existing_id["id"] = util.DemoData.job_config1
    assert client.post(
        "/job/configure",
        json=job_config_existing_id,
    ).status_code == 409


def test_post_job_with_scheduling(client_and_db):
    """
    Test endpoint `POST-/job/configure` of config-API with scheduled job.
    """
    client, _ = client_and_db
    start = now() + timedelta(days=1)
    json = client.post(
        "/job/configure",
        json={
            "schedule": {
                "active": True,
                "start": start.isoformat(),
                "repeat": {"unit": "week", "interval": 1},
            },
            "templateId": util.DemoData.template1,
            "status": "ok",
            "name": "some config",
        },
    ).json

    # add
    scheduled = client.get("/schedule").json["scheduled"]
    assert len(scheduled) == 1
    assert json["id"] == scheduled[0]["jobConfig"]

    assert (
        datetime.fromisoformat(
            client.get(f"/job/configure?id={json['id']}").json["scheduledExec"]
        )
        == start
    )

    # remove scheduling
    assert client.put(
        "/job/configure",
        json={
            "id": json["id"],
            "schedule": {
                "active": False,
                "start": (now() + timedelta(days=1)).isoformat(),
                "repeat": {"unit": "week", "interval": 1},
            },
            "status": "ok",
            "templateId": util.DemoData.template1,
            "name": "some config",
        },
    ).status_code == 200
    assert len(client.get("/schedule").json["scheduled"]) == 0

    assert (
        "scheduledExec"
        not in client.get(f"/job/configure?id={json['id']}").json
    )


def test_post_job_with_scheduling_and_execution(
    client_and_db, run_service, temp_folder
):
    """
    Test endpoint `POST-/job/configure` of config-API with scheduled job
    and wait for execution.
    """

    token = {"value": str(uuid4()), "expires": False}
    file = str(uuid4())
    assert not (temp_folder / file).exists()

    def _process():
        (temp_folder / file).touch()
        return jsonify(token), 201

    run_service(
        routes=[
            ("/process", _process, ["POST"]),
        ],
        port=8087,
    )
    client, db = client_and_db
    response = client.post(
        "/job/configure",
        json={
            "schedule": {
                "active": True,
                "start": now().isoformat(),
            },
            "status": "ok",
            "templateId": util.DemoData.template1,
            "name": "some config",
        },
    )

    time0 = time()
    while not (temp_folder / file).exists() and time() - time0 < 3:
        sleep(0.01)

    assert (temp_folder / file).exists()

    assert (
        db.get_row("job_configs", response.json["id"]).eval()["latest_exec"]
        == token["value"]
    )


def test_put_job(client_and_db):
    """
    Test endpoint `PUT-/job/configure` of config-API for selective
    changes in the config.
    """
    client, db = client_and_db
    job_config0 = db.get_row("job_configs", util.DemoData.job_config1).eval()

    del job_config0["user_created"]
    del job_config0["datetime_created"]
    # write 'latest_exec'
    # to check that updating the job-config does not delete it
    token_value = str(uuid4())
    db.update(
        "job_configs",
        {"id": util.DemoData.job_config1, "latest_exec": token_value},
    )

    assert (
        client.put(
            "/job/configure",
            json=JobConfig.from_row(job_config0).json | {"name": "test"}
        ).status_code
        == 200
    )

    updated_job_config = db.get_row(
        "job_configs", util.DemoData.job_config1
    ).eval()
    assert updated_job_config["name"] == "test"
    assert updated_job_config["latest_exec"] == token_value
    client.delete("/schedule")


def test_put_job_not_found(client_and_db, minimal_job_config):
    """
    Test endpoint `PUT-/job/configure` of config-API for non-existing
    job config.
    """
    client, _ = client_and_db

    response = client.put(
        "/job/configure",
        json=minimal_job_config | {"id": str(uuid4())},
    )

    assert response.status_code == 404
    assert response.mimetype == "text/plain"


def test_job_options(client_and_db):
    """Test endpoint `OPTIONS-/job/configure` of config-API."""
    client, _ = client_and_db
    response = client.options("/job/configure")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert sorted(response.json) == sorted(
        [v for k, v in util.DemoData.__dict__.items() if "job" in k]
    )


def test_job_options_by_template(client_and_db):
    """Test endpoint `OPTIONS-/job/configure` of config-API."""
    client, _ = client_and_db

    template1_jobs = client.options(
        f"/job/configure?templateId={util.DemoData.template1}"
    ).json
    assert len(template1_jobs) == 1
    assert util.DemoData.job_config1 in template1_jobs

    template2_jobs = client.options(
        f"/job/configure?templateId={util.DemoData.template2}"
    ).json
    assert len(template2_jobs) == 1
    assert util.DemoData.job_config2 in template2_jobs


@pytest.mark.parametrize(
    ("job_config_id"),
    (pytest_args := [
        (  # job-config referenced by a job
            util.DemoData.job_config1
        ),
        (  # job-config not referenced by a job
            util.DemoData.job_config3
        ),
    ]),
    ids=["with_job", "no_job"]
)
def test_delete_job_config(client_and_db, job_config_id):
    """Test endpoint `DELETE-/job/configure` of config-API."""
    client, db = client_and_db

    job_tokens = db.get_rows(
        "jobs", job_config_id, "job_config_id", ["token"]
    ).eval()

    response = client.delete(f"/job/configure?id={job_config_id}")

    assert response.status_code == 200
    assert response.mimetype == "text/plain"
    assert sorted(db.get_column("job_configs", "id").eval()) == sorted(
        [
            v
            for k, v in util.DemoData.__dict__.items()
            if "job" in k and v != job_config_id
        ]
    )

    if job_tokens:
        # ensure there is no job referencing the job_config_id
        assert not db.get_rows(
            "jobs", job_config_id, "job_config_id", ["token"]
        ).eval()


def test_delete_job_config_unknown(client_and_db):
    """
    Test endpoint `DELETE-/job/configure` of config-API for non-existing
    config.
    """
    client, db = client_and_db
    nonexistent_id = str(uuid4())
    assert nonexistent_id not in db.get_column("job_configs", "id").eval()
    response = client.delete(f"/job/configure?id={nonexistent_id}")

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
            "schedule": {
                "active": True,
                "start": (now() + timedelta(days=1)).isoformat(),
                "repeat": {"unit": "week", "interval": 1},
            },
            "templateId": util.DemoData.template1,
            "status": "ok",
            "name": "some config",
        },
    ).json["id"]

    assert len(client.get("/schedule").json["scheduled"]) == 1
    client.delete(f"/job/configure?id={id_}")
    assert len(client.get("/schedule").json["scheduled"]) == 0


def test_user_options(client_and_db):
    """Test endpoint `OPTIONS-/user/configure` of config-API."""
    client, _ = client_and_db
    response = client.options("/user/configure")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert sorted(response.json) == sorted(
        [v for k, v in util.DemoData.__dict__.items() if "user" in k]
    )


def test_user_options_by_groups(client_and_db):
    """Test endpoint `OPTIONS-/user/configure` of config-API."""
    client, _ = client_and_db

    admins = client.options("/user/configure?group=admin").json
    assert len(admins) == 1
    assert util.DemoData.user0 in admins

    curators = client.options("/user/configure?group=curator").json
    assert len(curators) == 2
    assert util.DemoData.user1 in curators
    assert util.DemoData.user2 in curators

    everyone = client.options("/user/configure?group=admin,curator").json
    assert len(everyone) == 3
    assert util.DemoData.user0 in everyone
    assert util.DemoData.user1 in everyone
    assert util.DemoData.user2 in everyone


def test_get_user(client_and_db):
    """Test endpoint `GET-/user/configure` of config-API."""
    client, db = client_and_db
    response = client.get(
        f"/user/configure?id={util.DemoData.user0}"
    )

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == UserConfig.from_row(
        db.get_row("user_configs", util.DemoData.user0).eval()
    ).json | {"groups": [{"id": "admin"}]}


def test_post_user(client_and_db, minimal_user_config):
    """Test endpoint `POST-/user/configure` of config-API."""
    client, db = client_and_db
    response = client.post("/user/configure", json=minimal_user_config)

    assert response.status_code == 200
    assert response.mimetype == "application/json"

    written_config = db.get_row("user_configs", response.json["id"]).eval()
    assert written_config["id"] == response.json["id"]
    assert written_config["email"] == minimal_user_config["email"]
    assert written_config["username"] == minimal_user_config["username"]
    assert written_config["status"] == "inactive"

    written_secrets = db.get_rows(
        "user_secrets", response.json["id"], "user_id"
    ).eval()
    assert len(written_secrets) == 1


@pytest.mark.parametrize(
    ("groups", "expected_status_code"),
    [
        (
            [],
            200,
        ),
        (
            [
                {"id": "admin"}
            ],
            200,
        ),
        (
            [
                {"id": "curator", "workspace": util.DemoData.workspace1}
            ],
            200,
        ),
        (
            [
                {"id": "curator", "workspace": util.DemoData.workspace1},
                {"id": "admin"}
            ],
            200,
        ),
        (
            [
                {"id": "curator", "workspace": str(uuid4())},
            ],
            400,
        ),
    ],
    ids=[
        "empty-list",
        "admin",
        "curator",
        "admin_curator",
        "unknown-workspace",
    ],
)
def test_post_user_with_groups(
    client_and_db, minimal_user_config, groups, expected_status_code
):
    """
    Test endpoint `POST-/user/configure` of config-API
    for a user with group memberships.
    """
    client, db = client_and_db
    initial_users = db.get_rows("user_configs").eval()
    initial_user_groups = db.get_rows("user_groups").eval()

    minimal_user_config["groups"] = groups

    response = client.post("/user/configure", json=minimal_user_config)

    assert response.status_code == expected_status_code

    final_user_groups = db.get_rows("user_groups").eval()

    if expected_status_code == 200:
        # success
        assert response.mimetype == "application/json"
        # check db
        # 'user_configs'
        assert (
            len(db.get_rows("user_configs").eval()) == len(initial_users) + 1
        )
        written_config = db.get_row("user_configs", response.json["id"]).eval()
        assert written_config["id"] == response.json["id"]
        assert written_config["email"] == minimal_user_config["email"]
        assert written_config["username"] == minimal_user_config["username"]
        assert written_config["status"] == "inactive"
        # 'user_groups' contains all initial groups
        assert all(g in final_user_groups for g in initial_user_groups)
        # 'user_groups' contains rows for the new user
        assert len(final_user_groups) == len(initial_user_groups) + len(groups)
        # drop id to simplify assertion
        for g in final_user_groups:
            del g["id"]
        assert all(
            {"group_id": g["id"], "user_id": response.json["id"]}
            | (
                {"workspace_id": g["workspace"]}
                if "workspace" in g
                else {"workspace_id": None}
            )
            in final_user_groups
            for g in groups
        )
    elif expected_status_code == 400:
        # no success
        assert response.mimetype == "text/plain"
        # 'user_configs' contains only the initial users
        assert db.get_rows("user_configs").eval() == initial_users
        # 'user_groups' remains unchanged (contains only the initial groups)
        assert sorted(final_user_groups, key=lambda d: d["id"]) == sorted(
            initial_user_groups, key=lambda d: d["id"]
        )


def test_post_user_conflict(client_and_db, minimal_user_config):
    """
    Test endpoint `POST-/user/configure` of config-API for existing
    user.
    """
    client, _ = client_and_db

    # username-conflict
    response = client.post(
        "/user/configure", json=minimal_user_config | {"username": "admin"}
    )

    assert response.status_code == 409
    assert response.mimetype == "text/plain"

    # id-conflict
    response = client.post(
        "/user/configure",
        json=minimal_user_config | {"id": util.DemoData.user0},
    )

    assert response.status_code == 409
    assert response.mimetype == "text/plain"


def test_put_user(client_and_db):
    """
    Test endpoint `PUT-/user/configure` of config-API.
    """
    client, db = client_and_db
    user_config0 = db.get_row("user_configs", util.DemoData.user0).eval()
    assert (
        client.put(
            "/user/configure",
            json=UserConfig.from_row(user_config0 | {"email": "a@b.c"}).json,
        ).status_code
        == 200
    )
    user_config1 = db.get_row("user_configs", util.DemoData.user0).eval()
    assert user_config0 != user_config1
    assert user_config1["email"] == "a@b.c"


def test_put_user_deleted(client_and_db):
    """
    Test endpoint `PUT-/user/configure` of config-API.
    """
    client, db = client_and_db
    user_config0 = db.get_row("user_configs", util.DemoData.user0).eval()
    user_secret = db.get_rows(
        "user_secrets", util.DemoData.user0, "user_id"
    ).eval()
    assert len(user_secret) == 1

    assert (
        client.put(
            "/user/configure",
            json={
                "id": user_config0["id"],
                "username": "admin",
                "status": "deleted",
            },
        ).status_code
        == 200
    )
    user_secret = db.get_rows(
        "user_secrets", util.DemoData.user0, "user_id"
    ).eval()
    assert len(user_secret) == 0


@pytest.mark.parametrize(
    ("groups", "expected_status_code"),
    [
        (
            [],
            200,
        ),
        (
            [
                {"id": "admin"}
            ],
            200,
        ),
        (
            [
                {"id": "curator", "workspace": util.DemoData.workspace1}
            ],
            200,
        ),
        (
            [
                {"id": "curator", "workspace": util.DemoData.workspace1},
                {"id": "admin"}
            ],
            200,
        ),
        (
            [
                {"id": "curator", "workspace": str(uuid4())},
            ],
            400,
        ),
    ],
    ids=[
        "empty-list",
        "admin",
        "curator",
        "admin_curator",
        "unknown-workspace",
    ],
)
def test_put_user_with_groups(
    client_and_db, groups, expected_status_code
):
    """
    Test endpoint `PUT-/user/configure` of config-API
    for a user with group memberships.
    """
    client, db = client_and_db
    user_config_id = util.DemoData.user1

    # get initial groups for the user
    initial_groups = (
        client.get(f"/user/configure?id={user_config_id}").json["groups"]
    )

    # modify groups in the user_config
    user_config0 = db.get_row("user_configs", user_config_id).eval()
    del user_config0["user_created"]
    del user_config0["datetime_created"]
    response = client.put(
        "/user/configure",
        json=UserConfig.from_row(user_config0).json | {"groups": groups},
    )
    assert response.status_code == expected_status_code
    final_groups = (
        client.get(f"/user/configure?id={user_config_id}").json["groups"]
    )

    if expected_status_code == 200:
        # success
        assert response.mimetype == "text/plain"
        assert response.text == "OK"
        assert sorted(final_groups, key=lambda d: d["id"]) == sorted(
            groups, key=lambda d: d["id"]
        )
    elif expected_status_code == 400:
        # no success
        assert response.mimetype == "text/plain"
        # groups remained unchanged
        assert final_groups == initial_groups


def test_put_user_not_found(client_and_db, minimal_user_config):
    """
    Test endpoint `PUT-/user/configure` of config-API for non-existing
    user.
    """
    client, _ = client_and_db

    response = client.put(
        "/user/configure", json=minimal_user_config | {"id": str(uuid4())}
    )

    assert response.status_code == 404
    assert response.mimetype == "text/plain"


def test_delete_user(client_and_db):
    """Test endpoint `DELETE-/user/configure` of config-API."""
    client, db = client_and_db
    assert db.get_row("user_configs", util.DemoData.user0).eval() is not None
    assert len(
        db.get_rows("user_groups", util.DemoData.user0, "user_id").eval()
    ) == 1
    assert len(
        db.get_rows("user_secrets", util.DemoData.user0, "user_id").eval()
    ) == 1

    # delete user
    assert (
        client.delete("/user/configure?id=" + util.DemoData.user0).status_code
        == 200
    )
    # can be repeated and still get 200
    assert (
        client.delete("/user/configure?id=" + util.DemoData.user0).status_code
        == 200
    )
    assert db.get_row("user_configs", util.DemoData.user0).eval() is None
    assert len(
        db.get_rows("user_groups", util.DemoData.user0, "user_id").eval()
    ) == 0
    assert len(
        db.get_rows("user_secrets", util.DemoData.user0, "user_id").eval()
    ) == 0


def test_workspace_options(client_and_db):
    """Test endpoint `OPTIONS-/workspace/configure` of config-API."""
    client, _ = client_and_db

    response = client.options("/workspace/configure")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert sorted(response.json) == sorted([
        util.DemoData.workspace1,
        util.DemoData.workspace2,
    ])


def test_get_workspace(client_and_db):
    """Test endpoint `GET-/workspace/configure` of config-API."""
    client, db = client_and_db
    response = client.get(
        f"/workspace/configure?id={util.DemoData.workspace1}"
    )

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == WorkspaceConfig.from_row(
        db.get_row("workspaces", util.DemoData.workspace1).eval()
    ).json | {
        "users": [util.DemoData.user1],
        "templates": [util.DemoData.template1],
    }


def test_post_workspace(client_and_db, minimal_workspace_config):
    """Test endpoint `POST-/workspace/configure` of config-API."""
    client, db = client_and_db
    response = client.post(
        "/workspace/configure", json=minimal_workspace_config
    )

    assert response.status_code == 200
    assert response.mimetype == "application/json"

    written_config = db.get_row("workspaces", response.json["id"]).eval()
    assert written_config["id"] == response.json["id"]
    assert written_config["name"] == minimal_workspace_config["name"]


def test_post_workspace_conflict(client_and_db, minimal_workspace_config):
    """
    Test endpoint `POST-/workspace/configure` of config-API for existing
    workspace.
    """
    client, _ = client_and_db
    response = client.post(
        "/workspace/configure",
        json=minimal_workspace_config | {"id": util.DemoData.workspace1},
    )

    assert response.status_code == 409
    assert response.mimetype == "text/plain"


def test_put_workspace(client_and_db):
    """
    Test endpoint `PUT-/workspace/configure` of config-API for selective
    changes in the config.
    """
    client, db = client_and_db
    ws_config0 = db.get_row("workspaces", util.DemoData.workspace1).eval()
    assert (
        client.put(
            "/workspace/configure",
            json={"id": ws_config0["id"], "name": "test"},
        ).status_code
        == 200
    )
    ws_config1 = db.get_row("workspaces", util.DemoData.workspace1).eval()
    assert ws_config0 != ws_config1
    assert ws_config1["name"] == "test"


def test_put_workspace_not_found(client_and_db, minimal_workspace_config):
    """
    Test endpoint `PUT-/workspace/configure` of config-API for non-existing
    workspace.
    """
    client, _ = client_and_db

    response = client.put(
        "/workspace/configure",
        json=minimal_workspace_config | {"id": str(uuid4())},
    )

    assert response.status_code == 404
    assert response.mimetype == "text/plain"


def test_delete_workspace(client_and_db, minimal_workspace_config):
    """Test endpoint `DELETE-/workspace/configure` of config-API."""
    client, db = client_and_db
    ws_id = db.insert("workspaces", minimal_workspace_config).eval()
    assert client.delete("/workspace/configure?id=" + ws_id).status_code == 200
    # can be repeated and still get 200
    assert client.delete("/workspace/configure?id=" + ws_id).status_code == 200
    assert db.get_row("workspaces", ws_id).eval() is None


def test_delete_workspace_w_linked_template(
    client_and_db, minimal_workspace_config, minimal_template_config
):
    """
    Test endpoint `DELETE-/workspace/configure` of config-API while a
    template is linked to the workspace.
    """
    client, db = client_and_db

    # associated template
    ws_id = db.insert("workspaces", minimal_workspace_config).eval()
    template_id = db.insert(
        "templates",
        TemplateConfig.from_json(minimal_template_config).row
        | {"workspace_id": ws_id},
    ).eval()
    assert client.delete("/workspace/configure?id=" + ws_id).status_code == 200
    # workspace is cleared in template as well
    assert db.get_row("templates", template_id).eval()["workspace_id"] is None


def test_delete_workspace_w_linked_user(
    client_and_db, minimal_workspace_config
):
    """
    Test endpoint `DELETE-/workspace/configure` of config-API while a
    user is linked to the workspace.
    """
    client, db = client_and_db

    # associated template
    ws_id = db.insert("workspaces", minimal_workspace_config).eval()
    group_id = db.insert(
        "user_groups",
        {
            "user_id": util.DemoData.user0,
            "workspace_id": ws_id,
            "group_id": "curator",
        },
    ).eval()
    assert client.delete("/workspace/configure?id=" + ws_id).status_code == 200
    # user-group is deleted as well
    assert db.get_row("user_groups", group_id).eval() is None


def test_template_options(client_and_db):
    """Test endpoint `OPTIONS-/template/configure` of config-API."""
    client, _ = client_and_db
    response = client.options("/template/configure")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert sorted(response.json) == sorted([
        util.DemoData.template1,
        util.DemoData.template2,
        util.DemoData.template3,
    ])


def test_get_template(client_and_db):
    """Test endpoint `GET-/template/configure` of config-API."""
    client, db = client_and_db
    response = client.get(
        f"/template/configure?id={util.DemoData.template1}"
    )

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == TemplateConfig.from_row(
        db.get_row("templates", util.DemoData.template1).eval()
    ).json


def test_post_template(client_and_db, minimal_template_config):
    """Test endpoint `POST-/template/configure` of config-API."""
    client, db = client_and_db
    response = client.post(
        "/template/configure", json=minimal_template_config
    )

    assert response.status_code == 200
    assert response.mimetype == "application/json"

    written_config = db.get_row("templates", response.json["id"]).eval()
    assert written_config["id"] == response.json["id"]
    assert written_config["name"] == minimal_template_config["name"]


def test_post_template_conflict(client_and_db, minimal_template_config):
    """
    Test endpoint `POST-/template/configure` of config-API for existing
    template.
    """
    client, _ = client_and_db
    response = client.post(
        "/template/configure",
        json=minimal_template_config | {"id": util.DemoData.template1},
    )

    assert response.status_code == 409
    assert response.mimetype == "text/plain"


def test_put_template(client_and_db, minimal_template_config):
    """
    Test endpoint `PUT-/template/configure` of config-API for selective
    changes in the config.
    """
    client, db = client_and_db
    t_config0 = db.get_row("templates", util.DemoData.template1).eval()
    assert (
        client.put(
            "/template/configure",
            json=minimal_template_config
            | {"id": t_config0["id"], "name": "test"},
        ).status_code
        == 200
    )
    t_config1 = db.get_row("templates", util.DemoData.template1).eval()
    assert t_config0 != t_config1
    assert t_config1["name"] == "test"


def test_put_template_not_found(client_and_db, minimal_template_config):
    """
    Test endpoint `PUT-/template/configure` of config-API for non-existing
    template.
    """
    client, _ = client_and_db

    response = client.put(
        "/template/configure",
        json=minimal_template_config | {"id": str(uuid4())},
    )

    assert response.status_code == 404
    assert response.mimetype == "text/plain"


def test_delete_template(client_and_db, minimal_template_config):
    """Test endpoint `DELETE-/template/configure` of config-API."""
    client, db = client_and_db
    t_id = db.insert(
        "templates", TemplateConfig.from_json(minimal_template_config).row
    ).eval()
    assert client.delete("/template/configure?id=" + t_id).status_code == 200
    # can be repeated and still get 200
    assert client.delete("/template/configure?id=" + t_id).status_code == 200
    assert db.get_row("templates", t_id).eval() is None


def test_list_hotfolder_sources(client_and_db):
    """Test endpoint `GET-/template/hotfolder-sources` of config-API."""
    client, _ = client_and_db
    response = client.get("/template/hotfolder-sources")

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert sorted([src["id"] for src in response.json]) == sorted(
        [v for k, v in util.DemoData.__dict__.items() if "hotfolder" in k]
    )
