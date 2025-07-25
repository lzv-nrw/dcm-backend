"""Test-module for user-endpoint."""

from hashlib import md5
from io import StringIO
import sys
import re

from argon2 import PasswordHasher
import pytest

from dcm_backend.models import UserConfig
from dcm_backend import app_factory, util


class StdOutReader:
    def __init__(self):
        self.lines = []
        self._stdout = sys.stdout
        self._stringio = StringIO()

    def __enter__(self):
        sys.stdout = self._stringio
        return self

    def __exit__(self, *args):
        sys.stdout = self._stdout
        self.lines = self._stringio.getvalue().splitlines()


@pytest.fixture(name="user0_credentials")
def _user0_password():
    return {
        "username": "admin",
        "password": md5(
            util.DemoData.admin_password.encode(encoding="utf-8")
        ).hexdigest(),
    }


@pytest.fixture(name="client_and_db")
def _client_and_dbs(testing_config):
    class TestingConfigWithoutUserActivation(testing_config):
        REQUIRE_USER_ACTIVATION = False

    config = TestingConfigWithoutUserActivation()
    return app_factory(config, block=True).test_client(), config.db


def test_login(client_and_db, user0_credentials):
    """Test endpoint `POST-/user` of user-API."""
    client, db = client_and_db
    response = client.post("/user", json=user0_credentials)

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert response.json == UserConfig.from_row(
        db.get_row("user_configs", util.DemoData.user0).eval()
    ).json | {"groups": [{"id": "admin"}]}


def test_login_rehash(client_and_db, user0_credentials):
    """
    Test endpoint `POST-/user` of user-API regarding secret-re-hashing.
    """
    client, db = client_and_db
    assert client.post("/user", json=user0_credentials).status_code == 200

    # replace existing hash in db by one with different settings
    config = db.get_rows(
        "user_configs", user0_credentials["username"], "username", ["id"]
    ).eval()[0]
    secrets = db.get_rows("user_secrets", config["id"], "user_id").eval()[0]
    db.update(
        "user_secrets",
        {
            "id": secrets["id"],
            "user_id": config["id"],
            "password": PasswordHasher(time_cost=1).hash(
                user0_credentials["password"]
            ),
        },
    )

    # credentials still valid
    assert client.post("/user", json=user0_credentials).status_code == 200

    # check for re-hash
    secrets2 = db.get_rows("user_secrets", config["id"], "user_id").eval()[0]
    assert secrets["password"] != secrets2["password"]


@pytest.mark.parametrize(
    "credentials",
    [
        {"username": "not-admin"},
        {"password": "not-password"},
    ],
    ids=["bad-user", "bad-password"],
)
def test_login_failed(credentials, client_and_db, user0_credentials):
    """Test endpoint `POST-/user` of user-API with bad credentials."""
    client, _ = client_and_db
    response = client.post("/user", json=user0_credentials | credentials)

    assert response.status_code == 401
    assert response.mimetype == "text/plain"


def test_new_user_then_login(client_and_db):
    """
    Test endpoints `POST-/user/configure` and `POST-/user` of user-API.
    """
    client, _ = client_and_db

    # user does not exist
    assert (
        client.post(
            "/user",
            json={"username": "newuser", "password": md5(b"a").hexdigest()},
        ).status_code
        == 401
    )

    # create new user (and capture stdout for initial password)
    with StdOutReader() as output:
        assert (
            client.post(
                "/user/configure",
                json={"username": "newuser", "email": "a@b.c"},
            ).status_code
            == 200
        )
    password = re.findall(r"\(password=(.*)\)", output.lines[0])[0]

    # attempt login again
    assert (
        client.post(
            "/user",
            json={
                "username": "newuser",
                "password": md5(password.encode(encoding="utf-8")).hexdigest(),
            },
        ).status_code
        == 200
    )


def test_user_change_password(client_and_db, user0_credentials):
    """Test endpoint `POST-/user/password` of user-API."""
    client, _ = client_and_db

    # validate correct credentials
    assert client.post("/user", json=user0_credentials).status_code == 200

    # change password
    new_credentials = user0_credentials | {
        "password": md5(b"another-pw").hexdigest()
    }
    assert (
        client.put(
            "/user/password",
            json=user0_credentials
            | {"newPassword": new_credentials["password"]},
        ).status_code
        == 200
    )
    # attempt login again
    assert client.post("/user", json=user0_credentials).status_code == 401
    assert client.post("/user", json=new_credentials).status_code == 200


def test_new_user_and_login_with_user_activation(testing_config):
    """
    Test creation of new users with user account activation requirement.
    """
    class TestingConfigWithUserActivation(testing_config):
        REQUIRE_USER_ACTIVATION = True

    config = TestingConfigWithUserActivation()
    client = app_factory(config, block=True).test_client()

    # create new user (and capture stdout for initial password)
    with StdOutReader() as output:
        assert (
            client.post(
                "/user/configure",
                json={"username": "newuser", "email": "a@b.c"},
            ).status_code
            == 200
        )
    password = re.findall(r"\(password=(.*)\)", output.lines[0])[0]

    # attempt login
    assert (
        client.post(
            "/user",
            json={
                "username": "newuser",
                "password": md5(password.encode(encoding="utf-8")).hexdigest(),
            },
        ).status_code
        == 403
    )

    # change password
    new_password = md5(b"another-pw").hexdigest()
    assert (
        client.put(
            "/user/password",
            json={
                "username": "newuser",
                "password": md5(password.encode(encoding="utf-8")).hexdigest(),
                "newPassword": new_password,
            },
        ).status_code
        == 200
    )
    # attempt login again
    assert (
        client.post(
            "/user", json={"username": "newuser", "password": new_password}
        ).status_code
        == 200
    )
