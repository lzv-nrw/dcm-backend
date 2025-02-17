"""Test-module for user-endpoint."""

from hashlib import md5
from io import StringIO
import sys
import re

from argon2 import PasswordHasher
import pytest
from dcm_common.db import NativeKeyValueStoreAdapter, MemoryStore

from dcm_backend import app_factory


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
    return {"userId": "user0", "password": md5(b"password").hexdigest()}


@pytest.fixture(name="minimal_user_config")
def _minimal_user_config(user0_credentials):
    return {
        "userId": user0_credentials["userId"],
        "password": PasswordHasher().hash(user0_credentials["password"]),
    }


@pytest.fixture(name="client_and_db")
def _client_and_dbs(testing_config, minimal_user_config):
    class TestingConfigWithoutUserActivation(testing_config):
        REQUIRE_USER_ACTIVATION = False

    config_db = NativeKeyValueStoreAdapter(MemoryStore())
    config_db.write(minimal_user_config["userId"], minimal_user_config)
    return (
        app_factory(
            TestingConfigWithoutUserActivation(), user_config_db=config_db
        ).test_client(),
        config_db,
    )


def test_login(client_and_db, user0_credentials):
    """Test endpoint `POST-/user` of user-API."""
    client, _ = client_and_db
    response = client.post("/user", json=user0_credentials)

    assert response.status_code == 200
    assert response.mimetype == "text/plain"


def test_login_rehash(client_and_db, user0_credentials):
    """
    Test endpoint `POST-/user` of user-API regarding secret-re-hashing.
    """
    client, db = client_and_db
    assert client.post("/user", json=user0_credentials).status_code == 200

    # replace existing hash in db by one with different settings
    existing_config = db.read(user0_credentials["userId"])
    db.write(
        user0_credentials["userId"],
        existing_config
        | {
            "password": PasswordHasher(time_cost=1).hash(
                user0_credentials["password"]
            )
        },
    )

    # credentials still valid
    assert client.post("/user", json=user0_credentials).status_code == 200

    # check for re-hash
    existing_config2 = db.read(user0_credentials["userId"])
    assert existing_config["password"] != existing_config2["password"]


@pytest.mark.parametrize(
    "credentials",
    [
        {"userId": "user1"},
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
            json={"userId": "user1", "password": md5(b"user1").hexdigest()},
        ).status_code
        == 401
    )

    # create new user (and capture stdout for initial password)
    with StdOutReader() as output:
        assert (
            client.post("/user/configure", json={"userId": "user1"}).status_code
            == 200
        )
    password = re.findall(r"\(password=(.*)\)", output.lines[0])[0]

    # attempt login again
    assert (
        client.post(
            "/user",
            json={
                "userId": "user1",
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


def test_user_creation_and_login_with_user_activation(testing_config):
    """
    Test creation of new users with user account activation requirement.
    """
    class TestingConfigWithUserActivation(testing_config):
        REQUIRE_USER_ACTIVATION = True

    config_db = NativeKeyValueStoreAdapter(MemoryStore())
    client = app_factory(
        TestingConfigWithUserActivation(), user_config_db=config_db
    ).test_client()

    # create new user (and capture stdout for initial password)
    with StdOutReader() as output:
        assert (
            client.post(
                "/user/configure", json={"userId": "user1"}
            ).status_code
            == 200
        )
    password = re.findall(r"\(password=(.*)\)", output.lines[0])[0]

    # attempt login
    assert (
        client.post(
            "/user",
            json={
                "userId": "user1",
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
                "userId": "user1",
                "password": md5(password.encode(encoding="utf-8")).hexdigest(),
                "newPassword": new_password,
            },
        ).status_code
        == 200
    )
    # attempt login again
    assert (
        client.post(
            "/user", json={"userId": "user1", "password": new_password}
        ).status_code
        == 200
    )
