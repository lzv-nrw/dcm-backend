"""
Test module for the app factory.
"""

from hashlib import md5

import pytest
from dcm_common.db import NativeKeyValueStoreAdapter, MemoryStore

from dcm_backend import app_factory


@pytest.mark.parametrize(
    "with_scheduled_config",
    [False, True],
    ids=["without-scheduled-job", "with-scheduled-job"]
)
def test_scheduler_initialization(with_scheduled_config, testing_config):
    """
    Test whether existing configs are properly loaded and scheduled.
    """
    config_db = NativeKeyValueStoreAdapter(MemoryStore())
    if with_scheduled_config:
        config_db.write(
            "test-id",
            {
                "schedule": {
                    "active": True,
                    "repeat": {
                        "unit": "day",
                        "interval": 1
                    }
                },
                "id": "test-id",
                "job": {
                    "args": {},
                    "from": "import_ips",
                    "to": "validation"
                }
            }
        )
    client = app_factory(
        testing_config(), job_config_db=config_db
    ).test_client()
    if with_scheduled_config:
        assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 1
    else:
        assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 0


@pytest.mark.parametrize(
    "with_demo_user",
    [False, True],
    ids=["without-demo_user", "with-demo_user"],
)
def test_demo_user_initialization(testing_config, with_demo_user):
    """
    Test whether demo users are properly loaded.
    """
    user_db = NativeKeyValueStoreAdapter(MemoryStore())

    class TestingDemoUserConfig(testing_config):
        CREATE_DEMO_USERS = with_demo_user

    app_factory(TestingDemoUserConfig(), user_config_db=user_db)

    if with_demo_user:
        assert len(user_db.keys()) > 0
        for key in user_db.keys():
            assert key == user_db.read(key)["userId"]
    else:
        assert len(user_db.keys()) == 0


def test_demo_user_initialization_login(testing_config):
    """
    Test whether demo users login works properly.
    """
    class TestingDemoUserConfig(testing_config):
        CREATE_DEMO_USERS = True

    client = app_factory(TestingDemoUserConfig()).test_client()

    assert (
        client.post(
            "/user",
            json={
                "userId": "Einstein",
                "password": md5(b"relativity").hexdigest(),
            },
        ).status_code
        == 200
    )
