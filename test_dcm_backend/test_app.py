"""
Test module for the app factory.
"""

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
    client = app_factory(testing_config(), config_db=config_db).test_client()

    if with_scheduled_config:
        assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 1
    else:
        assert len(client.get("/schedule").json["scheduler"]["scheduled"]) == 0
