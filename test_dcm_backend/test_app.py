"""
Test module for the app factory.
"""

import pytest

from dcm_backend import app_factory


@pytest.mark.parametrize(
    "with_scheduled_config",
    [False, True],
    ids=["without-scheduled-job", "with-scheduled-job"]
)
def test_scheduler_initialization(with_scheduled_config, testing_config):
    """Test whether existing configs are properly scheduled."""
    class ThisTestingConfig(testing_config):
        DB_GENERATE_DEMO = with_scheduled_config
        SCHEDULING_AT_STARTUP = True

    client = app_factory(ThisTestingConfig(), block=True).test_client()
    if with_scheduled_config:
        assert len(client.get("/schedule").json["scheduled"]) == 1
    else:
        assert len(client.get("/schedule").json["scheduled"]) == 0

    client.delete("/schedule")
