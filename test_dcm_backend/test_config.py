"""
Test module for the app config.
"""

import pytest

from dcm_backend.config import AppConfig


def test_archive_api_proxy(testing_config):
    """
    Test the app config when the environment variable
    ARCHIVE_API_PROXY is set.
    """

    proxy = {"http": "https://www.lzv.nrw/proxy"}

    testing_config.ARCHIVE_API_PROXY = proxy

    assert (
        testing_config().CONTAINER_SELF_DESCRIPTION["configuration"][
            "settings"
        ]["ingest"]["proxy"]
    ) == proxy


def test_unknown_db_adapter():
    """
    Test behavior of `AppConfig`-constructor for unknown db-adapter.
    """
    class TestConfig(AppConfig):
        DB_ADAPTER = "unknown"

    with pytest.raises(ValueError):
        TestConfig()
