"""
Test module for the app config.
"""

import pytest

from dcm_backend.config import AppConfig


def test_unknown_db_adapter():
    """
    Test behavior of `AppConfig`-constructor for unknown db-adapter.
    """
    class TestConfig(AppConfig):
        DB_ADAPTER = "unknown"

    with pytest.raises(ValueError):
        TestConfig()
