"""ConfigurationController-component test-module."""

from uuid import uuid1
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from dcm_common.util import now
from dcm_common.db import MemoryStore, NativeKeyValueStoreAdapter

from dcm_backend.components import ConfigurationController


@pytest.fixture(name="config")
def _config():
    return {
        "id": str(uuid1()),
        "active": True,
        "scheduling": None,
        "job": {"from": "import_ies", "args": {}}
    }


def test_set(config):
    """
    Test method `set` of class `ConfigurationController`.
    """
    db = MemoryStore()
    cc = ConfigurationController(NativeKeyValueStoreAdapter(db))
    config["last_modified"] = now().isoformat()
    key = cc.set(config)

    assert key in db.keys()
    assert db.read(key) == config


def test_set_update(config):
    """
    Test method `set` of class `ConfigurationController` for updating
    configurations.
    """
    db = MemoryStore()
    cc = ConfigurationController(NativeKeyValueStoreAdapter(db))
    key = cc.set(config)

    _config = config.copy()
    _config["active"] = False
    cc.set(_config)

    assert db.read(key)["active"] == _config["active"]


def test_set_missing_id(config):
    """
    Test method `set` of class `ConfigurationController` with missing
    'id'-field.
    """
    db = MemoryStore()
    cc = ConfigurationController(NativeKeyValueStoreAdapter(db))

    del config["id"]
    key = cc.set(config)

    assert db.read(key)["id"] == key


def test_set_missing_last_modified(config):
    """
    Test method `set` of class `ConfigurationController` with missing
    'last_modified'-field.
    """
    db = MemoryStore()
    cc = ConfigurationController(NativeKeyValueStoreAdapter(db))

    time_0 = now()
    key = cc.set(config)
    time_1 = now()

    assert (
        time_0
        <= datetime.fromisoformat(db.read(key)["last_modified"])
        <= time_1
    )


def test_get_no_changes(config):
    """
    Test behavior of method `get` of class `ConfigurationController` if
    there have been no changes since last request.
    """
    db = MemoryStore()
    cc = ConfigurationController(NativeKeyValueStoreAdapter(db))
    config["last_modified"] = (now() + timedelta(seconds=-1)).isoformat()
    cc.set(config)

    _, token1 = cc.get()
    configs2, token2 = cc.get(token1)

    assert token2 == token1
    assert not configs2


def test_get_no_changes_sub_second_requests(config):
    """
    Test behavior of method `get` of class `ConfigurationController` if
    there have been no changes since last request but requests happen in
    the same second as a config has been last modified. Check whether
    after that duration a repeating token is returned and the configs
    are omitted.
    """
    db = MemoryStore()
    cc = ConfigurationController(NativeKeyValueStoreAdapter(db))

    # duration where datetime-stamps for token and config are identical
    fixed_now = now()
    with patch(
        "dcm_backend.components.configuration_controller.now",
        side_effect=lambda **kwargs: fixed_now
    ):
        cc.set(config)
        configs1, token1 = cc.get()
        configs2, token2 = cc.get(token1)
        configs3, token3 = cc.get(token2)
        assert token2 != token1
        assert configs2 == configs1
        assert token3 != token2
        assert configs3 == configs1

    # fast-forward by faking to the point where datetime-stamps for
    # token and config differ
    fixed_now = now() + timedelta(seconds=1)
    with patch(
        "dcm_backend.components.configuration_controller.now",
        side_effect=lambda **kwargs: fixed_now
    ):
        configs2, token2 = cc.get(token1)
        configs3, token3 = cc.get(token2)

    assert token3 == token2
    assert not configs3


def test_cleanup(config):
    """
    Submit and get until maximum number of tokens, then check returned
    configs.
    """

    cc = ConfigurationController(NativeKeyValueStoreAdapter(MemoryStore()), 3)
    # Fake the addition of the config in the past
    # Get initial token
    fixed_now = now() + timedelta(seconds=-1)
    with patch(
        "dcm_backend.components.configuration_controller.now",
        side_effect=lambda **kwargs: fixed_now
    ):
        cc.set(config)
        _, token = cc.get()
    # Fast-forward until history length exceeded
    for i in range(3):
        _config = config.copy()
        _config["id"] = str(i)
        _config["last_modified"] = now().isoformat()
        cc.set(_config)
        cc.get()
    # Reuse old token
    # Validate token has been forgotten
    fixed_now = now() + timedelta(seconds=1)
    with patch(
        "dcm_backend.components.configuration_controller.now",
        side_effect=lambda **kwargs: fixed_now
    ):
        configs1, token1 = cc.get(token)

    assert config["id"] in configs1
    assert token1 != token


@pytest.mark.parametrize(
    "caching",
    [True, False],
    ids=["cache", "no-cache"]
)
def test_caching(caching, config):
    """
    Test whether `ConfigurationController` uses a cached version of the
    database by modifying that database manually.
    """
    db = MemoryStore()
    cc = ConfigurationController(NativeKeyValueStoreAdapter(db), caching)

    key = cc.set(config)
    configs, _ = cc.get()
    assert configs[key] == config

    # validate cached version still exists even though deleted in db
    db.delete(key)
    configs, _ = cc.get()
    if caching:
        assert configs[key] == config
    else:
        assert key not in configs


def test_cache_initialization(config):
    """
    Test whether `ConfigurationController` cache is initialized
    correctly.
    """
    db = MemoryStore()
    config["last_modified"] = now().isoformat()
    db.write(config["id"], config)

    cc = ConfigurationController(NativeKeyValueStoreAdapter(db))
    configs, _ = cc.get()
    assert configs[config["id"]] == config
