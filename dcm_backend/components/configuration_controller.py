"""
This module defines the `ConfigurationController` component of the
dcm-backend-app.
"""

from datetime import datetime
from uuid import uuid1
from typing import Optional
import sys

from dcm_common.db import KeyValueStoreAdapter, MemoryStore
from dcm_common.models import JSONable
from dcm_common.util import now


class ConfigurationController:
    """
    A `ConfigurationController` can be used to store and fetch job
    configuration objects (as JSONable) to and from a database. It can
    use a cache for the database and record a history of previous
    requests to provide incremental updates (see `token` in method
    `get`).

    Keyword arguments:
    db -- adapter for configuration database
    caching -- if `True`, cache contents of database internally
               (default True)
    max_tokens -- maximum number of tokens stored in history
                  (if exceeded, oldest tokens are deleted first)
                  (default 1000) (if None, then set to sys.maxsize)
    """

    def __init__(
        self, db: KeyValueStoreAdapter, caching: bool = True,
        max_tokens: Optional[int] = 1000
    ) -> None:
        self._db = db  # adapter for 'persistent' database
        self._cache = (  # used to cache _db internally
            self._generate_cache() if caching else None
        )
        self._history: dict[str, datetime] = {}
        self._token_fifo: list[str] = []
        self._max_tokens: int = (
            max_tokens if max_tokens is not None else sys.maxsize
        )

    def _generate_cache(self) -> MemoryStore:
        """Instantiate `MemoryStore` and load contents of `self._db`."""
        cache = MemoryStore()
        for key in self._db.keys():
            cache.write(key, self._db.read(key))
        return cache

    def set(self, config: JSONable) -> str:
        """
        Writes `config` to database and returns associated config-id.

        If either 'id' or 'last_modified' are not set, they are
        generated automatically.
        """
        if "id" not in config:
            config["id"] = self._db.push(None)

        if "last_modified" not in config:
            config["last_modified"] = now().isoformat()

        self._db.write(config["id"], config)
        if self._cache:
            # also write in cache
            self._cache.write(config["id"], config)

        return config["id"]

    def get(self, token: Optional[str] = None) -> tuple[dict, str]:
        """
        Returns a tuple of
        * a collection of configurations as JSON
          (schema job-id: job-config) and
        * a token identifying a request-history.

        If a token is given, only an incremental update is returned.

        Note that the datetime stamps are only down to seconds. If a
        token and config share their value, the config will be returned
        repeatedly (along with varying tokens). This is intended
        behavior which ensures that always the most recent config is
        returned, but eventually 'fixes' itself when tokens are
        iterated (as soon as the last token and config datetimes
        differ by more than a second).
        """

        # build configs-dict
        configs: dict[str, JSONable] = {}
        for key in (self._cache or self._db).keys():
            config = (self._cache or self._db).read(key)

            # if token given, check whether config has been modified
            if (
                token is not None
                and token in self._history
                and datetime.fromisoformat(
                    config["last_modified"]
                ) < self._history[token]
            ):
                continue
            configs[config["id"]] = config

        # if valid token has been given but nothing has changed,
        # return with the same token
        if len(configs) == 0 and token is not None and token in self._history:
            return configs, token

        _token = str(uuid1())
        self._history[_token] = now()

        self._token_fifo.append(_token)
        self._cleanup()

        return configs, _token

    def _cleanup(self) -> None:
        if len(self._token_fifo) <= self._max_tokens:
            return

        n_tokens = len(self._token_fifo) - self._max_tokens
        for token in self._token_fifo[:n_tokens]:
            del self._history[token]

        self._token_fifo = self._token_fifo[n_tokens:]
