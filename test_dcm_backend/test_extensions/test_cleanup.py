"""Test module for the cleanup-extension."""

from uuid import uuid4
from pathlib import Path
import os

from dcm_common.db import SQLiteAdapter3
from dcm_common.services.extensions.common import (
    ExtensionLoaderResult,
    ExtensionConditionRequirement,
)
import pytest

from dcm_backend.extensions.cleanup import run_cleanup
from dcm_backend.config import AppConfig


@pytest.fixture(name="db")
def _db():
    # setup empty database
    db = SQLiteAdapter3(allow_overflow=False)
    db.read_file(AppConfig.DB_SCHEMA)
    return db


@pytest.fixture(name="cleanup_target")
def _cleanup_target(file_storage: Path):
    # setup cleanup-directory
    cleanup_target = (file_storage / str(uuid4())).resolve()
    (file_storage / cleanup_target.name).mkdir(parents=True)

    # create data in cleanup-directory
    (cleanup_target / "some-file").touch()
    (cleanup_target / "some-directory").mkdir()
    return cleanup_target


def test_run_cleanup_simple(
    file_storage: Path, db: SQLiteAdapter3, cleanup_target: Path
):
    """Test function `run_cleanup`."""

    result = ExtensionLoaderResult()

    # first run registers artifacts in database
    run_cleanup(
        [cleanup_target],
        file_storage,
        0,
        db,
        result,
        [],
    )

    assert len(db.get_column("artifacts", "id").eval()) == 2
    assert (cleanup_target / "some-file").is_file()
    assert (cleanup_target / "some-directory").is_dir()
    assert result.ready.is_set()

    # second one performs cleanup
    run_cleanup(
        [cleanup_target],
        file_storage,
        0,
        db,
        result,
        [],
    )

    assert len(db.get_column("artifacts", "id").eval()) == 0
    assert not (cleanup_target / "some-file").is_file()
    assert not (cleanup_target / "some-directory").is_dir()


def test_run_cleanup_missing_requirement(
    file_storage: Path, db: SQLiteAdapter3, cleanup_target: Path
):
    """Test function `run_cleanup`."""

    result = ExtensionLoaderResult()
    result.ready.set()

    run_cleanup(
        [cleanup_target],
        file_storage,
        0,
        db,
        result,
        [ExtensionConditionRequirement(lambda: False, "test")],
    )

    assert len(db.get_column("artifacts", "id").eval()) == 0
    assert (cleanup_target / "some-file").is_file()
    assert (cleanup_target / "some-directory").is_dir()

    assert not result.ready.is_set()


def test_run_cleanup_already_deleted(
    file_storage: Path, db: SQLiteAdapter3, cleanup_target: Path
):
    """Test function `run_cleanup`."""

    result = ExtensionLoaderResult()

    run_cleanup(
        [cleanup_target],
        file_storage,
        0,
        db,
        result,
        [],
    )

    assert len(db.get_column("artifacts", "id").eval()) == 2

    (cleanup_target / "some-file").unlink()
    (cleanup_target / "some-directory").rmdir()

    # runs without error
    run_cleanup(
        [cleanup_target],
        file_storage,
        0,
        db,
        result,
        [],
    )

    assert len(db.get_column("artifacts", "id").eval()) == 0


def test_run_cleanup_fifo_ignored(
    file_storage: Path, db: SQLiteAdapter3, cleanup_target: Path
):
    """Test function `run_cleanup`."""

    (cleanup_target / "some-file").unlink()
    (cleanup_target / "some-directory").rmdir()
    os.mkfifo(cleanup_target / "some-fifo")

    result = ExtensionLoaderResult()

    run_cleanup(
        [cleanup_target],
        file_storage,
        0,
        db,
        result,
        [],
    )

    assert len(db.get_column("artifacts", "id").eval()) == 0
