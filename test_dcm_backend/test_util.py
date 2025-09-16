"""Test module for `util.py`."""

import json
from uuid import uuid4

import pytest

from dcm_backend import util


def test_load_hotfolders_from_string_basic(temp_folder):
    """Test function `load_hotfolders_from_string`."""

    hotfolders = util.load_hotfolders_from_string(
        json.dumps(
            [
                {
                    "id": "0",
                    "mount": str(temp_folder),
                    "name": "a",
                    "description": "b",
                },
                {"id": "1", "mount": str(temp_folder), "name": "c"},
            ]
        )
    )

    assert len(hotfolders) == 2
    assert "0" in hotfolders
    assert "1" in hotfolders
    assert hotfolders["0"].id_ == "0"
    assert hotfolders["0"].mount == temp_folder
    assert hotfolders["0"].name == "a"
    assert hotfolders["0"].description == "b"
    assert hotfolders["1"].id_ == "1"
    assert hotfolders["1"].mount == temp_folder
    assert hotfolders["1"].name == "c"
    assert hotfolders["1"].description is None


def test_load_hotfolders_from_string_bad_id():
    """Test function `load_hotfolders_from_string`."""
    with pytest.raises(ValueError) as exc_info:
        util.load_hotfolders_from_string(json.dumps([{"id": 0}]))
    print(exc_info.value)


def test_load_hotfolders_from_string_duplicate_id(temp_folder):
    """Test function `load_hotfolders_from_string`."""
    with pytest.raises(ValueError) as exc_info:
        util.load_hotfolders_from_string(
            json.dumps(
                [
                    {"id": "0", "mount": str(temp_folder), "name": "a"},
                    {"id": "0"},
                ]
            )
        )
    print(exc_info.value)


def test_load_hotfolders_from_string_not_hotfolder():
    """Test function `load_hotfolders_from_string`."""
    with pytest.raises(ValueError) as exc_info:
        util.load_hotfolders_from_string(json.dumps([{"id": "0"}]))
    print(exc_info.value)


def test_load_hotfolders_from_file_basic(temp_folder):
    """Test function `load_hotfolders_from_file`."""

    file = temp_folder / str(uuid4())
    file.write_text(
        json.dumps(
            [
                {
                    "id": "0",
                    "mount": str(temp_folder),
                    "name": "a",
                    "description": "b",
                },
                {"id": "1", "mount": str(temp_folder), "name": "c"},
            ]
        ),
        encoding="utf-8",
    )

    hotfolders = util.load_hotfolders_from_file(file)

    assert len(hotfolders) == 2
    assert "0" in hotfolders
    assert "1" in hotfolders
    assert hotfolders["0"].id_ == "0"
    assert hotfolders["0"].mount == temp_folder
    assert hotfolders["0"].name == "a"
    assert hotfolders["0"].description == "b"
    assert hotfolders["1"].id_ == "1"
    assert hotfolders["1"].mount == temp_folder
    assert hotfolders["1"].name == "c"
    assert hotfolders["1"].description is None
