"""Test module for `util.py`."""

import json
from uuid import uuid4

import pytest

from dcm_backend import util
from dcm_backend.models import ArchiveAPI


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


@pytest.fixture(name="minimal_archive_configuration")
def _minimal_archive_configuration():
    return {
        "id": "0",
        "name": "a",
        "type": ArchiveAPI.ROSETTA_REST_V0.value,
        "transferDestinationId": "0a",
        "details": {
            "url": "",
            "materialFlow": "",
            "producer": "",
            "basicAuth": "",
        },
    }


def test_load_archive_configurations_from_string_basic():
    """Test function `load_archive_configurations_from_string`."""

    archive_0 = {
        "id": "0",
        "name": "a",
        "description": "b",
        "type": ArchiveAPI.ROSETTA_REST_V0.value,
        "transferDestinationId": "0a",
        "details": {
            "url": "",
            "materialFlow": "",
            "producer": "",
            "basicAuth": "",
            "authFile": __file__,
            "proxy": {"http": ""},
        },
    }
    archive_1 = {
        "id": "1",
        "name": "a",
        "type": ArchiveAPI.ROSETTA_REST_V0.value,
        "transferDestinationId": "1a",
        "details": {
            "url": "",
            "materialFlow": "",
            "producer": "",
            "basicAuth": "",
        },
    }
    archives = util.load_archive_configurations_from_string(
        json.dumps([archive_0, archive_1])
    )

    assert len(archives) == 2
    assert "0" in archives
    assert "1" in archives
    assert archives["0"].json == archive_0
    assert archives["1"].json == archive_1


def test_load_archive_configurations_from_string_bad_id(
    minimal_archive_configuration,
):
    """Test function `load_archive_configurations_from_string`."""
    with pytest.raises(ValueError) as exc_info:
        util.load_archive_configurations_from_string(
            json.dumps([minimal_archive_configuration | {"id": 0}])
        )
    print(exc_info.value)


def test_load_archive_configurations_from_string_duplicate_id(
    minimal_archive_configuration,
):
    """Test function `load_archive_configurations_from_string`."""
    with pytest.raises(ValueError) as exc_info:
        util.load_archive_configurations_from_string(
            json.dumps(
                [minimal_archive_configuration, minimal_archive_configuration]
            )
        )
    print(exc_info.value)


def test_load_archive_configurations_from_string_unknown_type(
    minimal_archive_configuration,
):
    """Test function `load_archive_configurations_from_string`."""
    with pytest.raises(ValueError) as exc_info:
        util.load_archive_configurations_from_string(
            json.dumps([minimal_archive_configuration | {"type": "unknown"}])
        )
    print(exc_info.value)


def test_load_archive_configurations_from_string_not_an_archive(
    minimal_archive_configuration,
):
    """Test function `load_archive_configurations_from_string`."""
    del minimal_archive_configuration["name"]
    with pytest.raises(ValueError) as exc_info:
        util.load_archive_configurations_from_string(
            json.dumps([minimal_archive_configuration])
        )
    print(exc_info.value)


def test_load_archive_configurations_from_file_basic(
    temp_folder, minimal_archive_configuration
):
    """Test function `load_archive_configurations_from_file`."""

    file = temp_folder / str(uuid4())
    file.write_text(
        json.dumps([minimal_archive_configuration]), encoding="utf-8"
    )

    archives = util.load_archive_configurations_from_file(file)

    assert len(archives) == 1
    assert (
        archives[minimal_archive_configuration["id"]].json
        == minimal_archive_configuration
    )
