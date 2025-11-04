"""Test-module for artifact-endpoints."""

from uuid import uuid4
import zipfile
from pathlib import Path

from dcm_common import LoggingContext as Context

from dcm_backend import app_factory


def test_get_artifact(testing_config):
    """Test basic functionality of GET-/artifact endpoint."""

    config = testing_config()
    app = app_factory(config)
    app.extensions["orchestra"].stop(stop_on_idle=True)
    client = app.test_client()

    # create dummy file
    file = (
        config.FS_MOUNT_POINT
        / config.ARTIFACT_BUNDLE_DESTINATION
        / str(uuid4())
    )
    data = b"test"
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_bytes(data)

    # download and check contents
    response = client.get(f"/artifact?id={file.name}")
    assert response.status_code == 200
    assert response.data == data

    # check filename in header
    assert "filename=dcm-artifact-" in response.headers["Content-Disposition"]
    assert (
        "filename=test"
        in client.get(f"/artifact?id={file.name}&downloadName=test").headers[
            "Content-Disposition"
        ]
    )


def test_post_artifact_minimal(testing_config):
    """Test basic functionality of POST-/artifact endpoint."""

    config = testing_config()
    app = app_factory(config)
    client = app.test_client()

    # create dummy file
    target = config.artifact_sources[0] / str(uuid4())
    data = b"test"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)

    # request bundling
    response = client.post(
        "/artifact",
        json={
            "bundle": {
                "targets": [
                    {
                        "path": str(
                            target.relative_to(config.FS_MOUNT_POINT.resolve())
                        )
                    }
                ]
            }
        },
    )

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/artifact/report?token={token}").json

    assert json["data"]["success"]
    assert (
        config.FS_MOUNT_POINT
        / config.ARTIFACT_BUNDLE_DESTINATION
        / json["data"]["bundle"]["id"]
    ).is_file()
    assert json["data"]["bundle"]["size"] > 0

    # check file contents
    output = config.FS_MOUNT_POINT / str(uuid4())
    with zipfile.ZipFile(
        config.FS_MOUNT_POINT
        / config.ARTIFACT_BUNDLE_DESTINATION
        / json["data"]["bundle"]["id"],
        "r",
    ) as archive:
        archive.extractall(output)
    assert (
        output / target.relative_to(config.FS_MOUNT_POINT.resolve())
    ).is_file()


def test_post_artifact_invalid_target(testing_config):
    """Test POST-/artifact endpoint."""

    config = testing_config()
    app = app_factory(config)
    client = app.test_client()

    # create dummy file outside of allowed directory
    target = config.FS_MOUNT_POINT / str(uuid4()) / "file"
    data = b"test"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)

    # request bundling
    response = client.post(
        "/artifact",
        json={
            "bundle": {
                "targets": [
                    {"path": str(target.relative_to(config.FS_MOUNT_POINT))}
                ]
            }
        },
    )

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/artifact/report?token={token}").json

    assert not json["data"]["success"]
    assert Context.ERROR.name in json["log"]
    print(json["log"][Context.ERROR.name][0]["body"])


def test_post_artifact_file_too_large(testing_config):
    """Test POST-/artifact endpoint."""

    class ThisConfig(testing_config):
        ARTIFACT_FILE_MAX_SIZE = 1

    config = ThisConfig()
    app = app_factory(config)
    client = app.test_client()

    # create dummy file
    target = config.artifact_sources[0] / str(uuid4())
    data = b"test"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)

    # request bundling
    response = client.post(
        "/artifact",
        json={
            "bundle": {
                "targets": [
                    {
                        "path": str(
                            target.relative_to(config.FS_MOUNT_POINT.resolve())
                        )
                    }
                ]
            }
        },
    )

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/artifact/report?token={token}").json

    assert json["data"]["success"]
    assert (
        config.FS_MOUNT_POINT
        / config.ARTIFACT_BUNDLE_DESTINATION
        / json["data"]["bundle"]["id"]
    ).is_file()
    assert Context.WARNING.name in json["log"]
    print(json["log"][Context.WARNING.name][0]["body"])

    # check file contents
    output = config.FS_MOUNT_POINT / str(uuid4())
    with zipfile.ZipFile(
        config.FS_MOUNT_POINT
        / config.ARTIFACT_BUNDLE_DESTINATION
        / json["data"]["bundle"]["id"],
        "r",
    ) as archive:
        archive.extractall(output)
    assert not (
        output / target.relative_to(config.FS_MOUNT_POINT.resolve())
    ).is_file()
    placeholder_file = Path(
        str(output / target.relative_to(config.FS_MOUNT_POINT.resolve()))
        + ".omitted.txt"
    )
    assert placeholder_file.is_file()
    placeholder_data = placeholder_file.read_bytes()
    print(placeholder_data)
    assert placeholder_data != data


def test_post_artifact_bundle_too_large(testing_config):
    """Test POST-/artifact endpoint."""

    class ThisConfig(testing_config):
        ARTIFACT_BUNDLE_MAX_SIZE = 1

    config = ThisConfig()
    app = app_factory(config)
    client = app.test_client()

    # create dummy file
    target = config.artifact_sources[0] / str(uuid4())
    data = b"test"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)

    # request bundling
    response = client.post(
        "/artifact",
        json={
            "bundle": {
                "targets": [
                    {
                        "path": str(
                            target.relative_to(config.FS_MOUNT_POINT.resolve())
                        )
                    }
                ]
            }
        },
    )

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/artifact/report?token={token}").json

    assert not json["data"]["success"]
    assert Context.ERROR.name in json["log"]
    print(json["log"][Context.ERROR.name][0]["body"])


def test_post_artifact_bundle_as_path(testing_config):
    """Test POST-/artifact endpoint."""

    config = testing_config()
    app = app_factory(config)
    client = app.test_client()

    # create dummy file
    target = config.artifact_sources[0] / str(uuid4())
    data = b"test"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    as_path = "different-path"

    # request bundling
    response = client.post(
        "/artifact",
        json={
            "bundle": {
                "targets": [
                    {
                        "path": str(
                            target.relative_to(config.FS_MOUNT_POINT.resolve())
                        ),
                        "asPath": as_path,
                    }
                ]
            }
        },
    )

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/artifact/report?token={token}").json

    assert json["data"]["success"]

    # check file contents
    output = config.FS_MOUNT_POINT / str(uuid4())
    with zipfile.ZipFile(
        config.FS_MOUNT_POINT
        / config.ARTIFACT_BUNDLE_DESTINATION
        / json["data"]["bundle"]["id"],
        "r",
    ) as archive:
        archive.extractall(output)
    assert not (
        output / target.relative_to(config.FS_MOUNT_POINT.resolve())
    ).is_file()
    assert (output / as_path).is_file()


def test_post_artifact_bundle_conflict(testing_config):
    """Test POST-/artifact endpoint."""

    config = testing_config()
    app = app_factory(config)
    client = app.test_client()

    # create dummy files
    target = config.artifact_sources[0] / str(uuid4())
    data_0 = b"test-0"
    data_1 = b"test-1"
    target.mkdir(parents=True, exist_ok=True)
    (target / "a").write_bytes(data_0)
    (target / "b").write_bytes(data_1)

    # request bundling for two files under the same name
    response = client.post(
        "/artifact",
        json={
            "bundle": {
                "targets": [
                    {
                        "path": str(
                            (target / "a").relative_to(
                                config.FS_MOUNT_POINT.resolve()
                            )
                        ),
                        "asPath": "c",
                    },
                    {
                        "path": str(
                            (target / "b").relative_to(
                                config.FS_MOUNT_POINT.resolve()
                            )
                        ),
                        "asPath": "c",
                    },
                ]
            }
        },
    )

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/artifact/report?token={token}").json

    assert not json["data"]["success"]
    assert Context.ERROR.name in json["log"]
    print(json["log"][Context.ERROR.name][0]["body"])


def test_post_artifact_bundle_multiple_targets_w_directory_as_path(
    testing_config,
):
    """Test POST-/artifact endpoint."""

    config = testing_config()
    app = app_factory(config)
    client = app.test_client()

    # create dummy files
    # <src>/data
    # <src>/<uuid>/a/data
    # <src>/<uuid>/b/data
    target = config.artifact_sources[0] / str(uuid4())
    subdir_1 = "a"
    subdir_2 = "b"
    data_0 = b"test"
    data_1 = b"test-a"
    data_2 = b"test-b"
    target.mkdir(parents=True, exist_ok=True)
    (target / subdir_1).mkdir()
    (target / subdir_2).mkdir()
    (config.artifact_sources[0] / "data").write_bytes(data_0)
    (target / subdir_1 / "data").write_bytes(data_1)
    (target / subdir_2 / "data").write_bytes(data_2)
    as_path = "different-path"

    # request bundling
    response = client.post(
        "/artifact",
        json={
            "bundle": {
                "targets": [
                    {
                        "path": str(
                            (config.artifact_sources[0] / "data").relative_to(
                                config.FS_MOUNT_POINT.resolve()
                            )
                        ),
                    },
                    {
                        "path": str(
                            target.relative_to(config.FS_MOUNT_POINT.resolve())
                        ),
                        "asPath": as_path,
                    },
                ]
            }
        },
    )

    assert response.status_code == 201
    assert response.mimetype == "application/json"
    token = response.json["value"]

    # wait until job is completed
    app.extensions["orchestra"].stop(stop_on_idle=True)
    json = client.get(f"/artifact/report?token={token}").json

    assert json["data"]["success"]

    # check file contents
    output = config.FS_MOUNT_POINT / str(uuid4())
    with zipfile.ZipFile(
        config.FS_MOUNT_POINT
        / config.ARTIFACT_BUNDLE_DESTINATION
        / json["data"]["bundle"]["id"],
        "r",
    ) as archive:
        archive.extractall(output)
    # * single file without as_path
    assert (
        output
        / config.artifact_sources[0].relative_to(
            config.FS_MOUNT_POINT.resolve()
        )
        / "data"
    ).read_bytes() == data_0
    # * directory with as_path
    assert (output / as_path).is_dir()
    assert (output / as_path / subdir_1).is_dir()
    assert (output / as_path / subdir_2).is_dir()
    assert (output / as_path / subdir_1 / "data").read_bytes() == data_1
    assert (output / as_path / subdir_2 / "data").read_bytes() == data_2
