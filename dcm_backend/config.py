"""Configuration module for the dcm-backend-app."""

import os
import sys
from pathlib import Path
from importlib.metadata import version
from json import loads

import yaml
from dcm_common.services import OrchestratedAppConfig, FSConfig, DBConfig
from dcm_common.orchestra import dillignore
import dcm_database
import dcm_backend_api

from dcm_backend import util


@dillignore("controller", "worker_pool", "db")
class AppConfig(OrchestratedAppConfig, FSConfig, DBConfig):
    """
    Configuration for the dcm-backend-app.
    """

    # ------ EXTENSIONS ------
    DB_INIT_STARTUP_INTERVAL = 1.0
    SCHEDULER_INIT_STARTUP_INTERVAL = 1.0

    # ------ CLEANUP ------
    CLEANUP_DISABLED = int(os.environ.get("CLEANUP_DISABLED", 0)) == 1
    CLEANUP_TARGETS = os.environ.get(
        "CLEANUP_TARGETS", '["ie", "ip", "pip", "sip"]'
    )
    CLEANUP_INTERVAL = float(os.environ.get("CLEANUP_INTERVAL", 3600))
    CLEANUP_ARTIFACT_TTL = int(
        os.environ.get("CLEANUP_ARTIFACT_TTL", 604800)
    )

    # ------ ARTIFACT ------
    ARTIFACT_COMPRESSION = int(os.environ.get("ARTIFACT_COMPRESSION", 0)) == 1
    ARTIFACT_BUNDLE_DESTINATION = Path(
        os.environ.get("ARTIFACT_BUNDLE_DESTINATION", "bundles")
    )
    ARTIFACT_FILE_MAX_SIZE = int(
        os.environ.get("ARTIFACT_FILE_MAX_SIZE", 0)
    )
    ARTIFACT_BUNDLE_MAX_SIZE = int(
        os.environ.get("ARTIFACT_BUNDLE_MAX_SIZE", 0)
    )
    ARTIFACT_SOURCES = os.environ.get(
        "ARTIFACT_SOURCES", '["ie", "ip", "pip", "sip"]'
    )

    # ------ DATABASE ------
    DB_SCHEMA = Path(dcm_database.__file__).parent / "init.sql"
    DB_LOAD_SCHEMA = (int(os.environ.get("DB_LOAD_SCHEMA") or 0)) == 1
    DB_STRICT_SCHEMA_VERSION = (
        int(os.environ.get("DB_STRICT_SCHEMA_VERSION") or 0)
    ) == 1
    DB_GENERATE_DEMO = (int(os.environ.get("DB_GENERATE_DEMO") or 0)) == 1
    DB_GENERATE_DEMO_USERS = (
        int(os.environ.get("DB_GENERATE_DEMO_USERS") or 1)
    ) == 1
    DB_DEMO_ADMIN_PW = os.environ.get("DB_DEMO_ADMIN_PW")

    # ------ USERS ------
    REQUIRE_USER_ACTIVATION = (
        (int(os.environ.get("REQUIRE_USER_ACTIVATION") or 1)) == 1
    )

    # ------ TEMPLATES ------
    HOTFOLDER_SRC = os.environ.get("HOTFOLDER_SRC", "[]")
    ARCHIVES_SRC = os.environ.get("ARCHIVES_SRC", "[]")

    # ------ SCHEDULING ------
    SCHEDULING_CONTROLS_API = (
        (int(os.environ.get("SCHEDULING_CONTROLS_API") or 0)) == 1
    )
    SCHEDULING_AT_STARTUP = (
        (int(os.environ.get("SCHEDULING_AT_STARTUP") or 1)) == 1
    )
    SCHEDULING_DAEMON_INTERVAL = 1.0
    SCHEDULING_TIMEZONE = os.environ.get("SCHEDULING_TIMEZONE")

    # ------ JOB ------
    JOB_PROCESSOR_TIMEOUT = int(os.environ.get("JOB_PROCESSOR_TIMEOUT") or 30)
    JOB_PROCESSOR_HOST = (
        os.environ.get("JOB_PROCESSOR_HOST") or "http://localhost:8087"
    )
    JOB_PROCESSOR_POLL_INTERVAL = float(
        os.environ.get("JOB_PROCESSOR_POLL_INTERVAL") or 1.0
    )

    # ------ IDENTIFY ------
    # generate self-description
    API_DOCUMENT = \
        Path(dcm_backend_api.__file__).parent / "openapi.yaml"
    API = yaml.load(
        API_DOCUMENT.read_text(encoding="utf-8"),
        Loader=yaml.SafeLoader
    )

    def __init__(self, *args, **kwargs) -> None:
        # load hotfolders
        try:
            hotfolder_src = Path(self.HOTFOLDER_SRC)
            if not hotfolder_src.is_file():
                raise FileNotFoundError("Not a file.")
        except (OSError, FileNotFoundError):
            self.hotfolders = util.load_hotfolders_from_string(
                self.HOTFOLDER_SRC
            )
        else:
            self.hotfolders = util.load_hotfolders_from_file(hotfolder_src)

        # load archives
        try:
            archives_src = Path(self.ARCHIVES_SRC)
            if not archives_src.is_file():
                raise FileNotFoundError("Not a file.")
        except (OSError, FileNotFoundError):
            self.archives = util.load_archive_configurations_from_string(
                self.ARCHIVES_SRC
            )
        else:
            self.archives = util.load_archive_configurations_from_file(
                archives_src
            )

        self.cleanup_targets = self.load_directories_relative_to_fs_mount_point(
            loads(self.CLEANUP_TARGETS),
            "Cleanup target",
            False,
        )

        self.artifact_sources = self.load_directories_relative_to_fs_mount_point(
            # pylint: disable=no-member
            loads(self.ARTIFACT_SOURCES),
            "Artifact source",
            not hasattr(self, "TESTING") or not self.TESTING,
        )

        super().__init__(*args, **kwargs)

    def set_identity(self) -> None:
        super().set_identity()

        self.CONTAINER_SELF_DESCRIPTION["description"] = (
            "This API provides backend-related endpoints."
        )

        # version
        self.CONTAINER_SELF_DESCRIPTION["version"]["api"] = (
            self.API["info"]["version"]
        )
        self.CONTAINER_SELF_DESCRIPTION["version"]["app"] = version(
            "dcm-backend"
        )

        # configuration
        settings = self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
        settings["database"]["schemaVersion"] = version("dcm-database")
        settings["scheduling"] = {
            "controls_api": self.SCHEDULING_CONTROLS_API,
            "at_startup": self.SCHEDULING_AT_STARTUP,
            "timezone": self.SCHEDULING_TIMEZONE,
        }
        settings["job"] = {
            "timeout": {"duration": self.JOB_PROCESSOR_TIMEOUT},
            "polling_interval": self.JOB_PROCESSOR_POLL_INTERVAL,
        }
        settings["user"] = {
            "user_activation": self.REQUIRE_USER_ACTIVATION,
        }

        self.CONTAINER_SELF_DESCRIPTION["configuration"]["services"] = {
            "job_processor": self.JOB_PROCESSOR_HOST,
        }

    def load_directories_relative_to_fs_mount_point(
        self, dirs: list[str], what: str, log: bool
    ) -> list[Path]:
        """
        Transforms list of `dirs` (paths relative to fs-mount-point as
        strings) into a list of absolute `Path`s. In log-messages/errors
        the resource is referred to as `what`. If not `log`, only errors
        are raised.
        """
        result = []
        if not isinstance(dirs, list):
            raise ValueError(
                f"Failed to load {what.lower()}s: Expected array but got "
                + f"{type(dirs).__name__}."
            )
        for p in dirs:
            if not isinstance(p, str):
                raise ValueError(
                    f"Failed to load {what.lower()}s: Expected array of "
                    + f"strings but got array containing {type(p).__name__}."
                )
            if Path(p).is_absolute():
                raise ValueError(
                    f"{what} directory '{p}' is an absolute "
                    + "directory. (Expected a directory relative to "
                    + f"FS_MOUNT_POINT '{self.FS_MOUNT_POINT}'.)"
                )
            _p = (self.FS_MOUNT_POINT / p).resolve()
            if log and not _p.exists():
                print(
                    "\033[1;33m"
                    f"WARNING: {what} directory '{p}' does not exist "
                    + f"in FS_MOUNT_POINT '{self.FS_MOUNT_POINT}'."
                    + "\033[0m",
                    file=sys.stderr,
                )
            if log and not _p.is_dir():
                print(
                    "\033[1;33m"
                    f"WARNING: {what} '{p}' is not a directory."
                    + "\033[0m",
                    file=sys.stderr,
                )
            if self.FS_MOUNT_POINT.resolve() not in _p.parents:
                raise ValueError(
                    f"{what} directory '{p}' points outside of "
                    + f"FS_MOUNT_POINT '{self.FS_MOUNT_POINT}'."
                )
            result.append(_p)
        return result
