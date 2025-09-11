"""Configuration module for the dcm-backend-app."""

import os
from pathlib import Path
from importlib.metadata import version
import json

import yaml
from dcm_common.services import OrchestratedAppConfig, DBConfig
from dcm_common.orchestra import dillignore
import dcm_database
import dcm_backend_api


@dillignore("controller", "worker_pool", "db")
class AppConfig(OrchestratedAppConfig, DBConfig):
    """
    Configuration for the dcm-backend-app.
    """

    # include this in config for compatible with common orchestration-
    # app extension
    FS_MOUNT_POINT = Path.cwd()

    # ------ EXTENSIONS ------
    DB_INIT_STARTUP_INTERVAL = 1.0
    SCHEDULER_INIT_STARTUP_INTERVAL = 1.0

    # ------ INGEST (ARCHIVE_CONTROLLER) ------
    ROSETTA_AUTH_FILE = Path(
        os.environ["ROSETTA_AUTH_FILE"] if "ROSETTA_AUTH_FILE" in os.environ
        else (Path.home() / ".rosetta/rosetta_auth")
    )
    ROSETTA_MATERIAL_FLOW = os.environ.get("ROSETTA_MATERIAL_FLOW") \
        or "12345678"
    ROSETTA_PRODUCER = os.environ.get("ROSETTA_PRODUCER") \
        or "12345678"
    ARCHIVE_API_BASE_URL = \
        os.environ.get("ARCHIVE_API_BASE_URL") \
        or "https://lzv-test.hbz-nrw.de"
    ARCHIVE_API_PROXY = (
        json.loads(os.environ["ARCHIVE_API_PROXY"])
        if "ARCHIVE_API_PROXY" in os.environ else None
    )

    # ------ DATABASE ------
    DB_SCHEMA = Path(dcm_database.__file__).parent / "init.sql"
    DB_LOAD_SCHEMA = (int(os.environ.get("DB_LOAD_SCHEMA") or 0)) == 1
    DB_GENERATE_DEMO = (int(os.environ.get("DB_GENERATE_DEMO") or 0)) == 1
    DB_STRICT_SCHEMA_VERSION = (
        int(os.environ.get("DB_STRICT_SCHEMA_VERSION") or 0)
    ) == 1

    # ------ USERS ------
    DB_GENERATE_DEMO_USERS = (
        int(os.environ.get("DB_GENERATE_DEMO_USERS") or 0)
    ) == 1
    DB_DEMO_ADMIN_PW = os.environ.get("DB_DEMO_ADMIN_PW")
    REQUIRE_USER_ACTIVATION = (
        (int(os.environ.get("REQUIRE_USER_ACTIVATION") or 1)) == 1
    )

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
        settings["ingest"] = {
            "archive_identifier": "rosetta",
            "archive_settings": {
                "auth": str(self.ROSETTA_AUTH_FILE),
                "material_flow": self.ROSETTA_MATERIAL_FLOW,
                "producer": self.ROSETTA_PRODUCER
            },
            "network": {
                "url": self.ARCHIVE_API_BASE_URL
            }
        }
        if self.ARCHIVE_API_PROXY is not None:
            settings["ingest"]["proxy"] = self.ARCHIVE_API_PROXY
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
