"""Configuration module for the dcm-backend-app."""

import os
from pathlib import Path
from importlib.metadata import version
import json

import yaml
from dcm_common.services import OrchestratedAppConfig
import dcm_backend_api


class AppConfig(OrchestratedAppConfig):
    """
    Configuration for the dcm-backend-app.
    """

    # disable parallel-deployment
    ORCHESTRATION_ABORT_NOTIFICATIONS = False
    # include this in config for compatible with common orchestration-
    # app extension
    FS_MOUNT_POINT = Path.cwd()


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
    CONFIGURATION_DATABASE_ADAPTER = (
        os.environ.get("CONFIGURATION_DATABASE_ADAPTER")
    )
    CONFIGURATION_DATABASE_SETTINGS = (
        json.loads(os.environ["CONFIGURATION_DATABASE_SETTINGS"])
        if "CONFIGURATION_DATABASE_SETTINGS" in os.environ else None
    )
    REPORT_DATABASE_ADAPTER = (
        os.environ.get("REPORT_DATABASE_ADAPTER")
    )
    REPORT_DATABASE_SETTINGS = (
        json.loads(os.environ["REPORT_DATABASE_SETTINGS"])
        if "REPORT_DATABASE_SETTINGS" in os.environ else None
    )

    # ------ SCHEDULING ------
    SCHEDULING_CONTROLS_API = (
        (int(os.environ.get("SCHEDULING_CONTROLS_API") or 0)) == 1
    )
    SCHEDULING_AT_STARTUP = (
        (int(os.environ.get("SCHEDULING_AT_STARTUP") or 1)) == 1
    )
    SCHEDULING_INTERVAL = float(os.environ.get("SCHEDULING_INTERVAL") or 1.0)
    SCHEDULING_DAEMON_INTERVAL = 1.0

    # ------ JOB ------
    JOB_PROCESSOR_TIMEOUT = int(os.environ.get("JOB_PROCESSOR_TIMEOUT") or 30)
    JOB_PROCESSOR_HOST = (
        os.environ.get("JOB_PROCESSOR_HOST") or "http://localhost:8086"
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

        # configuration-db
        self._config_db_settings = {
            "type": self.CONFIGURATION_DATABASE_ADAPTER or "native",
            "settings": (
                self.CONFIGURATION_DATABASE_SETTINGS
                or {"backend": "memory"}
            )
        }
        self.config_db = self._load_adapter(
            "config_db", self._config_db_settings["type"],
            self._config_db_settings["settings"]
        )

        # report-db
        self._report_db_settings = {
            "type": self.REPORT_DATABASE_ADAPTER or "native",
            "settings": (
                self.REPORT_DATABASE_SETTINGS
                or {"backend": "memory"}
            )
        }
        self.report_db = self._load_adapter(
            "report_db", self._report_db_settings["type"],
            self._report_db_settings["settings"]
        )

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
        settings["database"] = {
            "configuration": self._config_db_settings,
            "report": self._report_db_settings,
        }
        settings["scheduling"] = {
            "controls_api": self.SCHEDULING_CONTROLS_API,
            "at_startup": self.SCHEDULING_AT_STARTUP,
            "interval": self.SCHEDULING_INTERVAL,
        }
        settings["job"] = {
            "timeout": {"duration": self.JOB_PROCESSOR_TIMEOUT},
            "polling_interval": self.JOB_PROCESSOR_POLL_INTERVAL,
        }

        self.CONTAINER_SELF_DESCRIPTION["configuration"]["services"] = {
            "job_processor": self.JOB_PROCESSOR_HOST,
        }
