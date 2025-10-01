from typing import Optional
from pathlib import Path
from random import choices
import datetime
import json

from flask import Flask, jsonify, request
import pytest
from dcm_common.services.tests import (
    external_service, run_service, tmp_setup, tmp_cleanup, wait_for_report
)

from dcm_backend.config import AppConfig


@pytest.fixture(scope="session", name="fixtures")
def _fixtures():
    return Path("test_dcm_backend/fixtures/")


@pytest.fixture(scope="session", name="temp_folder")
def _temp_folder():
    return Path("test_dcm_backend/temp_folder/")


@pytest.fixture(scope="session", autouse=True)
def disable_extension_logging():
    """
    Disables the stderr-logging via the helper method `print_status`
    of the `dcm_common.services.extensions`-subpackage.
    """
    # pylint: disable=import-outside-toplevel
    from dcm_common.services.extensions.common import PrintStatusSettings

    PrintStatusSettings.silent = True


@pytest.fixture(name="testing_config")
def _testing_config(fixtures):
    """Returns test-config"""
    # setup config-class
    class TestingConfig(AppConfig):
        TESTING = True

        ARCHIVE_CONTROLLER_DEFAULT_ARCHIVE = "test-archive"
        ARCHIVES_SRC = """[
    {
        "id": "test-archive",
        "name": "Test Archive",
        "type": "rosetta-rest-api-v0",
        "transferDestinationId": "test-archive",
        "details": {
            "url": "http://localhost:5050",
            "materialFlow": "000000",
            "producer": "000000",
            "basicAuth": "Authorization: Basic AAAaaa"
        }
    }
]"""

        ORCHESTRA_DAEMON_INTERVAL = 0.01
        ORCHESTRA_WORKER_INTERVAL = 0.01
        ORCHESTRA_WORKER_ARGS = {"messages_interval": 0.01}
        JOB_PROCESSOR_POLL_INTERVAL = 0.01
        SCHEDULING_AT_STARTUP = False
        DB_ADAPTER_STARTUP_IMMEDIATELY = True
        DB_ADAPTER_STARTUP_INTERVAL = 0.01
        DB_INIT_STARTUP_INTERVAL = 0.01
        SCHEDULER_INIT_STARTUP_INTERVAL = 0.01

        DB_LOAD_SCHEMA = True
        DB_GENERATE_DEMO = True

    return TestingConfig


@pytest.fixture(name="no_orchestra_testing_config")
def _no_orchestra_testing_config(testing_config):
    class NoOrchestraTestingConfig(testing_config):
        testing_config.ORCHESTRA_AT_STARTUP = False

    return NoOrchestraTestingConfig


@pytest.fixture(name="minimal_request_body")
def _minimal_request_body():
    """Returns minimal request body filled with test-subdir path."""
    return {
        "ingest": {
            "archiveId": "test-archive",
            "target": {
                "subdirectory": "/",
            }
        },
    }


@pytest.fixture(name="subdirectory")
def _subdirectory():
    return "subdirectory"


@pytest.fixture(name="producer")
def _producer():
    return "1234"


@pytest.fixture(name="material_flow")
def _material_flow():
    return "5678"


@pytest.fixture(name="rosetta_stub")
def _rosetta_stub():
    """
    Returns Rosetta-stub app.
    """
    def _app_factory(dir_: Optional[Path] = None):
        _app = Flask(__name__)

        deposit_dir = None
        sip_dir = None
        deposit_cache = None
        sip_cache = None
        mem = dir_ is None
        if mem:
            deposit_cache = {}
            sip_cache = {}
        else:
            deposit_dir = dir_ / "deposit"
            sip_dir = dir_ / "sip"

        def create_deposit(subdirectory, producer, material_flow):
            """
            Returns deposit-object as dictionary and writes deposit+sip
            to caches/files.
            """
            deposit_id = None
            for _ in range(10):
                __ = "".join(choices("0123456789", k=4))
                if (dir_ is None and __ not in deposit_cache) or (
                    dir_ is not None and not (dir_ / __).exists()
                ):
                    deposit_id = __
                    break
            if deposit_id is None:
                return jsonify({}), 500
            sip_id = f"SIP{deposit_id}"
            date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            deposit = {
                "subdirectory": subdirectory,
                "id": deposit_id,
                "creation_date": date,
                "submission_date": date,
                "update_date": date,
                "status": "INPROCESS",  # REJECTED, DECLINED, INPROCESS, FINISHED, DELETED, ERROR, IN_HUMAN_STAGE
                "title": None,
                "producer_agent": {
                    "value": "1234",
                    "desc": "Description of the producer agent"
                },
                "producer": {
                    "value": producer,
                    "desc": "Description of the producer"
                },
                "material_flow": {
                    "value": material_flow,
                    "desc": "Description of the material flow"
                },
                "sip_id": sip_id,
                "sip_reason": None,
                "link": "/rest/v0/deposits/" + deposit_id
            }
            sip = {
                "link": "/rest/v0/sips/" + sip_id,
                "id": f"SIP{deposit_id}",
                "externalId": None,
                "externalSystem": None,
                "stage": "Deposit",  # Deposit, Loading, Validation, Assessor, Arranger, Approver, Bytestream, Enrichment, ToPermanent, Finished
                "status": "INPROCESS",  # REJECTED, DECLINED, INPROCESS, FINISHED, DELETED, ERROR, IN_HUMAN_STAGE
                "numberofIEs": "1",
                "iePids": f"IE{deposit_id}",
            }
            if mem:
                deposit_cache[deposit_id] = deposit
                sip_cache[sip_id] = sip
            else:
                deposit_dir.mkdir(parents=True, exist_ok=True)
                sip_dir.mkdir(parents=True, exist_ok=True)
                (deposit_dir / deposit_id).write_text(
                    json.dumps(deposit), encoding="utf-8"
                )
                (sip_dir / sip_id).write_text(
                    json.dumps(sip), encoding="utf-8"
                )
            return deposit

        def get_deposit(deposit_id):
            """
            Returns deposit data from cache/disk or `None` if
            unavailable.
            """
            result = None
            if mem:
                result = deposit_cache.get(deposit_id)
            else:
                if (deposit_dir / deposit_id).is_file():
                    result = json.loads(
                        (deposit_dir / deposit_id).read_text(encoding="utf-8")
                    )
            return result

        def get_sip(sip_id):
            """
            Tries to load data from working dir and returns appropriate
            response + status code.
            """
            result = None
            if mem:
                result = sip_cache.get(sip_id)
            else:
                if (sip_dir / sip_id).is_file():
                    result = json.loads(
                        (sip_dir / sip_id).read_text(encoding="utf-8")
                    )
            if result is None:
                # for some reason requesting a non-existent SIP returns
                # this object
                return {
                    "link": None,
                    "id": None,
                    "externalId": None,
                    "externalSystem": None,
                    "stage": None,
                    "status": None,
                    "numberofIEs": None,
                    "iePids": None,
                }
            return result

        @_app.route("/rest/v0/deposits/<id_>", methods=["GET"])
        def deposits_get(id_: str):
            data = get_deposit(id_)
            if data is None:
                return jsonify(None), 204
            return jsonify(data), 200

        @_app.route("/rest/v0/sips/<id_>", methods=["GET"])
        def sips_get(id_: str):
            data = get_sip(id_)
            if data is None:
                return jsonify(None), 204
            return jsonify(data), 200

        @_app.route("/rest/v0/deposits", methods=["POST"])
        def deposits_post():
            deposit = create_deposit(
                request.json["subdirectory"],
                request.json["producer"]["value"],
                request.json["material_flow"]["value"]
            )
            return jsonify(deposit), 200

        return _app
    return _app_factory()


@pytest.fixture(name="processor_port")
def _processor_port():
    return 5051


@pytest.fixture(name="processor_url")
def _processor_url(processor_port):
    return f"http://localhost:{processor_port}"


@pytest.fixture(name="run_job_processor_dummy")
def _run_job_processor_dummy(run_service, processor_port):
    """ run dummy Job-Processor instance """
    def _(
        post_response: Optional[dict] = None,
        get_response: Optional[dict] = None,
        error_code: Optional[int] = None,
    ):
        """
        Keyword arguments:
        post_response -- response dict for a post request
        get_response -- response dict for a get request
        error_code -- error code (default None leads to status code 200)
        """

        if post_response is None:
            post_response = {}

        if error_code is None:
            get_status_code = 200
            post_status_code = 201
        else:
            get_status_code = error_code
            post_status_code = error_code

        run_service(
            routes=[
                (
                    "/ping",
                    lambda: ("pong", get_status_code),
                    ["GET"]
                ),
                (
                    "/status",
                    lambda: ({"ready": True}, get_status_code),
                    ["GET"]
                ),                (
                    "/process",
                    lambda: (post_response, post_status_code),
                    ["POST"]
                ),
                (
                    "/delete",
                    lambda: (post_response, post_status_code),
                    ["DELETE"]
                ),
                (
                    "/report",
                    lambda: (get_response, get_status_code),
                    ["GET"]
                ),
            ],
            port=processor_port
        )
    return _


@pytest.fixture(name="jp_request_body")
def _jp_request_body():
    return {
        "process": {
            "from": "import_ies",
            "to": "import_ies",
            "args": {
                "import_ies": {}
            }
        }
    }


@pytest.fixture(name="jp_token")
def _jp_token():
    return {
        "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
        "expires": True,
        "expires_at": "2024-08-09T13:15:10+00:00"
    }


@pytest.fixture(name="jp_report")
def _jp_report(processor_url, jp_token, jp_request_body):
    return {
        "host": processor_url,
        "token": jp_token,
        "args": jp_request_body,
        "progress": {
            "status": "completed",
            "verbose": "Job terminated normally.",
            "numeric": 100
        },
        "log": {
            "EVENT": [
                {
                    "datetime": "2024-08-09T12:15:10+00:00",
                    "origin": "Job Processor",
                    "body": "Some event"
                },
            ]
        },
        "data": {
            "success": True,
            "records": {
                "/remote_storage/sip/abcde-12345": {
                    "completed": True,
                    "success": True,
                    "stages": {
                        "import_ies": {
                            "completed": True,
                            "success": True,
                            "logId": "0@import_module"
                        }
                    }
                }
            }
        }
    }
