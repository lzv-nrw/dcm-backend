from typing import Optional
from pathlib import Path
from random import randint
import datetime

import pytest
from dcm_common.services.tests import (
    external_service, run_service, tmp_setup, tmp_cleanup, wait_for_report
)
from dcm_common.util import get_output_path
from dcm_common.util import now

from dcm_backend.config import AppConfig
from dcm_backend import app_factory
from dcm_backend.models import JobConfig, Schedule, Repeat, TimeUnit
from dcm_backend.components import JobProcessorAdapter


@pytest.fixture(scope="session", name="fixtures")
def _fixtures():
    return Path("test_dcm_backend/fixtures/")


@pytest.fixture(scope="session", name="temp_folder")
def _temp_folder():
    return Path("test_dcm_backend/temp_folder/")


@pytest.fixture(name="testing_config")
def _testing_config(fixtures):
    """Returns test-config"""
    # setup config-class
    class TestingConfig(AppConfig):
        ROSETTA_MATERIAL_FLOW = "000000"
        ROSETTA_PRODUCER = "000000"
        ROSETTA_AUTH_FILE = fixtures / ".rosetta/rosetta_auth"
        ARCHIVE_API_BASE_URL = "http://localhost:5050"
        TESTING = True
        ORCHESTRATION_AT_STARTUP = False
        ORCHESTRATION_DAEMON_INTERVAL = 0.001
        ORCHESTRATION_ORCHESTRATOR_INTERVAL = 0.001
        ORCHESTRATION_ABORT_NOTIFICATIONS_STARTUP_INTERVAL = 0.01
        SCHEDULING_AT_STARTUP = False
        SCHEDULING_INTERVAL = 0.001
        JOB_PROCESSOR_POLL_INTERVAL = 0.01

    return TestingConfig


@pytest.fixture(name="test_subdir")
def _test_subdir(temp_folder):
    """Create a test-subdir and returns path relative to `temp_folder`."""
    test_subdir = get_output_path(temp_folder)
    (test_subdir / "payload.txt").touch()
    return test_subdir.relative_to(temp_folder)


@pytest.fixture(name="minimal_request_body")
def _minimal_request_body(test_subdir):
    """Returns minimal request body filled with test-subdir path."""
    return {
        "ingest": {
            "archive_identifier": "rosetta",
            "rosetta": {
                "subdir": str(test_subdir),
            }
        },
    }


@pytest.fixture(name="client")
def _client(testing_config):
    """
    Returns test_client.
    """

    return app_factory(testing_config()).test_client()


@pytest.fixture(name="subdirectory")
def _subdirectory():
    return "subdirectory"


@pytest.fixture(name="producer")
def _producer():
    return "1234"


@pytest.fixture(name="material_flow")
def _material_flow():
    return "5678"


@pytest.fixture(name="deposit_response")
def _deposit_response(subdirectory, producer, material_flow):
    def _(_deposit_id):
        date_now = datetime.datetime.now()
        return {
            "subdirectory": subdirectory,
            "id": _deposit_id,
            "creation_date":
                date_now.strftime("%d/%m/%Y %H:%M:%S"),
            "submission_date":
                (
                    date_now + datetime.timedelta(seconds=10)
                ).strftime("%d/%m/%Y %H:%M:%S"),
            "update_date":
                (
                    date_now + datetime.timedelta(seconds=20)
                ).strftime("%d/%m/%Y %H:%M:%S"),
            "status": "INPROCESS",
            "title": None,
            "producer_agent": {
                "value": "123456789",
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
            "sip_id": "101010",
            "sip_reason": "Files Rejected",
            "link": "/rest/v0/deposits/" + str(_deposit_id)
        }
    return _


@pytest.fixture(name="run_rosetta_dummy")
def _run_rosetta_dummy(run_service, deposit_response):
    """ run dummy Rosetta instance """
    def _(
        post_response: Optional[dict | str] = None,
        get_response: Optional[dict | str] = None,
        post_error_code: Optional[int] = None,
        get_error_code: Optional[int] = None,
        request_id: Optional[str] = None
    ):
        """
        Keyword arguments:
        post_response -- response dict or string for a post request
                         (default None leads to a response dict with random
                         deposit id)
        get_response -- response dict or string for a get request
                        (default None leads to a response dict with random
                        deposit id)
        post_error_code -- error code for a post request
                           (default None leads to status code 200)
        get_error_code -- error code for a get request
                          (default None leads to status code 200)
        request_id -- the id to create the url for the 'get' method
                      (default None leads to using response["id"] for the url)
        """

        if post_error_code is None:
            post_error_code = 200
        if get_error_code is None:
            get_error_code = 200

        _deposit_id = str(randint(1000, 9999))
        if get_response is None:
            get_response = deposit_response(_deposit_id)
        if post_response is None:
            post_response = deposit_response(_deposit_id)

        # create id for 'get' url
        if request_id:
            _id = request_id
        elif isinstance(get_response, dict) and "id" in get_response:
            _id = get_response["id"]
        else:
            _id = "0000"

        run_service(
            routes=[
                (
                    f"/rest/v0/deposits/{_id}",
                    lambda: (get_response, get_error_code),
                    ["GET"]
                ),
                (
                    "/rest/v0/deposits",
                    lambda: (post_response, post_error_code),
                    ["POST"]
                )
            ],
            port=5050
        )
    return _


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


@pytest.fixture(name="job_id")
def _job_id():
    return "0000"


@pytest.fixture(name="job_config")
def _job_config(job_id):
    return JobConfig(
        id_=job_id,
        last_modified=now().isoformat(),
        job={"process": {"from": "transfer", "args": {}}},
        schedule=Schedule(
            active=True,
            repeat=Repeat(unit=TimeUnit.SECOND, interval=1)
        )
    )


@pytest.fixture(name="job_processor_adapter")
def _job_processor_adapter(processor_url):
    return JobProcessorAdapter(processor_url)


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
