"""JobProcessorAdapter-component test-module."""

import pytest
from dcm_common.services import APIResult

from dcm_backend.components import JobProcessorAdapter


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return JobProcessorAdapter(url)


@pytest.fixture(name="target")
def _target():
    return None


@pytest.fixture(name="request_body")
def _request_body():
    return {"process": {"id": "abc"}}


@pytest.fixture(name="token")
def _token():
    return {
        "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
        "expires": True,
        "expires_at": "2024-08-09T13:15:10+00:00",
    }


@pytest.fixture(name="report")
def _report(url, token, request_body):
    return {
        "host": url,
        "token": token,
        "args": request_body,
        "progress": {
            "status": "completed",
            "verbose": "Job terminated normally.",
            "numeric": 100,
        },
        "log": {
            "EVENT": [
                {
                    "datetime": "2024-08-09T12:15:10+00:00",
                    "origin": "Job Processor",
                    "body": "Some event",
                },
            ]
        },
        "data": {
            "success": True,
            "issues": 0,
            "records": {
                "record-0": {
                    "id": "record-0",
                    "started": True,
                    "completed": True,
                    "bitstream": False,
                    "skipObjectValidation": False,
                    "status": "complete",
                    "stages": {},
                }
            },
        },
    }


@pytest.fixture(name="report_fail")
def _report_fail(report):
    report["data"]["success"] = False
    return report


@pytest.fixture(name="job_processor")
def _job_processor(port, token, report, run_service):
    run_service(
        routes=[
            ("/process", lambda: (token, 201), ["POST"]),
            ("/progress", lambda: (report["progress"], 200), ["GET"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port,
    )


@pytest.fixture(name="job_processor_fail")
def _job_processor_fail(port, token, report_fail, run_service):
    run_service(
        routes=[
            ("/process", lambda: (token, 201), ["POST"]),
            ("/progress", lambda: (report_fail["progress"], 200), ["GET"]),
            ("/report", lambda: (report_fail, 200), ["GET"]),
        ],
        port=port,
    )


def test_run(
    adapter: JobProcessorAdapter, request_body, target, report, job_processor
):
    """Test method `run` of `JobProcessorAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert info.completed
    assert info.success
    assert info.report["progress"] == report["progress"]


def test_run_fail(
    adapter: JobProcessorAdapter,
    request_body,
    target,
    report_fail,
    job_processor_fail,
):
    """Test method `run` of `JobProcessorAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert info.completed
    assert info.success
    assert info.report["progress"] == report_fail["progress"]


def test_success(
    adapter: JobProcessorAdapter, request_body, target, job_processor
):
    """Test property `success` of `JobProcessorAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert adapter.success(info)


def test_success_fail(
    adapter: JobProcessorAdapter, request_body, target, job_processor_fail
):
    """Test property `success` of `JobProcessorAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert adapter.success(info)


def test_get_report(
    adapter: JobProcessorAdapter, token, report, job_processor
):
    """Test `get_report` of `JobProcessorAdapter` for full report."""
    assert report == adapter.get_report(token["value"])
