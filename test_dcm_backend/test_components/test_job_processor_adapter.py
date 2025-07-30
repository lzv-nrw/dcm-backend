"""JobProcessorAdapter-component test-module."""

import pytest
from dcm_common.services import APIResult

from dcm_backend.util import DemoData
from dcm_backend.models import ImportSource, JobConfig
from dcm_backend.components import JobProcessorAdapter


class FakeDB:
    """Fake SQL-database"""
    TABLES = {}

    def __init__(self, tables=None):
        self.TABLES.update(tables or {})

    def get_row(self, table, value):
        """Get row"""
        class FakeTransactionResult:
            """Fake transaction result"""
            @staticmethod
            def eval(*args, **kwargs):
                """Fake eval"""
                return self.TABLES.get(table, {}).get(value)
        return FakeTransactionResult()

    def get_rows(self, table):
        """Get rows"""
        class FakeTransactionResult:
            """Fake transaction result"""
            @staticmethod
            def eval(*args, **kwargs):
                """Fake eval"""
                return self.TABLES.get(table, {}).values()
        return FakeTransactionResult()


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return JobProcessorAdapter(FakeDB(), url)


@pytest.fixture(name="target")
def _target():
    return None


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "process": {"from": "transfer", "args": {}}, "id": "abcdef-123456"
    }


@pytest.fixture(name="token")
def _token():
    return {
        "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
        "expires": True,
        "expires_at": "2024-08-09T13:15:10+00:00"
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
                        "transfer": {
                            "completed": True,
                            "success": True,
                            "logId": "0@transfer_module"
                        },
                        "ingest": {
                            "completed": True,
                            "success": True,
                            "logId": "1@backend"
                        }
                    }
                }
            }
        }
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
        port=port
    )


@pytest.fixture(name="job_processor_fail")
def _job_processor_fail(port, token, report_fail, run_service):
    run_service(
        routes=[
            ("/process", lambda: (token, 201), ["POST"]),
            ("/progress", lambda: (report_fail["progress"], 200), ["GET"]),
            ("/report", lambda: (report_fail, 200), ["GET"]),
        ],
        port=port
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
    adapter: JobProcessorAdapter, request_body, target, report_fail, job_processor_fail
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


def test_build_request_body_draft_job():
    """
    Test method `JobProcessorAdapter.build_request_body` for draft job
    configuration.
    """
    adapter = JobProcessorAdapter(
        FakeDB(
            {
                "templates": {
                    DemoData.template1: {
                        "id": DemoData.template1,
                        "status": "ok",
                    }
                },
            }
        ),
        "",
    )
    with pytest.raises(ValueError) as exc_info:
        adapter.build_request_body(
            JobConfig(
                template_id=DemoData.template1,
                status="draft",
                id_=DemoData.job_config1,
            )
        )

    assert DemoData.job_config1 in str(exc_info.value)


def test_build_request_body_draft_template():
    """
    Test method `JobProcessorAdapter.build_request_body` for draft
    template configuration.
    """
    adapter = JobProcessorAdapter(
        FakeDB(
            {
                "templates": {
                    DemoData.template1: {
                        "id": DemoData.template1,
                        "status": "draft",
                    }
                },
            }
        ),
        "",
    )
    with pytest.raises(ValueError) as exc_info:
        adapter.build_request_body(
            JobConfig(
                template_id=DemoData.template1,
                status="ok",
                id_=DemoData.job_config1,
            )
        )

    assert DemoData.template1 in str(exc_info.value)


def test_build_request_body_from():
    """
    Test method `JobProcessorAdapter.build_request_body` with acceptable
    input.
    """
    adapter = JobProcessorAdapter(
        FakeDB(
            {
                "templates": {
                    DemoData.template1: {
                        "id": DemoData.template1,
                        "status": "ok",
                        "type": "hotfolder",
                        "additional_information": {
                            "source_id": DemoData.hotfolder_import_source1
                        }
                    },
                    DemoData.template2: {
                        "id": DemoData.template2,
                        "status": "ok",
                        "type": "plugin",
                        "additional_information": {
                            "plugin": "plugin",
                            "args": {}
                        }
                    }
                },
                "hotfolder_import_sources": {
                    DemoData.hotfolder_import_source1: {
                        "id": DemoData.hotfolder_import_source1,
                        "name": "some source",
                        "path": "some/path",
                    }
                }
            }
        ),
        "",
    )
    assert (
        adapter.build_request_body(
            JobConfig(
                template_id=DemoData.template1,
                status="ok",
                id_=DemoData.job_config1,
            )
        )["process"]["from"]
        == "import_ips"
    )
    assert (
        adapter.build_request_body(
            JobConfig(
                template_id=DemoData.template2,
                status="ok",
                id_=DemoData.job_config2,
            )
        )["process"]["from"]
        == "import_ies"
    )


def test_build_request_body_import_ies_plugin():
    """
    Test method `JobProcessorAdapter.build_request_body_import_ies` for
    plugin-import.
    """
    plugin = {"plugin": "demo", "args": {"arg0": "value0"}}

    request_body = {}
    JobProcessorAdapter.build_request_body_import_ies(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "plugin",
            "additionalInformation": plugin,
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
        },
    )

    assert request_body == {
        "import": {
            "plugin": plugin["plugin"],
            "args": plugin["args"] | {"test": False},
        }
    }


def test_build_request_body_import_ies_plugin_missing_plugin():
    """
    Test method `JobProcessorAdapter.build_request_body_import_ies` for
    plugin-import.
    """

    request_body = {}
    with pytest.raises(ValueError):
        JobProcessorAdapter.build_request_body_import_ies(
            request_body,
            {
                "id": DemoData.template1,
                "status": "ok",
                "type": "plugin",
                "additionalInformation": {},
            },
            {
                "id": DemoData.job_config1,
                "templateId": DemoData.template1,
                "status": "ok",
            },
        )


def test_build_request_body_import_ies_plugin_test_mode():
    """
    Test method `JobProcessorAdapter.build_request_body_import_ies` for
    plugin-import.
    """
    plugin = {"plugin": "demo", "args": {}}

    request_body = {}
    JobProcessorAdapter.build_request_body_import_ies(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "plugin",
            "additionalInformation": plugin,
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
        },
        True,
    )

    assert request_body["import"]["args"]["test"] is True


@pytest.mark.parametrize(
    "test_mode",
    (True, False),
)
def test_build_request_body_test_mode_pass_through(test_mode):
    """
    Test `test_mode`-flag propagation through method
    `JobProcessorAdapter.build_request_body`.
    """
    adapter = JobProcessorAdapter(
        FakeDB(
            {
                "templates": {
                    DemoData.template1: {
                        "id": DemoData.template1,
                        "status": "ok",
                        "type": "hotfolder",
                        "additional_information": {
                            "source_id": DemoData.hotfolder_import_source1
                        }
                    },
                    DemoData.template2: {
                        "id": DemoData.template2,
                        "status": "ok",
                        "type": "oai",
                        "additional_information": {
                            "transfer_url_filters": []
                        }
                    }
                },
                "hotfolder_import_sources": {
                    DemoData.hotfolder_import_source1: {
                        "id": DemoData.hotfolder_import_source1,
                        "name": "some source",
                        "path": "some/path",
                    }
                }
            }
        ),
        "",
    )
    assert (
        adapter.build_request_body(
            JobConfig(
                template_id=DemoData.template1,
                status="ok",
                id_=DemoData.job_config1,
            ),
            {},
            test_mode,
        )["process"]["args"]["import_ips"]["import"].get("test", False)
        is test_mode
    )
    assert (
        adapter.build_request_body(
            JobConfig(
                template_id=DemoData.template2,
                status="ok",
                id_=DemoData.job_config2,
            ),
            {},
            test_mode,
        )["process"]["args"]["import_ies"]["import"]["args"].get("test", False)
        is test_mode
    )


def test_build_request_body_import_ies_oai_pmh():
    """
    Test method `JobProcessorAdapter.build_request_body_import_ies` for
    oai-import.
    """
    url = "https://lzv.nrw"
    prefix = "oai_dc"
    transfer_url_filters = [
        {
            "regex": r"https://lzv.nrw\?transfer=[0-9]+",
            "path": "a/b",
        },
        {
            "regex": r"https://lzv.nrw\?transfer=[0-9]+"
        }
    ]
    sets = ["set0", "set1"]
    from_ = "a"
    until = "b"
    identifiers = ["0", "1"]

    request_body = {}
    JobProcessorAdapter.build_request_body_import_ies(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "oai",
            "additionalInformation": {
                "url": url,
                "metadataPrefix": prefix,
                "transferUrlFilters": transfer_url_filters,
            },
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
            "dataSelection": {
                "identifiers": identifiers,
                "sets": sets,
                "from": from_,
                "until": until,
            }
        },
    )

    assert request_body == {
        "import": {
            "plugin": "oai_pmh_v2",
            "args": {
                "base_url": url,
                "metadata_prefix": prefix,
                "transfer_url_info": [
                    {
                        "regex": transfer_url_filters[0]["regex"],
                        "path": transfer_url_filters[0]["path"],
                    },
                    {
                        "regex": transfer_url_filters[1]["regex"],
                    }
                ],
                "identifiers": identifiers,
                "set_spec": sets,
                "from_": from_,
                "until": until,
                "test": False
            },
        }
    }


def test_build_request_body_import_ies_oai_pmh_test_mode():
    """
    Test method `JobProcessorAdapter.build_request_body_import_ies` for
    oai-import.
    """
    request_body = {}
    JobProcessorAdapter.build_request_body_import_ies(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "oai",
            "additionalInformation": {},
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
        },
        True,
    )

    assert request_body == {
        "import": {
            "plugin": "oai_pmh_v2",
            "args": {
                "base_url": None,
                "metadata_prefix": None,
                "test": True
            },
        }
    }


def test_build_request_body_build_ip_hotfolder():
    """
    Test method `JobProcessorAdapter.build_request_body_build_ip` for
    hotfolder-import.
    """

    request_body = {}
    JobProcessorAdapter.build_request_body_build_ip(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "hotfolder",
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
        },
    )

    assert request_body == {}


def test_build_request_body_build_ip_non_hotfolder_mapper():
    """
    Test method `JobProcessorAdapter.build_request_body_build_ip` for
    non-hotfolder-import with mapper/mapping-script.
    """
    mapper = "mapper source code"

    request_body = {}
    JobProcessorAdapter.build_request_body_build_ip(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "plugin",
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
            "dataProcessing": {
                "mapping": {"type": "python", "data": {"contents": mapper}}
            },
        },
    )

    assert request_body == {
        "build": {
            "mappingPlugin": {
                "plugin": "generic-mapper-plugin-string",
                "args": {"mapper": {"string": mapper, "args": {}}},
            },
            "validate": False,
        }
    }


def test_build_request_body_build_ip_non_hotfolder_xslt():
    """
    Test method `JobProcessorAdapter.build_request_body_build_ip` for
    non-hotfolder-import with xslt/mapping-script.
    """
    xslt = "xslt source code"

    request_body = {}
    JobProcessorAdapter.build_request_body_build_ip(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "plugin",
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
            "dataProcessing": {
                "mapping": {"type": "xslt", "data": {"contents": xslt}}
            },
        },
    )

    assert request_body == {
        "build": {
            "mappingPlugin": {
                "plugin": "xslt-plugin",
                "args": {"xslt": xslt},
            },
            "validate": False,
        }
    }


def test_build_request_body_build_ip_non_hotfolder_plugin():
    """
    Test method `JobProcessorAdapter.build_request_body_build_ip` for
    non-hotfolder-import with plugin.
    """
    plugin = {"plugin": "demo", "args": {"arg0": "value0"}}

    request_body = {}
    JobProcessorAdapter.build_request_body_build_ip(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "plugin",
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
            "dataProcessing": {
                "mapping": {"type": "plugin", "data": plugin}
            }
        },
    )

    assert request_body == {
        "build": {"mappingPlugin": plugin, "validate": False}
    }


def test_build_request_body_import_ips_non_hotfolder():
    """
    Test method `JobProcessorAdapter.build_request_body_import_ips` for
    non-hotfolder-import.
    """

    request_body = {}
    JobProcessorAdapter.build_request_body_import_ips(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "plugin",
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
        },
        []
    )

    assert request_body == {}


def test_build_request_body_import_ips_test_mode():
    """
    Test method `JobProcessorAdapter.build_request_body_import_ips` for
    test-import.
    """
    id_ = "some-id"
    path = "this/is/the/path"

    request_body = {}
    JobProcessorAdapter.build_request_body_import_ips(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "hotfolder",
            "additionalInformation": {"sourceId": id_},
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
        },
        [
            ImportSource(id_, "src1", path),
        ],
        True
    )

    assert request_body == {
        "import": {"target": {"path": path}, "test": True}
    }


def test_build_request_body_import_ips_hotfolder():
    """
    Test method `JobProcessorAdapter.build_request_body_import_ips` for
    hotfolder-import.
    """
    id_ = "some-id"
    path = "this/is/the/path"
    subdir = "hotfolder/subdir"

    request_body = {}
    JobProcessorAdapter.build_request_body_import_ips(
        request_body,
        {
            "id": DemoData.template1,
            "status": "ok",
            "type": "hotfolder",
            "additionalInformation": {"sourceId": id_},
        },
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
            "dataSelection": {"path": subdir},
        },
        [
            ImportSource("not-it", "src0", "not-the-path"),
            ImportSource(id_, "src1", path),
        ],
    )

    assert request_body == {
        "import": {"target": {"path": path + "/" + subdir}, "test": False}
    }


def test_build_request_body_import_ips_hotfolder_unknown_source():
    """
    Test method `JobProcessorAdapter.build_request_body_import_ips` for
    hotfolder-import and unknown import source.
    """
    id_ = "unknown-id"

    request_body = {}
    with pytest.raises(ValueError) as exc_info:
        JobProcessorAdapter.build_request_body_import_ips(
            request_body,
            {
                "id": DemoData.template1,
                "status": "ok",
                "type": "hotfolder",
                "additionalInformation": {"sourceId": id_},
            },
            {
                "id": DemoData.job_config1,
                "templateId": DemoData.template1,
                "status": "ok",
            },
            [],
        )

    assert id_ in str(exc_info.value)


def test_build_request_body_validate_payload():
    """
    Test method `JobProcessorAdapter.build_request_body_validate_payload`.
    """

    request_body = {}
    JobProcessorAdapter.build_request_body_validate_payload(request_body)

    assert request_body == {
        "validation": {
            "plugins": {
                "integrity": {"plugin": "integrity-bagit", "args": {}},
                "format": {"plugin": "jhove-fido-mimetype-bagit", "args": {}},
            }
        }
    }


def test_build_request_body_prepare_ip():
    """
    Test method `JobProcessorAdapter.build_request_body_prepare_ip`.
    """

    _operations = [
        {"type": "complement", "targetField": "Field1", "value": "value"},
        {
            "type": "overwriteExisting",
            "targetField": "Field2",
            "value": "value",
        },
        {
            "type": "findAndReplace",
            "targetField": "Field3",
            "items": [{"regex": r".*", "value": "value"}],
        },
    ]

    request_body = {}
    JobProcessorAdapter.build_request_body_prepare_ip(
        request_body,
        {
            "id": DemoData.job_config1,
            "templateId": DemoData.template1,
            "status": "ok",
            "dataProcessing": {
                "preparation": {
                    "rightsOperations": _operations[0:2],
                    "sigPropOperations": _operations[1:],
                    "preservationOperations": _operations[2:3],
                }
            },
        },
    )

    assert request_body == {
        "preparation": {
            "bagInfoOperations": _operations[0:3],
            "sigPropOperations": _operations[1:],
        }
    }


def test_build_request_body_ingest():
    """
    Test method `JobProcessorAdapter.build_request_body_ingest`.
    """

    request_body = {}
    JobProcessorAdapter.build_request_body_ingest(request_body)

    assert request_body == {
        "ingest": {"archiveId": "", "target": {}}
    }
