"""ArchiveConfiguration-data model test-module."""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import ArchiveAPI
from dcm_backend.models.archive_configuration import (
    ArchiveConfiguration,
    RosettaRestV0Details,
)


test_rosetta_rest_v0_details_json = get_model_serialization_test(
    RosettaRestV0Details,
    (
        (
            (),
            {
                "url": "url-0",
                "material_flow": "mf-0",
                "producer": "p-0",
                "basic_auth": "",
            },
        ),
        (
            (),
            {
                "url": "url-0",
                "material_flow": "mf-0",
                "producer": "p-0",
                "auth_file": Path(__file__),
                "basic_auth": "",
                "proxy": {"http": "url-1"},
            },
        ),
    ),
)


test_archive_configuration_json = get_model_serialization_test(
    ArchiveConfiguration,
    (
        (
            (),
            {
                "id_": "0",
                "name": "n",
                "type_": ArchiveAPI.ROSETTA_REST_V0,
                "details": RosettaRestV0Details("", "", "", basic_auth=""),
                "transfer_destination_id": "1",
            },
        ),
        (
            (),
            {
                "id_": "0",
                "name": "n",
                "type_": ArchiveAPI.ROSETTA_REST_V0,
                "details": RosettaRestV0Details("", "", "", basic_auth=""),
                "transfer_destination_id": "1",
                "description": "d",
            },
        ),
    ),
)
