"""Hotfolder-data model test-module."""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import Hotfolder, HotfolderDirectoryInfo


test_hotfolder_json = get_model_serialization_test(
    Hotfolder,
    (
        ((), {"id_": "0", "mount": Path("p"), "name": "n"}),
        (
            (),
            {
                "id_": "0",
                "mount": Path("p"),
                "name": "n",
                "description": "some description",
            },
        ),
    ),
)


test_hotfolder_directory_info_json = get_model_serialization_test(
    HotfolderDirectoryInfo,
    (
        ((), {"name": "n"}),
        ((), {"name": "n", "in_use": True, "linked_job_configs": ["0"]}),
    ),
)
