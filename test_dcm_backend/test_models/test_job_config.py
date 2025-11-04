"""JobConfig-data model test-module."""

from dcm_common.util import now
from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import (
    TimeUnit,
    Repeat,
    Schedule,
    DataSelectionHotfolder,
    DataSelectionOAI,
    PluginConfig,
    FileConfig,
    DataProcessingMappingType,
    DataProcessingMapping,
    DataProcessingPreparation,
    DataProcessing,
    JobConfig,
)


def test_unit_enum():
    """Test enums of class `TimeUnit`."""
    assert TimeUnit.MINUTE.value == "minute"
    assert TimeUnit("day") == TimeUnit.DAY


test_repeat_json = get_model_serialization_test(
    Repeat, (
        ((TimeUnit.DAY, 1), {}),
    )
)


test_schedule_json = get_model_serialization_test(
    Schedule, (
        ((True,), {}),
        ((True,), {"start": "2024-01-01T00:00:00+01:00"}),
        ((True,), {"start": now()}),
        ((True,), {"end": "2024-01-01T00:00:00+01:00"}),
        ((True,), {"end": now()}),
        ((True,), {"repeat": Repeat(TimeUnit.DAY, 1)}),
    )
)


test_data_processing_mapping_json = get_model_serialization_test(
    DataProcessingMapping, (
        ((DataProcessingMappingType.PLUGIN, PluginConfig("", {})), {}),
        ((DataProcessingMappingType.XSLT, FileConfig("", "", "")), {}),
        ((DataProcessingMappingType.PYTHON, FileConfig("", "", "")), {}),
    )
)


test_jobconfig_json = get_model_serialization_test(
    JobConfig,
    (
        ((), {"template_id": "t1", "status": "draft"}),  # minimal
        (  # hotfolder
            (),
            {
                "template_id": "t1",
                "status": "draft",
                "data_selection": DataSelectionHotfolder(path="some/path"),
            },
        ),
        (  # oai
            (),
            {
                "template_id": "t1",
                "status": "draft",
                "data_selection": DataSelectionOAI(
                    identifiers=[
                        "oai:lzv.nrw:46ddd9fc-2b0c-4c3b-a490-0ed4087238fc"
                    ],
                    sets=["set0"],
                    from_="2023-08-04",
                    until="2023-08-05",
                ),
            },
        ),
        (  # complete
            (),
            {
                "template_id": "t1",
                "status": "ok",
                "id_": "c1",
                "latest_exec": "token0",
                "name": "Job Config 1",
                "description": "description of Job Config 1",
                "contact_info": "contact info",
                "data_selection": DataSelectionHotfolder(path="some/path"),
                "data_processing": DataProcessing(
                    mapping=DataProcessingMapping(
                        type_=DataProcessingMappingType.PLUGIN,
                        data=PluginConfig(plugin="demo", args={}),
                    ),
                    preparation=DataProcessingPreparation(
                        rights_operations=[{"type_": "a"}, {"type_": "b"}],
                        sig_prop_operations=[{"type_": "a"}, {"type_": "b"}],
                        preservation_operations=[{"type_": "a"}, {"type_": "b"}],
                    ),
                ),
                "schedule": Schedule(
                    active=True,
                    start="2024-01-01T00:00:00+01:00",
                    end="2024-01-01T00:00:00+01:00",
                    repeat=Repeat(unit=TimeUnit.DAY, interval=1),
                ),
                "workspace_id": "ws1",
                "scheduled_exec": now(False),
                "ies": 5,
                "user_created": "a",
                "datetime_created": "0",
                "user_modified": "b",
                "datetime_modified": "1",
            },
        ),
    ),
)


def test_job_config_row_from_row():
    """Test database-serialization."""
    row = JobConfig(
        template_id="t1",
        status="ok",
        id_="c1",
        latest_exec="token0",
        name="Job Config 1",
        description="description of Job Config 1",
        contact_info="contact info",
        data_selection=DataSelectionHotfolder(path="some/path"),
        data_processing=DataProcessing(
            mapping=DataProcessingMapping(
                type_=DataProcessingMappingType.PLUGIN,
                data=PluginConfig(plugin="demo", args={}),
            ),
            preparation=DataProcessingPreparation(
                rights_operations=[{"type_": "a"}, {"type_": "b"}],
                sig_prop_operations=[{"type_": "a"}, {"type_": "b"}],
                preservation_operations=[{"type_": "a"}, {"type_": "b"}],
            ),
        ),
        schedule=Schedule(
            active=True,
            start="2024-01-01T00:00:00+01:00",
            end="2024-01-01T00:00:00+01:00",
            repeat=Repeat(unit=TimeUnit.DAY, interval=1),
        ),
        workspace_id="ws1",
        scheduled_exec=now(False),
        user_created="a",
        datetime_created="0",
        user_modified="b",
        datetime_modified="1",
    ).row
    assert JobConfig.from_row(row).row == row
