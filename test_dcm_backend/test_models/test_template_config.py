"""TemplateConfig-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models.template_config import (
    PluginInfo,
    HotfolderInfo,
    TransferUrlFilter,
    OAIInfo,
    TemplateConfig,
)


test_plugin_info_json = get_model_serialization_test(
    PluginInfo,
    (
        ((), {},),
        (
            (),
            {
                "plugin": "p-0",
                "args": {"arg0": "value0"},
            },
        ),
    ),
)


def test_plugin_info_row_from_row():
    """Test database-serialization."""
    row = PluginInfo("p-0", {"arg0": "value0"}).row
    assert PluginInfo.from_row(row).row == row


test_hotfolder_info_json = get_model_serialization_test(
    HotfolderInfo,
    (
        ((), {},),
        (
            (),
            {
                "source_id": "some-id",
            },
        ),
    ),
)


def test_hotfolder_info_row_from_row():
    """Test database-serialization."""
    row = HotfolderInfo("some-id").row
    assert HotfolderInfo.from_row(row).row == row


test_transfer_url_filter_json = get_model_serialization_test(
    TransferUrlFilter,
    (
        (
            (),
            {
                "regex": "",
            },
        ),
        (
            (),
            {
                "regex": r"(https://lzv\.nrw/oai/transfer=[a-z0-9]+)",
                "path": "0",
            },
        ),
    ),
)


def test_transfer_url_filter_row_from_row():
    """Test database-serialization."""
    row = TransferUrlFilter(
        r"(https://lzv\.nrw/oai/transfer=[a-z0-9]+)", "0"
    ).row
    assert TransferUrlFilter.from_row(row).row == row


test_oai_info_json = get_model_serialization_test(
    OAIInfo,
    (
        ((), {},),
        (
            (),
            {
                "url": "url",
                "metadata_prefix": "prefix",
                "transfer_url_filters": [
                    TransferUrlFilter(
                        r"(https://lzv\.nrw/oai/transfer=[a-z0-9]+)",
                        "0",
                    )
                ],
            },
        ),
    ),
)


def test_oai_info_row_from_row():
    """Test database-serialization."""
    row = OAIInfo(
        "url",
        "prefix",
        [
            TransferUrlFilter(
                r"(https://lzv\.nrw/oai/transfer=[a-z0-9]+)",
                "0",
            )
        ],
    ).row
    assert OAIInfo.from_row(row).row == row


test_template_config_json = get_model_serialization_test(
    TemplateConfig,
    (
        ((), {"status": "draft"},),
        (
            (),
            {
                "status": "draft",
                "type_": "plugin",
                "additional_information": PluginInfo("p-0", {}),
            },
        ),
        (
            (),
            {
                "status": "draft",
                "type_": "hotfolder",
                "additional_information": HotfolderInfo("some-id"),
            },
        ),
        (
            (),
            {
                "status": "draft",
                "type_": "oai",
                "additional_information": OAIInfo("url", "prefix", []),
            },
        ),
        (
            (),
            {
                "status": "ok",
                "id_": "a",
                "workspace_id": "ws0",
                "name": "Display Name",
                "description": "some description",
                "type_": "plugin",
                "additional_information": PluginInfo("p-0", {}),
                "user_created": "a",
                "datetime_created": "0",
                "user_modified": "b",
                "datetime_modified": "1",
            },
        ),
    ),
)


def test_template_config_row_from_row():
    """Test database-serialization."""
    row = TemplateConfig(
        status="ok",
        id_="a",
        workspace_id="ws0",
        name="Display Name",
        description="some description",
        type_="plugin",
        additional_information=PluginInfo("p-0", {}),
        user_created="a",
        datetime_created="0",
        user_modified="b",
        datetime_modified="1",
    ).row
    assert TemplateConfig.from_row(row).row == row


def test_template_config_draft_row_from_row():
    """Test database-serialization."""
    row = TemplateConfig(
        status="draft",
    ).row
    assert TemplateConfig.from_row(row).row == row
