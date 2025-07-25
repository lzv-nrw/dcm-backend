"""WorkspaceConfig-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models.workspace_config import WorkspaceConfig


test__workspace_config_json = get_model_serialization_test(
    WorkspaceConfig,
    (
        ((), {"name": "a"}),
        ((), {"name": "Display Name", "templates": []}),
        ((), {"id_": "a", "name": "Display Name", "users": []}),
        ((), {"id_": "a", "name": "Display Name", "templates": []}),
        (
            (),
            {"id_": "a", "name": "Display Name", "users": [], "templates": []},
        ),
        (
            (),
            {
                "name": "Display Name",
                "users": ["users01", "users02"],
            },
        ),
        (
            (),
            {
                "name": "Display Name",
                "templates": ["template01", "template02"],
            },
        ),
        (
            (),
            {
                "name": "Display Name",
                "users": ["users01", "users02"],
                "templates": ["template01", "template02"],
            },
        ),
        (
            (),
            {
                "id_": "workspace01",
                "name": "Display Name",
                "templates": ["template01", "template02"],
            },
        ),
        (
            (),
            {
                "id_": "workspace01",
                "name": "Display Name",
                "user_created": "a",
                "datetime_created": "0",
                "user_modified": "b",
                "datetime_modified": "1",
            },
        ),
    ),
)


def test_workspace_config_row_from_row():
    """Test database-serialization."""
    row = WorkspaceConfig(
        id_="a",
        name="Display Name",
        user_created="a",
        datetime_created="0",
        user_modified="b",
        datetime_modified="1",
    ).row
    assert WorkspaceConfig.from_row(row).row == row
