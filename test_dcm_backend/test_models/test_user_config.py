"""UserConfig-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models.user_config import (
    UserConfig,
    UserSecrets,
    UserCredentials,
    GroupMembership
)


test_group_membership_json = get_model_serialization_test(
    GroupMembership,
    (
        ((), {"id_": "group0"}),
        ((), {"id_": "group0", "workspace": "workspace01"}),
    ),
)


test_user_config_json = get_model_serialization_test(
    UserConfig,
    (
        ((), {}),
        (
            (),
            {
                "id_": "a",
                "external_id": "b",
                "username": "a",
                "status": "ok",
                "firstname": "c",
                "lastname": "d",
                "groups": [
                    GroupMembership("group1"),
                    GroupMembership("group2", "workspace01"),
                ],
                "widget_config": {"widget0": {"arg0": 0, "arg1": "a"}},
                "user_created": "a",
                "datetime_created": "0",
                "user_modified": "b",
                "datetime_modified": "1",
            },
        ),
    ),
)


test_user_secrets_json = get_model_serialization_test(
    UserSecrets,
    (
        ((), {}),
        (
            (),
            {"id_": "a", "user_id": "b", "password": "c", "password_raw": "d"},
        ),
    ),
)


test_user_credentials_json = get_model_serialization_test(
    UserCredentials,
    (
        (("user0", "password"), {}),
    )
)


def test_user_config_row_from_row():
    """Test database-serialization."""
    row = UserConfig(
        id_="a",
        external_id="b",
        username="a",
        status="ok",
        firstname="c",
        lastname="d",
        groups=[
            GroupMembership("group1"),
            GroupMembership("group2", "workspace01"),
        ],
        widget_config={"widget0": {"arg0": 0, "arg1": "a"}},
        user_created="a",
        datetime_created="0",
        user_modified="b",
        datetime_modified="1",
    ).row
    assert UserConfig.from_row(row).row == row
