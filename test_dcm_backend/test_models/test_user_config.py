"""UserConfig-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models.user_config import UserConfig, UserCredentials


test_user_config_json = get_model_serialization_test(
    UserConfig,
    (
        ((), {"user_id": "a"}),
        (
            (),
            {
                "user_id": "a",
                "external_id": "b",
                "firstname": "c",
                "lastname": "d",
                "roles": ["role1", "role2"],
            },
        ),
    ),
)


test_user_credentials_json = get_model_serialization_test(
    UserCredentials,
    (
        (("user0", "password"), {}),
    )
)


def test_user_config_password_default():
    """
    Test default behavior of `UserConfig` regarding (de-)serialization
    of the password-attribute.
    """

    # is (by-default) not serialized
    config = UserConfig(user_id="a", _password="b", _active=False)
    assert config.json == {"userId": "a", "roles": []}

    # is (by-default) deserialized
    # special case where the secret properties like _password should be
    # tested explicitly despite them being internal attributes
    assert (
        # pylint: disable=protected-access
        UserConfig.from_json(config.json | {"password": "b"})._password
        == "b"
    )
    assert (
        # pylint: disable=protected-access
        not UserConfig.from_json(config.json | {"active": False})._active
    )

    # does not have getter methods
    assert not hasattr(config, "password")
    assert not hasattr(config, "active")


def test_user_config_with_secret():
    """
    Test non-default behavior of `UserConfig` regarding
    (de-)serialization of secret attributes.
    """

    # is serialized
    config = UserConfig(user_id="a", _password="b", _active=False)
    assert config.with_secret.json == {
        "userId": "a",
        "password": "b",
        "active": False,
        "roles": []
    }

    # does have getter methods
    assert not config.with_secret.active
    assert config.with_secret.password == "b"
