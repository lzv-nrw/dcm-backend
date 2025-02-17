"""
UserConfig data-model definition
"""

from typing import Optional
from dataclasses import dataclass, field

from dcm_common.models import DataModel


@dataclass(kw_only=True)
class UserConfig(DataModel):
    """
    Data model for a user config.

    This model excludes `_password` and `_active` from serialization.
    See also `UserConfig.with_secret`.

    Keyword arguments:
    user_id -- local user id
    external_id -- external user id
    firstname -- first name
    lastname -- last name
    email -- email address
    roles -- list of the user roles
    _active -- whether the user can log in (has been activated)
               (excluded from default serialization, see
               `with_secret`)
    _password -- local user secret
                 (excluded from default serialization, see
                 `with_secret`)
    """
    user_id: str
    external_id: Optional[str] = None
    firstname: Optional[str] = None
    lastname: Optional[str] = None
    email: Optional[str] = None
    roles: list[str] = field(default_factory=list)
    _active: bool = True
    _password: Optional[str] = None

    @property
    def with_secret(self) -> "_UserConfigWithSecret":
        """Converts this `UserConfig` into a `_UserConfigWithSecret`."""
        return _UserConfigWithSecret(**self.__dict__)

    @DataModel.serialization_handler("user_id", "userId")
    @classmethod
    def user_id_serialization_handler(cls, value):
        """Handles `user_id`-serialization."""
        return value

    @DataModel.deserialization_handler("user_id", "userId")
    @classmethod
    def user_id_deserialization_handler(cls, value):
        """Handles `user_id`-deserialization."""
        return value

    @DataModel.serialization_handler("external_id", "externalId")
    @classmethod
    def external_id_serialization_handler(cls, value):
        """Handles `external_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("external_id", "externalId")
    @classmethod
    def external_id_deserialization_handler(cls, value):
        """Handles `external_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("_active", "active")
    @classmethod
    def _active_deserialization_handler(cls, value):
        """Handles `_active`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("_password", "password")
    @classmethod
    def _password_deserialization_handler(cls, value):
        """Handles `_password`-deserialization."""
        return value

    def set_active(self, value: bool) -> None:
        """Set `active`."""
        self._active = value

    def set_password(self, value: Optional[str]) -> None:
        """Set `_password`."""
        self._password = value


@dataclass
class _UserConfigWithSecret(UserConfig):
    """
    Extension of data model for a user config which includes
    (de-)serialization of `_password`.
    """
    @DataModel.serialization_handler("_active", "active")
    @classmethod
    def _active_serialization_handler(cls, value):
        """Handles `_active`-serialization."""
        return value

    @DataModel.serialization_handler("_password", "password")
    @classmethod
    def _password_serialization_handler(cls, value):
        """Handles `_password`-serialization."""
        return value

    @property
    def active(self) -> bool:
        """Returns `_active`."""
        return self._active

    @property
    def password(self) -> Optional[str]:
        """Returns `_password`."""
        return self._password


@dataclass
class UserCredentials(DataModel):
    """Data model for user credentials."""
    user_id: str
    password: str

    @DataModel.serialization_handler("user_id", "userId")
    @classmethod
    def user_id_serialization_handler(cls, value):
        """Handles `user_id`-serialization."""
        return value

    @DataModel.deserialization_handler("user_id", "userId")
    @classmethod
    def user_id_deserialization_handler(cls, value):
        """Handles `user_id`-deserialization."""
        return value
