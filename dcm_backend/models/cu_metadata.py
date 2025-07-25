"""
CuMetadata data-model definition
"""

from typing import Optional
from dataclasses import dataclass

from dcm_common.models import DataModel


@dataclass
class CuMetadata(DataModel):
    """
    Data model for create/update-metadata.

    Keyword arguments:
    user_created -- user that issued creation
                    (default None)
    datetime_created -- datetime of creation
                        (default None)
    user_modified -- user that performed last modification
                    (default None)
    datetime_modified -- datetime of last modification
                         (default None)
    """

    user_created: Optional[str] = None
    datetime_created: Optional[str] = None
    user_modified: Optional[str] = None
    datetime_modified: Optional[str] = None

    @DataModel.serialization_handler("user_created", "userCreated")
    @classmethod
    def user_created_serialization_handler(cls, value):
        """Handles `user_created`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("user_created", "userCreated")
    @classmethod
    def user_created_deserialization_handler(cls, value):
        """Handles `user_created`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("datetime_created", "datetimeCreated")
    @classmethod
    def datetime_created_serialization_handler(cls, value):
        """Handles `datetime_created`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("datetime_created", "datetimeCreated")
    @classmethod
    def datetime_created_deserialization_handler(cls, value):
        """Handles `datetime_created`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("user_modified", "userModified")
    @classmethod
    def user_modified_serialization_handler(cls, value):
        """Handles `user_modified`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("user_modified", "userModified")
    @classmethod
    def user_modified_deserialization_handler(cls, value):
        """Handles `user_modified`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("datetime_modified", "datetimeModified")
    @classmethod
    def datetime_modified_serialization_handler(cls, value):
        """Handles `datetime_modified`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("datetime_modified", "datetimeModified")
    @classmethod
    def datetime_modified_deserialization_handler(cls, value):
        """Handles `datetime_modified`-deserialization."""
        if value is None:
            DataModel.skip()
        return value
