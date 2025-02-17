"""
User View-class definition
"""

from typing import Optional
import sys

from flask import Blueprint, Response
from argon2 import PasswordHasher
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common.db import KeyValueStoreAdapter
from dcm_common import services
from dcm_common.services.views.interface import View

from dcm_backend.config import AppConfig
from dcm_backend.models import UserConfig, UserCredentials
from dcm_backend import handlers


class UserView(View):
    """
    View-class for managing user-authentication.

    Keyword arguments:
    config -- `AppConfig`-object
    config_db -- adapter for user configuration-database
    password_hasher -- instance of argon2.PasswordHasher
    """

    NAME = "user"

    def __init__(
        self,
        config: AppConfig,
        config_db: KeyValueStoreAdapter,
        password_hasher: PasswordHasher,
    ) -> None:
        super().__init__(config)
        self.config_db = config_db
        self.password_hasher = password_hasher

    def _validate_credentials(
        self, credentials: UserCredentials
    ) -> tuple[bool, Optional[UserConfig]]:
        """
        Returns `True` and an associated `UserConfig` if the
        `credentials` are valid.
        """
        # collect from database
        _config = self.config_db.read(credentials.user_id)
        if _config is None:
            print(
                f"Unknown user '{credentials.user_id}' attempted login.",
                file=sys.stderr,
            )
            return False, None

        # convert into UserConfig
        config = UserConfig.from_json(_config)

        # validate
        try:
            return (
                config.user_id == credentials.user_id
                and self.password_hasher.verify(
                    config.with_secret.password, credentials.password
                ),
                config
            )
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            print(
                f"Failed login attempt for user '{credentials.user_id}': "
                + str(exc_info),
                file=sys.stderr,
            )
            return False, None

    def _login(self, bp: Blueprint):
        @bp.route("/user", methods=["POST"], provide_automatic_options=False)
        @flask_handler(  # process query
            handler=services.handlers.no_args_handler,
            json=flask_args,
        )
        @flask_handler(
            handler=handlers.user_login_handler,
            json=flask_json,
        )
        def login(credentials: UserCredentials):
            """Determine user authentication."""
            valid, config = self._validate_credentials(credentials)
            if not valid:
                return Response("FAILED", mimetype="text/plain", status=401)

            if (
                self.config.REQUIRE_USER_ACTIVATION
                and not config.with_secret.active
            ):
                return Response(
                    "This account is currently inactive, activate by "
                    + "setting initial password.",
                    mimetype="text/plain",
                    status=403,
                )

            # as recommended by the author: check for re-hashing requirement
            # https://argon2-cffi.readthedocs.io/en/stable/api.html#argon2.PasswordHasher.check_needs_rehash
            if self.password_hasher.check_needs_rehash(
                config.with_secret.password
            ):
                print(
                    "Argon2: re-hashing password for user "
                    + f"'{credentials.user_id}'.",
                    file=sys.stderr,
                )
                # replace password and write back with new password
                self.config_db.write(
                    credentials.user_id,
                    UserConfig.from_json(
                        config.json
                        | {
                            "password": self.password_hasher.hash(
                                credentials.password
                            )
                        }
                    ).with_secret.json,
                )

            return Response("OK", mimetype="text/plain", status=200)

    def _change_password(self, bp: Blueprint):
        @bp.route(
            "/user/password", methods=["PUT"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=services.handlers.no_args_handler,
            json=flask_args,
        )
        @flask_handler(
            handler=handlers.user_change_password_handler,
            json=flask_json,
        )
        def change_password(credentials: UserCredentials, new_password: str):
            """Change user password."""
            valid, config = self._validate_credentials(credentials)
            if not valid:
                return Response("FAILED", mimetype="text/plain", status=401)

            # replace password and write back with new password
            self.config_db.write(
                credentials.user_id,
                UserConfig.from_json(
                    config.json
                    | {
                        "password": self.password_hasher.hash(new_password),
                        "active": True
                    }
                ).with_secret.json,
            )

            return Response("OK", mimetype="text/plain", status=200)

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        self._login(bp)
        self._change_password(bp)
