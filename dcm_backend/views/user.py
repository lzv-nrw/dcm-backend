"""
User View-class definition
"""

from typing import Optional
import sys

from flask import Blueprint, Response, jsonify
from argon2 import PasswordHasher
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common.db import SQLAdapter
from dcm_common import services
from dcm_common.services.views.interface import View

from dcm_backend.config import AppConfig
from dcm_backend.models import (
    UserConfig,
    GroupMembership,
    UserSecrets,
    UserCredentials,
)
from dcm_backend import handlers


class UserView(View):
    """
    View-class for managing user-authentication.

    Keyword arguments:
    config -- `AppConfig`-object
    db -- database adapter
    password_hasher -- instance of argon2.PasswordHasher
    """

    NAME = "user"

    def __init__(
        self,
        config: AppConfig,
        db: SQLAdapter,
        password_hasher: PasswordHasher,
    ) -> None:
        super().__init__(config)
        self.db = db
        self.password_hasher = password_hasher

    def _validate_credentials(
        self, credentials: UserCredentials
    ) -> tuple[bool, Optional[UserConfig], Optional[UserSecrets]]:
        """
        Returns `True` and an associated `UserConfig` if the
        `credentials` are valid.
        """
        # collect config from database
        _config = self.db.get_rows(
            "user_configs", credentials.username, "username"
        ).eval()
        if len(_config) != 1:
            print(
                f"Unknown user '{credentials.username}' attempted login.",
                file=sys.stderr,
            )
            return False, None, None

        # convert into UserConfig
        config = UserConfig.from_row(_config[0])

        # collect secrets from database
        _secrets = self.db.get_rows(
            "user_secrets", config.id_, "user_id"
        ).eval()
        if len(_secrets) != 1:
            print(
                f"Unable to fetch secrets for user '{credentials.username}'.",
                file=sys.stderr,
            )
            return False, None, None
        secrets = UserSecrets.from_row(_secrets[0])

        # validate
        try:
            return (
                self.password_hasher.verify(
                    secrets.password, credentials.password
                ),
                config,
                secrets,
            )
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            print(
                f"Failed authentication for user '{credentials.username}': "
                + str(exc_info),
                file=sys.stderr,
            )
            return False, None, None

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
            valid, config, secrets = self._validate_credentials(credentials)
            if not valid:
                return Response(
                    "Unauthorized", mimetype="text/plain", status=401
                )

            if (
                self.config.REQUIRE_USER_ACTIVATION
                and config.status == "inactive"
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
                secrets.password
            ):
                print(
                    "Argon2: re-hashing password for user "
                    + f"'{credentials.username}'.",
                    file=sys.stderr,
                )
                # replace password and write back with new password
                secrets.password = self.password_hasher.hash(
                    credentials.password
                )
                self.db.update("user_secrets", secrets.row).eval()

            print(
                f"Successful login for user '{credentials.username}'.",
                file=sys.stderr,
            )
            config.groups.extend(
                map(
                    lambda x: GroupMembership(
                        x["group_id"], x["workspace_id"]
                    ),
                    self.db.get_rows(
                        "user_groups",
                        config.id_,
                        "user_id",
                        ["group_id", "workspace_id"],
                    ).eval(),
                )
            )
            return jsonify(config.json), 200

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
            valid, config, secrets = self._validate_credentials(credentials)
            if not valid:
                return Response(
                    "Unauthorized", mimetype="text/plain", status=401
                )

            with self.db.new_transaction() as t:
                # replace password and write back with new password
                secrets.password = self.password_hasher.hash(new_password)
                t.add_update("user_secrets", secrets.row)

                # update user status
                if config.status == "inactive":
                    print(
                        f"Activating user '{credentials.username}'.",
                        file=sys.stderr,
                    )
                config.status = "ok"
                t.add_update("user_configs", config.row)
            t.result.eval("changing user password")

            print(
                f"Changed password for user '{credentials.username}'.",
                file=sys.stderr,
            )
            return Response("OK", mimetype="text/plain", status=200)

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        self._login(bp)
        self._change_password(bp)
