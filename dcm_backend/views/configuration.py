"""
Configuration View-class definition
"""

from typing import Optional
from hashlib import md5
from uuid import uuid4

from flask import Blueprint, jsonify, Response
from argon2 import PasswordHasher
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common.db import KeyValueStoreAdapter
from dcm_common import services
from dcm_common.services.views.interface import View

from dcm_backend.config import AppConfig
from dcm_backend.models import JobConfig, UserConfig
from dcm_backend.components import Scheduler
from dcm_backend import handlers


class ConfigurationView(View):
    """
    View-class for managing configurations of
    * jobs
    * users

    Keyword arguments:
    config -- `AppConfig`-object
    job_config_db -- adapter for job configuration-database
    user_config_db -- adapter for user configuration-database
    scheduler -- `Scheduler`-object
    password_hasher -- instance of argon2.PasswordHasher
    """

    NAME = "configuration"

    def __init__(
        self,
        config: AppConfig,
        job_config_db: KeyValueStoreAdapter,
        user_config_db: KeyValueStoreAdapter,
        scheduler: Scheduler,
        password_hasher: PasswordHasher
    ) -> None:
        super().__init__(config)
        self.job_config_db = job_config_db
        self.user_config_db = user_config_db
        self.scheduler = scheduler
        self.password_hasher = password_hasher

    def _get_job_config(self, bp: Blueprint):
        @bp.route(
            "/job/configure", methods=["GET"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def get_config(id_: str):
            """Fetch job config associated with given `id_`."""
            config = self.job_config_db.read(id_)
            if config:
                return jsonify(config), 200
            return Response(
                f"Unknown config '{id_}'.", mimetype="text/plain", status=404
            )

    def _post_job_config(self, bp: Blueprint):
        @bp.route(
            "/job/configure", methods=["POST"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.job_config_post_handler,
            json=flask_json,
        )
        def post_config(config: JobConfig):
            """Write job config. Set id and name if missing."""
            if config.id_ is None:
                config.id_ = self.job_config_db.push({})
            if config.name is None:
                config.name = f"Unnamed Config {config.id_[0:8]}"
            self.job_config_db.write(config.id_, config.json)
            self.scheduler.schedule(config)
            return jsonify({"id": config.id_, "name": config.name}), 200

    def _list_job_configs(self, bp: Blueprint):
        @bp.route(
            "/job/configure",
            methods=["OPTIONS"],
            provide_automatic_options=False
        )
        def list_configs():
            """List ids of available job configs."""
            return jsonify(self.job_config_db.keys()), 200

    def _delete_job_config(self, bp: Blueprint):
        @bp.route(
            "/job/configure",
            methods=["DELETE"],
            provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def delete_config(id_: str):
            """Delete job config by `id_`."""
            self.job_config_db.delete(id_)
            self.scheduler.schedule(JobConfig({}, id_=id_))
            return Response(
                f"Deleted config '{id_}'.", mimetype="text/plain", status=200
            )

    def _list_users(self, bp: Blueprint):
        @bp.route(
            "/user/configure",
            methods=["OPTIONS"],
            provide_automatic_options=False
        )
        def list_users():
            """List ids of existing users."""
            return jsonify(self.user_config_db.keys()), 200

    def _get_user_config(self, bp: Blueprint):
        @bp.route(
            "/user/configure", methods=["GET"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def get_user_config(id_: str):
            """Fetch user config associated with given `id_`."""
            config = self.user_config_db.read(id_)
            if config:
                # this back and forth transformation is used to remove
                # any secrets from the response
                return jsonify(UserConfig.from_json(config).json), 200
            return Response(
                f"Unknown user '{id_}'.", mimetype="text/plain", status=404
            )

    def _put_user_config(self, bp: Blueprint):
        @bp.route(
            "/user/configure",
            methods=["PUT"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.user_config_post_handler,
            json=flask_json,
        )
        def put_user_config(config: UserConfig):
            """Update user config."""
            # try to load from db
            _existing_config = self.user_config_db.read(config.user_id)
            if _existing_config is None:
                return Response(
                    f"User '{config.user_id}' does not exist.", 404
                )
            existing_config = UserConfig.from_json(_existing_config)

            # write given config hydrated with existing data (like password)
            self.user_config_db.write(
                config.user_id,
                UserConfig.from_json(
                    existing_config.with_secret.json | config.json
                ).with_secret.json,
            )
            return Response("OK", mimetype="text/plain", status=200)

    def create_user(
        self,
        user_id: Optional[str] = None,
        config: Optional[UserConfig] = None,
        password: Optional[str] = None,
    ) -> UserConfig:
        """
        Returns a `UserConfig` based on the given input. Requires either
        `user_id` or `config`. If `config` is given, changes are made in
        place. Sets the 'active'-flag: if `password` is given or
        `not AppConfig.REQUIRE_USER_ACTIVATION`, the user is set to
        active.
        """
        # check requirements
        if user_id is not None and config is not None:
            raise ValueError(
                "'ConfigurationView.create_user' accepts only 'user_id' OR "
                + "'config'."
            )
        if user_id is None and config is None:
            raise ValueError(
                "'ConfigurationView.create_user' requires either 'user_id' or "
                + "'config'."
            )

        # generate password if needed
        if password is None:
            _password = str(uuid4())
        else:
            _password = password

        # create config object if needed
        if config is None:
            config = UserConfig(user_id=user_id)

        # update config
        config.set_active(password is not None)
        config.set_password(self.password_hasher.hash(
            md5(_password.encode(encoding="utf-8")).hexdigest()
        ))
        if not config.with_secret.active:
            # TODO, send email..
            print(
                "Set initial password at: "
                + self.config.USER_ACTIVATION_URL_FMT.format(
                    password=_password
                )
            )

        return config

    def _post_user_config(self, bp: Blueprint):
        @bp.route(
            "/user/configure",
            methods=["POST"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.user_config_post_handler,
            json=flask_json,
        )
        def post_user_config(config: UserConfig):
            """Create new user config."""
            # check db for existing record
            if config.user_id in self.user_config_db.keys():
                return Response(
                    f"User '{config.user_id}' does already exist.",
                    mimetype="text/plain",
                    status=409,
                )

            # create user
            self.create_user(config=config)

            # write to db
            self.user_config_db.write(
                config.user_id,
                config.with_secret.json,
            )
            return Response("OK", mimetype="text/plain", status=200)

    def _delete_user_config(self, bp: Blueprint):
        @bp.route(
            "/user/configure",
            methods=["DELETE"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def delete_user_config(id_: str):
            """Delete user config by `id_`."""
            self.user_config_db.delete(id_)
            return Response(
                f"Deleted user '{id_}'.", mimetype="text/plain", status=200
            )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        self._get_job_config(bp)
        self._post_job_config(bp)
        self._list_job_configs(bp)
        self._delete_job_config(bp)
        self._list_users(bp)
        self._get_user_config(bp)
        self._put_user_config(bp)
        self._post_user_config(bp)
        self._delete_user_config(bp)
