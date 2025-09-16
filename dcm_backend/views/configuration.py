"""
Configuration View-class definition
"""

from typing import Optional
import sys
from hashlib import md5
from uuid import uuid4
from pathlib import Path

from flask import Blueprint, jsonify, Response
from argon2 import PasswordHasher
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common.db import SQLAdapter
from dcm_common import services
from dcm_common.services.views.interface import View

from dcm_backend.config import AppConfig
from dcm_backend.models import (
    JobConfig,
    GroupMembership,
    UserConfig,
    UserSecrets,
    UserConfigWithSecrets,
    WorkspaceConfig,
    TemplateConfig,
    HotfolderInfo,
    HotfolderDirectoryInfo,
)
from dcm_backend.components import Scheduler
from dcm_backend import handlers


class ConfigurationView(View):
    """
    View-class for managing configurations of
    * jobs
    * users
    * workspaces
    * templates

    Keyword arguments:
    config -- `AppConfig`-object
    db -- database adapter
    scheduler -- `Scheduler`-object
    password_hasher -- instance of argon2.PasswordHasher
    """

    NAME = "configuration"

    def __init__(
        self,
        config: AppConfig,
        db: SQLAdapter,
        scheduler: Scheduler,
        password_hasher: PasswordHasher
    ) -> None:
        super().__init__(config)
        self.db = db
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
            query = self.db.get_row("job_configs", id_).eval()

            if query is None:
                return Response(
                    f"Unknown job config '{id_}'.",
                    mimetype="text/plain",
                    status=404,
                )

            config = JobConfig.from_row(query)
            # fetch associated workspace
            config.workspace_id = self.db.get_row(
                "templates", config.template_id
            ).eval().get("workspace_id")
            # set scheduledExec; get first hit in list of planned executions
            config.scheduled_exec = next(
                iter(
                    sorted(
                        map(
                            lambda p: p.at,
                            self.scheduler.get_plans(config.id_),
                        )
                    )
                ),
                None,
            )
            # TODO: fetch associated jobs (reports)
            return jsonify(config.json), 200

    def _post_job_config(self, bp: Blueprint):

        @bp.route(
            "/job/configure", methods=["POST"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.get_job_config_handler(
                False, accept_creation_md=True
            ),
            json=flask_json,
        )
        def post_config(config: JobConfig):
            """Create new job config."""
            if config.id_ is not None:
                if (
                    self.db.get_row(
                        "job_configs", config.id_, cols=["id"]
                    ).eval()
                    is not None
                ):
                    return Response(
                        f"Job '{config.id_}' does already exist.",
                        mimetype="text/plain",
                        status=409,
                    )

            # write given config
            config.id_ = self.db.insert("job_configs", config.row).eval()
            if config.status == "ok":
                self.scheduler.schedule(config)
            return jsonify({"id": config.id_}), 200

    def _put_job_config(self, bp: Blueprint):

        @bp.route(
            "/job/configure",
            methods=["PUT"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.get_job_config_handler(
                True, accept_modification_md=True
            ),
            json=flask_json,
        )
        def put_job_config(config: JobConfig):
            """Update job config."""
            # try to load from db
            query = self.db.get_row(
                "job_configs", config.id_, cols=["id", "latest_exec"]
            ).eval()
            if query is None:
                return Response(
                    f"Job configuration '{config.id_}' does not exist.",
                    mimetype="text/plain",
                    status=404,
                )

            # write given config
            self.db.update("job_configs", config.row | query).eval()
            # cancel any scheduled plans for this job config
            self.scheduler.clear_jobs(config.id_)
            # re-schedule if not draft
            if config.status == "ok":
                # TODO: pass previous execution
                self.scheduler.schedule(config)
            return Response("OK", mimetype="text/plain", status=200)

    def _list_job_configs(self, bp: Blueprint):
        @bp.route(
            "/job/configure",
            methods=["OPTIONS"],
            provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.list_job_configs_handler,
            json=flask_args,
        )
        def list_configs(template_id: Optional[str] = None):
            """List ids of available job configs."""
            if template_id is None:
                job_configs = self.db.get_column("job_configs", "id").eval()
            else:
                job_configs = []
                for config in self.db.get_rows(
                    "job_configs", template_id, "template_id", ["id"]
                ).eval(
                    f"fetching job configurations with template '{template_id}'"
                ):
                    if config.get("id") is not None:
                        job_configs.append(config["id"])
            return (
                jsonify(job_configs),
                200,
            )

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
            self.db.delete("job_configs", id_).eval()
            self.scheduler.clear_jobs(id_)
            return Response(
                f"Deleted config '{id_}'.", mimetype="text/plain", status=200
            )

    def _list_users(self, bp: Blueprint):
        @bp.route(
            "/user/configure",
            methods=["OPTIONS"],
            provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.list_users_handler,
            json=flask_args,
        )
        def list_users(groups: Optional[str] = None):
            """List ids of existing users."""

            if groups is None:
                users = self.db.get_column("user_configs", "id").eval()
            else:
                users = []
                # collect group by group
                for group in groups.split(","):
                    # process user by user
                    for user in self.db.get_rows(
                        "user_groups",
                        group,
                        "group_id",
                        ["user_id"],
                    ).eval(f"fetching users associated with group '{group}'"):
                        if (
                            user.get("user_id")
                            and user.get("user_id") not in users
                        ):
                            users.append(user.get("user_id"))
            return jsonify(users), 200

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
            query = self.db.get_row("user_configs", id_).eval()

            if query is None:
                return Response(
                    f"Unknown user '{id_}'.",
                    mimetype="text/plain",
                    status=404,
                )

            config = UserConfig.from_row(query)
            config.groups.extend(
                map(
                    lambda x: GroupMembership(
                        x["group_id"], x["workspace_id"]
                    ),
                    self.db.get_rows(
                        "user_groups",
                        id_,
                        "user_id",
                        ["group_id", "workspace_id"],
                    ).eval(),
                )
            )
            return jsonify(config.json), 200

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
            handler=handlers.get_user_config_handler(
                True, accept_modification_md=True
            ),
            json=flask_json,
        )
        def put_user_config(config: UserConfig):
            """Update user config."""
            # try to load from db
            query = self.db.get_row(
                "user_configs", config.id_, cols=["id", "username"]
            ).eval()
            if query is None:
                return Response(
                    f"User '{config.id_}' does not exist.",
                    mimetype="text/plain",
                    status=404,
                )
            if query["username"] != config.username:
                print(
                    f"Changed username for '{config.id_}' from "
                    + f"'{query['username']}' to '{config.username}'.",
                    file=sys.stderr,
                )

            # validate workspaces for group memberships
            existing_workspaces = self.db.get_column("workspaces", "id").eval()
            for group in config.groups:
                if group.workspace is not None and (
                    group.workspace not in existing_workspaces
                ):
                    return Response(
                        (
                            f"Cannot modify user '{config.id_}'. "
                            + f"Workspace '{group.workspace}' does not exist."
                        ),
                        mimetype="text/plain",
                        status=400,
                    )

            # write given config
            with self.db.new_transaction() as t:
                t.add_update("user_configs", config.row)
                # replace user groups
                t.add_delete("user_groups", config.id_, "user_id")
                for group in config.groups:
                    t.add_insert(
                        "user_groups",
                        {
                            "id": str(uuid4()),
                            "group_id": group.id_,
                            "user_id": config.id_,
                            "workspace_id": group.workspace,
                        },
                    )
            t.result.eval("updating user")

            # remove secrets of deleted users (groups are automatically
            # removed by the corresponding transaction & the handler not
            # allowing to specify groups for deleted user)
            if config.status == "deleted":
                print(
                    f"Deleted secret for user '{config.username}' "
                    + f"({config.id_}).",
                    file=sys.stderr,
                )
                self.db.delete("user_secrets", config.id_, "user_id").eval(
                    f"deleting secret for user '{config.username}'"
                )

            return Response("OK", mimetype="text/plain", status=200)

    def create_user(
        self,
        username: Optional[str] = None,
        config: Optional[UserConfig] = None,
        password: Optional[str] = None,
    ) -> UserConfigWithSecrets:
        """
        Returns a `UserConfigWithSecrets` based on the given input.
        Requires either `username` or `config`. If `config` is given,
        changes are made in place. Sets `status` to 'ok' if `password`
        is given or `not AppConfig.REQUIRE_USER_ACTIVATION`.
        """
        # check requirements
        if username is not None and config is not None:
            raise ValueError(
                "'ConfigurationView.create_user' accepts only 'username' OR "
                + "'config'."
            )
        if username is None and config is None:
            raise ValueError(
                "'ConfigurationView.create_user' requires either 'username' or"
                + " 'config'."
            )

        user = UserConfigWithSecrets(
            UserConfig(username=username) if config is None else config,
            # generate password if needed
            (
                UserSecrets(user_id=config.id_, password_raw=str(uuid4()))
                if password is None
                else UserSecrets(user_id=config.id_, password_raw=password)
            ),
        )

        # update config
        if password is not None or not self.config.REQUIRE_USER_ACTIVATION:
            user.config.status = "ok"
        else:
            user.config.status = "inactive"

        user.secrets.password = self.password_hasher.hash(
            md5(user.secrets.password_raw.encode(encoding="utf-8")).hexdigest()
        )

        return user

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
            handler=handlers.get_user_config_handler(
                False, accept_creation_md=True
            ),
            json=flask_json,
        )
        def post_user_config(config: UserConfig):
            """Create new user config."""
            # check for conflicts
            if (  # id
                config.id_ is not None
                and self.db.get_row(
                    "user_configs", config.id_, cols=["id"]
                ).eval()
                is not None
            ) or len(  # username
                self.db.get_rows(
                    "user_configs", config.username, "username", cols=["id"]
                ).eval()
            ) != 0:
                return Response(
                    f"User '{config.id_}' does already exist.",
                    mimetype="text/plain",
                    status=409,
                )

            # validate workspaces for group memberships
            existing_workspaces = self.db.get_column("workspaces", "id").eval()
            for group in config.groups:
                if group.workspace is not None and (
                    group.workspace not in existing_workspaces
                ):
                    return Response(
                        (
                            "Cannot add user with username "
                            + f"'{config.username}'. "
                            + f"Workspace '{group.workspace}' does not exist."
                        ),
                        mimetype="text/plain",
                        status=400,
                    )

            # create user
            user = self.create_user(config=config)

            # insert in database
            with self.db.new_transaction() as t:
                user.config.id_ = str(uuid4())
                user.secrets.id_ = str(uuid4())
                t.add_insert("user_configs", user.config.row)
                t.add_insert(
                    "user_secrets",
                    user.secrets.row | {"user_id": user.config.id_},
                )
                # create user groups
                for group in user.config.groups:
                    t.add_insert(
                        "user_groups",
                        {
                            "id": str(uuid4()),
                            "group_id": group.id_,
                            "user_id": user.config.id_,
                            "workspace_id": group.workspace,
                        },
                    )
            t.result.eval("creating user")

            return (
                jsonify(
                    {
                        "id": user.config.id_,
                        "secret": user.secrets.password_raw,
                        "requiresActivation": self.config.REQUIRE_USER_ACTIVATION,
                    }
                ),
                200,
            )

    def _delete_user_secrets(self, bp: Blueprint):

        @bp.route(
            "/user/configure/secrets",
            methods=["DELETE"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=services.no_args_handler,
            json=flask_json,
        )
        def delete_user_secrets(id_: str):
            """Reset user secrets/activation."""
            # try to load from db
            config_query = self.db.get_row(
                "user_configs", id_, cols=["id", "username", "status"]
            ).eval()
            if config_query is None:
                return Response(
                    f"User '{id_}' does not exist.",
                    mimetype="text/plain",
                    status=404,
                )
            secret_query = self.db.get_rows(
                "user_secrets", id_, "user_id", cols=["id"]
            ).eval()
            if secret_query is None or len(secret_query) != 1:
                return Response(
                    f"User '{id_}' is in a broken state (missing secrets).",
                    mimetype="text/plain",
                    status=500,
                )

            # log changes
            print(
                f"Resetting user secrets for '{config_query['username']}'.",
                file=sys.stderr,
            )
            if (
                self.config.REQUIRE_USER_ACTIVATION
                and config_query["status"] != "inactive"
            ):
                print(
                    "Resetting activation status for "
                    + f"'{config_query['username']}'.",
                    file=sys.stderr,
                )

            # create new user secrets
            new_user = self.create_user(
                config=UserConfig.from_row(config_query)
            )

            # insert in database
            with self.db.new_transaction() as t:
                t.add_update(
                    "user_configs",
                    {"id": id_, "status": new_user.config.status},
                )
                t.add_update(
                    "user_secrets",
                    {"password": new_user.secrets.password} | secret_query[0],
                )
            t.result.eval("updating user and secrets")

            return (
                jsonify(
                    {
                        "secret": new_user.secrets.password_raw,
                        "requiresActivation": self.config.REQUIRE_USER_ACTIVATION,
                    }
                ),
                200,
            )

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
            self.db.delete("user_configs", id_).eval()
            return Response(
                f"Deleted user '{id_}'.",
                mimetype="text/plain",
                status=200
            )

    def _list_workspaces(self, bp: Blueprint):
        @bp.route(
            "/workspace/configure",
            methods=["OPTIONS"],
            provide_automatic_options=False
        )
        def list_workspaces():
            """List ids of existing workspaces."""
            return jsonify(self.db.get_column("workspaces", "id").eval()), 200

    def _get_workspace_config(self, bp: Blueprint):
        @bp.route(
            "/workspace/configure",
            methods=["GET"],
            provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def get_workspace_config(id_: str):
            """Fetch workspace config associated with given `id_`."""
            query = self.db.get_row("workspaces", id_).eval()

            if query is None:
                return Response(
                    f"Unknown workspace '{id_}'.",
                    mimetype="text/plain",
                    status=404,
                )

            config = WorkspaceConfig.from_row(query)
            # FIXME: these should be a single transaction
            config.users.extend(
                map(
                    lambda x: x["user_id"],
                    self.db.get_rows(
                        "user_groups",
                        id_,
                        "workspace_id",
                        ["user_id"],
                    ).eval(),
                )
            )
            config.templates.extend(
                map(
                    lambda x: x["id"],
                    self.db.get_rows(
                        "templates", id_, "workspace_id", ["id"]
                    ).eval(),
                )
            )
            return jsonify(config.json), 200

    def _put_workspace_config(self, bp: Blueprint):

        @bp.route(
            "/workspace/configure",
            methods=["PUT"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.get_workspace_config_handler(
                True, accept_modification_md=True
            ),
            json=flask_json,
        )
        def put_workspace_config(config: WorkspaceConfig):
            """Update workspace config."""
            # try to load from db
            query = self.db.get_row(
                "workspaces", config.id_, cols=["id"]
            ).eval()
            if query is None:
                return Response(
                    f"Workspace '{config.id_}' does not exist.",
                    mimetype="text/plain",
                    status=404,
                )

            # write given config
            self.db.update("workspaces", config.row).eval()
            return Response("OK", mimetype="text/plain", status=200)

    def _post_workspace_config(self, bp: Blueprint):

        @bp.route(
            "/workspace/configure",
            methods=["POST"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.get_workspace_config_handler(
                False, accept_creation_md=True
            ),
            json=flask_json,
        )
        def post_workspace_config(config: WorkspaceConfig):
            """Create new workspace config."""
            # check for conflicts
            if config.id_ is not None:
                if (
                    self.db.get_row(
                        "workspaces", config.id_, cols=["id"]
                    ).eval()
                    is not None
                ):
                    return Response(
                        f"Workspace '{config.id_}' does already exist.",
                        mimetype="text/plain",
                        status=409,
                    )
            return (
                jsonify(
                    {"id": self.db.insert("workspaces", config.row).eval()}
                ),
                200,
            )

    def _delete_workspace_config(self, bp: Blueprint):
        @bp.route(
            "/workspace/configure",
            methods=["DELETE"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def delete_workspace_config(id_: str):
            """Delete workspace config by `id_`."""
            self.db.delete("workspaces", id_).eval()
            return Response(
                f"Deleted workspace '{id_}'.",
                mimetype="text/plain",
                status=200
            )

    def _list_templates(self, bp: Blueprint):
        @bp.route(
            "/template/configure",
            methods=["OPTIONS"],
            provide_automatic_options=False
        )
        def list_templates():
            """List ids of existing templates."""
            return jsonify(self.db.get_column("templates", "id").eval()), 200

    def _get_template_config(self, bp: Blueprint):
        @bp.route(
            "/template/configure",
            methods=["GET"],
            provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def get_template_config(id_: str):
            """Fetch template config associated with given `id_`."""
            query = self.db.get_row("templates", id_).eval()

            if query is None:
                return Response(
                    f"Unknown template '{id_}'.",
                    mimetype="text/plain",
                    status=404,
                )

            config = TemplateConfig.from_row(query)
            # TODO: fetch associated job-configurations
            return jsonify(config.json), 200

    def _put_template_config(self, bp: Blueprint):

        @bp.route(
            "/template/configure",
            methods=["PUT"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.get_template_config_handler(
                True, accept_modification_md=True
            ),
            json=flask_json,
        )
        def put_template_config(config: TemplateConfig):
            """Update template config."""
            # try to load from db
            query = self.db.get_row(
                "templates", config.id_, cols=["id"]
            ).eval()
            if query is None:
                return Response(
                    f"Template '{config.id_}' does not exist.",
                    mimetype="text/plain",
                    status=404,
                )

            # write given config
            self.db.update("templates", config.row).eval()
            return Response("OK", mimetype="text/plain", status=200)

    def _post_template_config(self, bp: Blueprint):

        @bp.route(
            "/template/configure",
            methods=["POST"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.get_template_config_handler(
                False, accept_creation_md=True
            ),
            json=flask_json,
        )
        def post_template_config(config: TemplateConfig):
            """Create new template config."""
            # check for conflicts
            if config.id_ is not None:
                if (
                    self.db.get_row(
                        "templates", config.id_, cols=["id"]
                    ).eval()
                    is not None
                ):
                    return Response(
                        f"Template '{config.id_}' does already exist.",
                        mimetype="text/plain",
                        status=409,
                    )
            return (
                jsonify(
                    {"id": self.db.insert("templates", config.row).eval()}
                ),
                200,
            )

    def _delete_template_config(self, bp: Blueprint):
        @bp.route(
            "/template/configure",
            methods=["DELETE"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def delete_template_config(id_: str):
            """Delete template config by `id_`."""
            self.db.delete("templates", id_).eval()
            return Response(
                f"Deleted template '{id_}'.",
                mimetype="text/plain",
                status=200
            )

    def _hotfolder_endpoints(self, bp: Blueprint):
        @bp.route("/template/hotfolder", methods=["OPTIONS"])
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        def hotfolder_options():
            return (
                jsonify(
                    list(
                        map(lambda h: h.json, self.config.hotfolders.values())
                    )
                ),
                200,
            )

        @bp.route("/template/hotfolder/directory", methods=["OPTIONS"])
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def hotfolder_directory_options(id_: str):
            if id_ not in self.config.hotfolders:
                return Response(
                    f"Unknown hotfolder identifier '{id_}'.",
                    mimetype="text/plain",
                    status=404,
                )
            if not self.config.hotfolders[id_].mount.is_dir():
                return Response(
                    "Encountered a bad hotfolder configuration with id "
                    + f"'{id_}' (hotfolder is not mounted). Please contact "
                    + "the system administrator.",
                    mimetype="text/plain",
                    status=404,
                )

            # collect directories
            directories = {
                p.name: HotfolderDirectoryInfo(p.name, False)
                for p in filter(
                    Path.is_dir,
                    self.config.hotfolders[id_].mount.glob("*"),
                )
            }

            # identify directories that are already in use
            # * find templates that use this hotfolder
            #   (both row and json are intended to cover both status=ok
            #   and status=draft; the latter stores raw JSON)
            template_ids = self.db.custom_cmd(
                # pylint: disable=consider-using-f-string
                """SELECT id FROM templates
                WHERE type='hotfolder'
                AND (
                  additional_information={} OR additional_information={}
                )""".format(
                    self.db.decode(HotfolderInfo(id_).row, "jsonb"),
                    self.db.decode(HotfolderInfo(id_).json, "jsonb"),
                )
            ).eval()
            # * collect data-selections of job-configurations that use
            #   these templates
            job_configs = []
            for template_id in map(lambda x: x[0], template_ids):
                job_configs.extend(
                    self.db.get_rows(
                        "job_configs",
                        template_id,
                        "template_id",
                        ["id", "data_selection"],
                    ).eval()
                )
            # * update directories using the collected data
            for job_config in job_configs:
                name = (job_config.get("data_selection") or {}).get("path")
                if name is None or name not in directories:
                    continue
                directories[name].in_use = True
                directories[name].linked_job_configs.append(
                    job_config.get("id")
                )

            return (
                jsonify(list(map(lambda d: d.json, directories.values()))),
                200,
            )

        @bp.route(
            "/template/hotfolder/directory",
            methods=["POST"],
            provide_automatic_options=False,
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.template_hotfolder_new_directory_handler,
            json=flask_json,
        )
        def new_hotfolder_directory(id_: str, name: Path):
            if id_ not in self.config.hotfolders:
                return Response(
                    f"Unknown hotfolder identifier '{id_}'.",
                    mimetype="text/plain",
                    status=404,
                )
            if not self.config.hotfolders[id_].mount.is_dir():
                return Response(
                    "Encountered a bad hotfolder configuration with id "
                    + f"'{id_}' (hotfolder is not mounted). Please contact "
                    + "the system administrator.",
                    mimetype="text/plain",
                    status=404,
                )
            if name.is_absolute():
                return Response(
                    "Directory is required to be relative.",
                    mimetype="text/plain",
                    status=422,
                )
            if len(name.parents) == 0:
                return Response(
                    "Directory is empty.",
                    mimetype="text/plain",
                    status=422,
                )
            if len(name.parents) > 1:
                return Response(
                    "Invalid directory name: only a single-level name is"
                    + "allowed (no subdirectories).",
                    mimetype="text/plain",
                    status=422,
                )

            # handle bad characters
            bad_characters = [
                # null
                "\x00",
                # forbidden on Windows / posix separator
                "/",
                "\\",
                ":",
                "*",
                "?",
                '"',
                "<",
                ">",
                "|",
                # control / whitespace chars
                "\n",
                "\r",
                "\t",
                "\x0b",
                "\x0c",
                # other characters
                "#",
                "%",
                "&",
                "{",
                "}",
                "$",
                "!",
                "@",
                "+",
                "`",
                "~",
                ";",
                "=",
                ",",
            ]
            for c in bad_characters:
                if c in name.name:
                    return Response(
                        f"Bad character {repr(c)} in directory name.",
                        mimetype="text/plain",
                        status=422,
                    )

            target = self.config.hotfolders[id_].mount / name
            if target.exists():
                return Response(
                    "Target directory already exists.",
                    mimetype="text/plain",
                    status=409,
                )
            try:
                target.mkdir(parents=False)
            # pylint: disable=broad-exception-caught
            except Exception as exc_info:
                return Response(
                    f"An unknown error occurred: {exc_info}.",
                    mimetype="text/plain",
                    status=500,
                )

            return Response(
                "OK", mimetype="text/plain", status=200
            )

    # FIXME: DEPRECATED, remove on next major release
    def _hotfolder_sources(self, bp: Blueprint):
        @bp.route("/template/hotfolder-sources", methods=["GET"])
        def hotfolder_sources():
            return jsonify([]), 200

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        self._get_job_config(bp)
        self._post_job_config(bp)
        self._put_job_config(bp)
        self._list_job_configs(bp)
        self._delete_job_config(bp)
        self._list_users(bp)
        self._get_user_config(bp)
        self._put_user_config(bp)
        self._post_user_config(bp)
        self._delete_user_secrets(bp)
        self._delete_user_config(bp)
        self._list_workspaces(bp)
        self._get_workspace_config(bp)
        self._put_workspace_config(bp)
        self._post_workspace_config(bp)
        self._delete_workspace_config(bp)
        self._list_templates(bp)
        self._get_template_config(bp)
        self._put_template_config(bp)
        self._post_template_config(bp)
        self._delete_template_config(bp)
        self._hotfolder_endpoints(bp)
        # FIXME: DEPRECATED, remove on next major release
        self._hotfolder_sources(bp)
