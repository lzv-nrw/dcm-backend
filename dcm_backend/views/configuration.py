"""
Configuration View-class definition
"""

from flask import Blueprint, jsonify, Response
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common.db import KeyValueStoreAdapter
from dcm_common import services
from dcm_common.services.views.interface import View

from dcm_backend.config import AppConfig
from dcm_backend.models import JobConfig
from dcm_backend.components import Scheduler
from dcm_backend import handlers


class ConfigurationView(View):
    """
    View-class for managing job-configurations.

    Keyword arguments:
    config -- `AppConfig`-object
    config_db -- adapter for configuration-database
    scheduler -- `Scheduler`-object
    """

    NAME = "configuration"

    def __init__(
        self,
        config: AppConfig,
        config_db: KeyValueStoreAdapter,
        scheduler: Scheduler
    ) -> None:
        super().__init__(config)
        self.config_db = config_db
        self.scheduler = scheduler

    def _get_config(self, bp: Blueprint):
        @bp.route(
            "/configure", methods=["GET"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def get_config(id_: str):
            """Fetch config associated with given `id_`."""
            config = self.config_db.read(id_)
            if config:
                return jsonify(config), 200
            return Response(
                f"Unknown config '{id_}'.", mimetype="text/plain", status=404
            )

    def _post_config(self, bp: Blueprint):
        @bp.route(
            "/configure", methods=["POST"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process body
            handler=handlers.config_post_handler,
            json=flask_json,
        )
        def post_config(config: JobConfig):
            """Write config. Set id and name if missing."""
            if config.id_ is None:
                config.id_ = self.config_db.push({})
            if config.name is None:
                config.name = f"Unnamed Config {config.id_[0:8]}"
            self.config_db.write(config.id_, config.json)
            self.scheduler.schedule(config)
            return jsonify({"id": config.id_, "name": config.name}), 200

    def _list_configs(self, bp: Blueprint):
        @bp.route(
            "/configure", methods=["OPTIONS"], provide_automatic_options=False
        )
        def list_configs():
            """List ids of available configs."""
            return jsonify(self.config_db.keys()), 200

    def _delete_config(self, bp: Blueprint):
        @bp.route(
            "/configure", methods=["DELETE"], provide_automatic_options=False
        )
        @flask_handler(  # process query
            handler=handlers.get_config_id_handler(True),
            json=flask_args,
        )
        def delete_config(id_: str):
            """Delete config by `id_`."""
            self.config_db.delete(id_)
            self.scheduler.schedule(JobConfig({}, id_=id_))
            return Response(
                f"Deleted config '{id_}'.", mimetype="text/plain", status=200
            )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        self._get_config(bp)
        self._post_config(bp)
        self._list_configs(bp)
        self._delete_config(bp)
