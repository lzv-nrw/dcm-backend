"""
- DCM Backend -
This flask app implements the 'DCM Backend'-API (see
`openapi.yaml` in the sibling-package `dcm_backend_api`).
"""

from typing import Optional
import threading

from flask import Flask
from dcm_common.db import KeyValueStoreAdapter
from dcm_common.orchestration import (
    ScalableOrchestrator, orchestrator_controls_bp
)
from dcm_common.services import DefaultView, ReportView, APIResult
from dcm_common.services import extensions as common_extensions

from dcm_backend.config import AppConfig
from dcm_backend.components import Scheduler, JobProcessorAdapter
from dcm_backend.views import (
    IngestView, ConfigurationView, JobView, get_scheduling_controls
)
from dcm_backend.models import Report, JobConfig
from dcm_backend import extensions


def app_factory(
    config: AppConfig,
    queue: Optional[KeyValueStoreAdapter] = None,
    registry: Optional[KeyValueStoreAdapter] = None,
    config_db: Optional[KeyValueStoreAdapter] = None,
    report_db: Optional[KeyValueStoreAdapter] = None,
    as_process: bool = False
):
    """
    Returns a flask-app-object.

    config -- app config derived from `AppConfig`
    queue -- queue adapter override
             (default None; use `MemoryStore`)
    registry -- registry adapter override
                (default None; use `MemoryStore`)
    config_db -- configuration-database adapter
                 (default None; use `MemoryStore`)
    report_db -- report-database adapter;
                  this adapter needs to be linked to the same resource
                  that the Job Processor uses for writing reports
                  (default None; use `MemoryStore`)
    as_process -- whether the app is intended to be run as process via
                  `app.run`; if `True`, startup tasks like starting
                  orchestration-daemon are prepended to `app.run`
                  instead of being run when this factory is executed
                  (default False)
    """

    app = Flask(__name__)
    app.config.from_object(config)

    # create Scheduler, Orchestrator, and View-classes
    adapter = JobProcessorAdapter(
        url=config.JOB_PROCESSOR_HOST,
        interval=config.JOB_PROCESSOR_POLL_INTERVAL,
        timeout=config.JOB_PROCESSOR_TIMEOUT
    )
    scheduler = Scheduler(
        job_cmd=lambda config: threading.Thread(
            target=adapter.run,
            args=(
                {"process": config.job, "id": config.id_}, None, APIResult()
            ),
            daemon=True
        ).start()
    )
    for config_id in (config_db or config.config_db).keys():
        scheduler.schedule(
            JobConfig.from_json(
                (config_db or config.config_db).read(config_id)
            )
        )
    orchestrator = ScalableOrchestrator(
        queue=queue or config.queue, registry=registry or config.registry
    )
    view_ingest = IngestView(
        config=config,
        report_type=Report,
        orchestrator=orchestrator,
        context=IngestView.NAME
    )
    view_configuration = ConfigurationView(
        config,
        config_db or config.config_db,
        scheduler
    )
    job_view = JobView(
        config,
        config_db or config.config_db,
        report_db or config.report_db,
        scheduler,
        adapter
    )

    # register extensions
    if config.ALLOW_CORS:
        common_extensions.cors(app)
    orchestratord = common_extensions.orchestration(
        app, config, orchestrator, "Transfer Module", as_process
    )
    scheduled = extensions.scheduling(
        app, config, scheduler, as_process
    )

    # register orchestrator-controls blueprint
    if getattr(config, "TESTING", False) or config.ORCHESTRATION_CONTROLS_API:
        app.register_blueprint(
            orchestrator_controls_bp(
                orchestrator, orchestratord,
                default_orchestrator_settings={
                    "interval": config.ORCHESTRATION_ORCHESTRATOR_INTERVAL,
                },
                default_daemon_settings={
                    "interval": config.ORCHESTRATION_DAEMON_INTERVAL,
                }
            ),
            url_prefix="/"
        )
    # register scheduling-controls blueprint
    if getattr(config, "TESTING", False) or config.SCHEDULING_CONTROLS_API:
        app.register_blueprint(
            get_scheduling_controls(
                scheduler, scheduled,
                default_scheduler_settings={
                    "interval": config.SCHEDULING_INTERVAL,
                },
                default_daemon_settings={"interval": 1}
            ),
            url_prefix="/"
        )

    # register blueprints
    app.register_blueprint(
        DefaultView(config, orchestrator).get_blueprint(),
        url_prefix="/"
    )
    app.register_blueprint(
        view_ingest.get_blueprint(),
        url_prefix="/"
    )
    app.register_blueprint(
        ReportView(config, orchestrator).get_blueprint(),
        url_prefix="/"
    )
    app.register_blueprint(
        view_configuration.get_blueprint(),
        url_prefix="/"
    )
    app.register_blueprint(
        job_view.get_blueprint(),
        url_prefix="/"
    )

    return app
