"""
- DCM Backend -
This flask app implements the 'DCM Backend'-API (see
`openapi.yaml` in the sibling-package `dcm_backend_api`).
"""

from typing import Optional
from time import time, sleep

from flask import Flask
from argon2 import PasswordHasher
from dcm_common import LoggingContext as Context, Logger
from dcm_common.util import now
from dcm_common.db import KeyValueStoreAdapter, SQLAdapter
from dcm_common.orchestration import (
    ScalableOrchestrator,
    get_orchestration_controls,
)
from dcm_common.services import DefaultView, ReportView, APIResult
from dcm_common.services import extensions as common_extensions

from dcm_backend.config import AppConfig
from dcm_backend.components import Scheduler, JobProcessorAdapter
from dcm_backend.views import (
    IngestView,
    ConfigurationView,
    JobView,
    UserView,
    get_scheduling_controls,
)
from dcm_backend.models import Report, JobConfig, TriggerType
from dcm_backend import extensions


def app_factory(
    config: AppConfig,
    queue: Optional[KeyValueStoreAdapter] = None,
    registry: Optional[KeyValueStoreAdapter] = None,
    db: Optional[SQLAdapter] = None,
    as_process: bool = False,
    block: bool = False,
):
    """
    Returns a flask-app-object.

    config -- app config derived from `AppConfig`
    queue -- queue adapter override
             (default None; use `MemoryStore`)
    registry -- registry adapter override
                (default None; use `MemoryStore`)
    db -- database adapter
          (default None; uses `config.db`)
    as_process -- whether the app is intended to be run as process via
                  `app.run`; if `True`, startup tasks like starting
                  orchestration-daemon are prepended to `app.run`
                  instead of being run when this factory is executed
                  (default False)
    block -- whether to block execution until all extensions are ready
             (up to 10 seconds); only relevant if not `as_process`
             (default False)
    """

    app = Flask(__name__)
    app.config.from_object(config)

    # create Scheduler, Orchestrator, and View-classes
    adapter = JobProcessorAdapter(
        db=db or config.db,
        url=config.JOB_PROCESSOR_HOST,
        interval=config.JOB_PROCESSOR_POLL_INTERVAL,
        timeout=config.JOB_PROCESSOR_TIMEOUT,
    )

    def scheduled_job_factory(job_config: JobConfig):
        """Factory for scheduler callbacks."""
        def _():
            result = APIResult()
            token = adapter.submit(
                None,
                adapter.build_request_body(
                    job_config=job_config,
                    base_request_body={
                        "context": {
                            "jobConfigId": job_config.id_,
                            "datetimeTriggered": now().isoformat(),
                            "triggerType": (
                                (
                                    TriggerType.ONETIME
                                    if job_config.schedule.repeat is None
                                    else TriggerType.SCHEDULED
                                ).value
                            ),
                        }
                    },
                ),
                result,
            )
            if Context.ERROR in Logger.from_json(result.report.get("log", {})):
                raise RuntimeError(f"An error occurred: {result.report}.")
            (db or config.db).update(
                "job_configs",
                {"id": job_config.id_, "latest_exec": token.value},
            )
        return _

    scheduler = Scheduler(
        scheduled_job_factory,
        zoneinfo=config.SCHEDULING_TIMEZONE,
    )

    orchestrator = ScalableOrchestrator(
        queue=queue or config.queue,
        registry=registry or config.registry,
        nprocesses=config.ORCHESTRATION_PROCESSES,
    )
    password_hasher = PasswordHasher()
    view_ingest = IngestView(
        config=config,
        report_type=Report,
        orchestrator=orchestrator,
        context=IngestView.NAME,
    )
    configuration_view = ConfigurationView(
        config,
        db or config.db,
        scheduler,
        password_hasher,
    )
    job_view = JobView(
        config,
        db or config.db,
        scheduler,
        adapter,
    )
    user_view = UserView(
        config,
        db or config.db,
        password_hasher,
    )

    # register extensions
    if config.ALLOW_CORS:
        common_extensions.cors(app)
    orchestrator_loader = common_extensions.orchestration_loader(
        app, config, orchestrator, "Backend", as_process
    )
    scheduling_loader = extensions.scheduling_loader(scheduler)
    db_loader = common_extensions.db_loader(app, config, config.db, as_process)
    db_init_loader = extensions.db_init_loader(
        app,
        config,
        db or config.db,
        configuration_view.create_user,
        as_process,
        [
            common_extensions.ExtensionEventRequirement(
                db_loader.ready, "database connection made"
            )
        ],
    )
    scheduling_init_loader = extensions.scheduling_init_loader(
        app,
        config,
        scheduler,
        db or config.db,
        as_process,
        [
            common_extensions.ExtensionEventRequirement(
                scheduling_loader.ready, "scheduling initialized"
            ),
            common_extensions.ExtensionEventRequirement(
                db_init_loader.ready, "database initialized"
            ),
        ],
    )

    def ready():
        """Define condition for readiness."""
        return (
            (
                not config.ORCHESTRATION_AT_STARTUP
                or orchestrator_loader.ready.is_set()
            )
            and (
                not config.SCHEDULING_AT_STARTUP
                or (
                    scheduling_loader.ready.is_set()
                    and scheduling_init_loader.ready.is_set()
                )
            )
            and (db_loader.ready.is_set() and db_init_loader.ready.is_set())
        )

    # block until ready
    if block and not as_process:
        time0 = time()
        while not ready() and time() - time0 < 10:
            sleep(0.01)

    # register orchestrator-controls blueprint
    if getattr(config, "TESTING", False) or config.ORCHESTRATION_CONTROLS_API:
        app.register_blueprint(
            get_orchestration_controls(
                orchestrator,
                orchestrator_loader.data,
                orchestrator_settings={
                    "interval": config.ORCHESTRATION_ORCHESTRATOR_INTERVAL,
                },
                daemon_settings={
                    "interval": config.ORCHESTRATION_DAEMON_INTERVAL,
                },
            ),
            url_prefix="/",
        )
    # register scheduling-controls blueprint
    if getattr(config, "TESTING", False) or config.SCHEDULING_CONTROLS_API:
        app.register_blueprint(
            get_scheduling_controls(scheduler),
            url_prefix="/",
        )

    # register blueprints
    app.register_blueprint(
        DefaultView(config, ready=ready).get_blueprint(),
        url_prefix="/",
    )
    app.register_blueprint(view_ingest.get_blueprint(), url_prefix="/")
    app.register_blueprint(
        ReportView(config, orchestrator).get_blueprint(), url_prefix="/"
    )
    app.register_blueprint(configuration_view.get_blueprint(), url_prefix="/")
    app.register_blueprint(job_view.get_blueprint(), url_prefix="/")
    app.register_blueprint(user_view.get_blueprint(), url_prefix="/")

    return app
