"""Database initialization-extension."""

from typing import Callable, Optional, Iterable
import os
from threading import Thread, Event
import signal
from importlib.metadata import version

from dcm_common.services.extensions.common import (
    print_status,
    startup_flask_run,
    add_signal_handler,
    ExtensionLoaderResult,
    _ExtensionRequirement,
)

from dcm_backend import util


def _db_init(config, db, user_create, abort, result, requirements):
    while not _ExtensionRequirement.check_requirements(
        requirements,
        "Initializing database delayed until '{}' is ready.",
    ):
        abort.wait(config.DB_INIT_STARTUP_INTERVAL)
        if abort.is_set():
            return

    # load schema if needed
    if config.DB_LOAD_SCHEMA:
        if (
            "deployment" not in db.get_table_names().eval("db initialization")
            or len(
                db.get_rows("deployment", True, "schema_loaded").eval(
                    "db initialization"
                )
            )
            == 0
        ):
            print_status(f"Loading SQL-schema file '{config.DB_SCHEMA}'.")
            db.read_file(config.DB_SCHEMA).eval("db initialization")
            db.insert("deployment", {"schema_loaded": True}).eval(
                "db initialization"
            )
            db.insert(
                "deployment", {"schema_version": version("dcm-database")}
            ).eval("db initialization")
        else:
            print_status("Skip loading SQL-schema (already initialized).")

    if config.DB_GENERATE_DEMO or config.DB_GENERATE_DEMO_USERS:
        if (
            "deployment" not in db.get_table_names().eval("db initialization")
            or len(
                db.get_rows("deployment", True, "demo_loaded").eval(
                    "db initialization"
                )
            )
            == 0
        ):
            if config.DB_DEMO_ADMIN_PW is not None:
                util.DemoData.admin_password = config.DB_DEMO_ADMIN_PW

            # setup demo-users
            util.create_demo_users(db, user_create)

            # setup demo-data
            if config.DB_GENERATE_DEMO:
                util.create_demo_workspaces(db)
                util.setup_demo_user_groups(db)
                util.create_demo_templates(db)
                util.create_demo_job_configs(db)
                util.create_demo_jobs(db)

            if not hasattr(config, "TESTING") or not config.TESTING:
                util.DemoData.print(True, config.DB_GENERATE_DEMO)
            db.insert("deployment", {"demo_loaded": True}).eval(
                "db initialization"
            )
        else:
            print_status("Skip loading demo (already initialized).")

    # check schema version in database against dcm-database
    def handler(msg):
        if config.DB_STRICT_SCHEMA_VERSION:
            print_status("ERROR: " + msg)
            os._exit(1)
        print_status("WARNING: " + msg)

    try:
        schema_version = next(
            (
                row
                for row in db.get_column("deployment", "schema_version").eval()
                if row is not None
            ),
            "unkown",
        )
    except ValueError as exc_info:
        handler(f"Unable to validate schema version ({exc_info}).")
    else:
        if schema_version != (package_version := version("dcm-database")):
            handler(
                "Database schema versions do not match (package: "
                + f"'{package_version}'; database: '{schema_version}')."
            )

    print_status("Database initialized.")

    result.ready.set()


def db_init_loader(
    app,
    config,
    db,
    user_create: Callable,
    as_process,
    requirements: Optional[Iterable[_ExtensionRequirement]] = None,
) -> ExtensionLoaderResult:
    """
    Register the database initialization extension.

    If `as_process`, the call to `init` is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    function is executed directly, i.e., in the same process from which
    this process has been called.
    """
    abort = Event()
    result = ExtensionLoaderResult()
    thread = Thread(
        target=_db_init,
        args=(config, db, user_create, abort, result, requirements or []),
    )
    result.data = thread
    if as_process:
        # app in separate process via app.run
        startup_flask_run(app, (thread.start,))
    else:
        # app native execution
        thread.start()

    # perform clean shutdown on exit
    def _exit():
        """Terminate connections."""
        abort.set()

    add_signal_handler(signal.SIGINT, _exit)
    add_signal_handler(signal.SIGTERM, _exit)

    return result
