"""Flask cleanup startup-extension."""

from typing import Optional, Iterable
import signal
from datetime import datetime, timedelta
from pathlib import Path
from shutil import rmtree

from dcm_common.daemon import CDaemon
from dcm_common.db import SQLAdapter
from dcm_common.services.extensions.common import (
    print_status,
    startup_flask_run,
    add_signal_handler,
    ExtensionLoaderResult,
    _ExtensionRequirement,
)
from dcm_common.util import list_directory_content

from dcm_backend.config import AppConfig


def run_cleanup(
    targets: list[Path],
    file_storage: Path,
    artifact_ttl: int,
    db: SQLAdapter,
    result: ExtensionLoaderResult,
    requirements=Iterable[_ExtensionRequirement],
):
    """Perform cleanup-actions"""
    # clean up expired artifacts
    if _ExtensionRequirement.check_requirements(
        requirements, "Missing cleanup-requirement '{}'."
    ):
        print_status("Running scheduled cleanup.")
        result.ready.set()
    else:
        print_status("Skipping scheduled cleanup (missing requirements).")
        result.ready.clear()
        return

    now = datetime.now().isoformat()
    for artifact_id, path in db.custom_cmd(
        f"SELECT id, path FROM artifacts WHERE datetime_expires < '{now}'",
        False,
    ).eval("getting expired artifacts"):
        print_status(f"Cleaning up expired artifact '{path}'.")
        artifact = file_storage / path
        try:
            if artifact.exists():
                if artifact.is_file():
                    artifact.unlink()
                else:
                    rmtree(artifact)
        except OSError as exc_info:
            print_status(
                f"Unable to clean up artifact '{path}' due to "
                + f"{type(exc_info).__name__}: {exc_info}"
            )
        finally:
            db.delete("artifacts", artifact_id).eval(
                "drop expired artifact-record"
            )

    # discover unknown artifacts
    recorded_artifacts = db.get_column("artifacts", "path").eval(
        "getting previously recorded artifacts"
    )
    current_artifacts = []
    # iterate all cleanup-targets
    for target in targets:
        current_artifacts.extend(
            map(
                lambda p: str(Path.relative_to(p, file_storage.resolve())),
                list_directory_content(
                    target, "*", lambda p: p.is_file() or p.is_dir()
                ),
            )
        )

    # write new records to database
    expiration = (datetime.now() + timedelta(seconds=artifact_ttl)).isoformat()
    for artifact in set(current_artifacts) - set(recorded_artifacts):
        print_status(f"Artifact '{artifact}' expires at {expiration}.")
        db.insert(
            "artifacts",
            {
                "path": artifact,
                "datetime_expires": expiration,
            },
        )

    print_status("Scheduled cleanup completed.")


def cleanup_loader(
    app,
    config: AppConfig,
    db: SQLAdapter,
    as_process,
    requirements: Optional[Iterable[_ExtensionRequirement]] = None,
) -> ExtensionLoaderResult:
    """
    Register the `cleanup` extension.

    If `as_process`, the daemon-startup call is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    daemon is executed directly, i.e., in the same process from which
    this process has been called.
    """
    result = ExtensionLoaderResult()
    daemon = CDaemon(
        target=run_cleanup,
        kwargs={
            "targets": config.cleanup_targets,
            "file_storage": config.FS_MOUNT_POINT,
            "artifact_ttl": config.CLEANUP_ARTIFACT_TTL,
            "db": db,
            "result": result,
            "requirements": requirements or [],
        },
    )
    result.data = daemon
    if not config.CLEANUP_DISABLED:
        if as_process:
            # app in separate process via app.run
            startup_flask_run(
                app,
                (
                    lambda: [
                        print_status("Created cleanup-schedule."),
                        daemon.run(config.CLEANUP_INTERVAL),
                    ],
                ),
            )
        else:
            # app native execution
            daemon.run(config.CLEANUP_INTERVAL)
            print_status("Created cleanup-schedule.")

    # perform clean shutdown on exit
    def _exit(
        *args,
        **kwargs,
    ):
        """Stop daemon and orchestrator."""
        if daemon.active:
            # needs to block here to prevent immediate restart
            daemon.stop(block=True)
        print_status("Unscheduled cleanup (received an abort from parent).")
        result.ready.clear()

    result.stop = _exit

    add_signal_handler(signal.SIGINT, _exit)
    add_signal_handler(signal.SIGTERM, _exit)

    return result
