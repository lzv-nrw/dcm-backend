"""
Artifact View-class definition
"""

from typing import Optional
from uuid import uuid4
from time import time
from pathlib import Path
import zipfile
import json
from dataclasses import dataclass

from flask import Blueprint, jsonify, Response, request, send_from_directory
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext as Context
from dcm_common.orchestra import JobConfig, JobContext, JobInfo
from dcm_common import services
from dcm_common.util import list_directory_content

from dcm_backend import handlers
from dcm_backend.models import BundleReport, BundleConfig, BundleInfo


@dataclass
class PlaceholderFile:
    """
    Placeholder file for files that are omitted while building archive.
    """

    reason: str
    original_name: str

    def __str__(self):
        return json.dumps(
            {
                "reason": self.reason,
                "originalName": self.original_name,
            },
            indent=2,
        )


class ArtifactView(services.OrchestratedView):
    """View-class for bundling artifacts."""

    NAME = "artifact"

    def register_job_types(self):
        self.config.worker_pool.register_job_type(
            self.NAME, self.bundle, BundleReport
        )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        @bp.route("/artifact", methods=["GET"])
        @flask_handler(  # unknown query
            handler=handlers.get_config_id_handler(True, ["downloadName"]),
            json=flask_args,
        )
        def download_bundle(id_: str):
            """
            Returns bundle/archive-file.
            """
            return send_from_directory(
                self.config.FS_MOUNT_POINT.resolve()
                / self.config.ARTIFACT_BUNDLE_DESTINATION,
                id_,
                as_attachment=True,
                download_name=request.args.get(
                    "downloadName", f"dcm-artifact-{int(time())}.zip"
                ),
            )

        @bp.route("/artifact", methods=["POST"])
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process bundle
            handler=handlers.get_post_artifact_handler(
                self.config.FS_MOUNT_POINT
            ),
            json=flask_json,
        )
        def bundle(
            bundle: BundleConfig,
            token: Optional[str] = None,
            callback_url: Optional[str] = None,
        ):
            """Submit artifacts for bundling."""
            try:
                token = self.config.controller.queue_push(
                    token or str(uuid4()),
                    JobInfo(
                        JobConfig(
                            self.NAME,
                            original_body=request.json,
                            request_body={
                                "bundle": bundle.json,
                                "callback_url": callback_url,
                            },
                        ),
                        report=BundleReport(
                            host=request.host_url, args=request.json
                        ),
                    ),
                )
            # pylint: disable=broad-exception-caught
            except Exception as exc_info:
                return Response(
                    f"Submission rejected: {exc_info}",
                    mimetype="text/plain",
                    status=500,
                )

            return jsonify(token.json), 201

        self._register_abort_job(bp, "/artifact")

    def validate_bundle_targets(
        self, context: JobContext, info: JobInfo, bundle_config: BundleConfig
    ) -> bool:
        """Performs validation and returns `True` if valid."""
        if len(bundle_config.targets) == 0:
            info.report.log.log(
                Context.ERROR,
                body="No target specified.",
            )
            context.push()
            return False

        for target in bundle_config.targets:
            if not any(
                src
                in (self.config.FS_MOUNT_POINT / target.path).resolve().parents
                for src in self.config.artifact_sources
            ):
                info.report.log.log(
                    Context.ERROR,
                    body=f"Target artifact '{target.path}' is not allowed.",
                )
                context.push()
                return False
        return True

    def _bundle(
        self,
        context: JobContext,
        info: JobInfo,
        bundle_config: BundleConfig,
        destination: Path,
    ) -> bool:
        """Run actual bundling. Returns `True` if successful."""
        # iterate files and add to output
        # abort if file becomes too large
        total_files = 0
        output_paths: list[Path] = []
        destination.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(
            destination,
            "w",
            (
                zipfile.ZIP_DEFLATED
                if self.config.ARTIFACT_COMPRESSION
                else zipfile.ZIP_STORED
            ),
        ) as a:
            for target in bundle_config.targets:
                info.report.progress.verbose = f"bundling '{target.path}'"
                context.push()
                if (self.config.FS_MOUNT_POINT / target.path).is_file():
                    files = [self.config.FS_MOUNT_POINT / target.path]
                else:
                    files = list_directory_content(
                        self.config.FS_MOUNT_POINT / target.path,
                        "**/*",
                        Path.is_file,
                    )
                # iterate files per target
                for f in files:
                    total_files += 1
                    # get default output-path (resolving to make sure both are
                    # absolute; otherwise relative_to might raise an error)
                    output_path = f.resolve().relative_to(
                        self.config.FS_MOUNT_POINT.resolve()
                    )
                    default_path = output_path
                    # replace root of output-path if request contains `as_path`
                    if target.as_path is not None:
                        output_path = target.as_path / output_path.relative_to(
                            target.path
                        )

                    # check for conflicts in output-path
                    if output_path in output_paths:
                        info.report.data.success = False
                        info.report.log.log(
                            Context.ERROR,
                            body=(
                                f"Path '{output_path}' already exists in "
                                + f"bundle (original path '{default_path}')."
                            ),
                        )
                        context.push()
                        destination.unlink()
                        return
                    output_paths.append(output_path)

                    # check size of individual file
                    if (
                        self.config.ARTIFACT_FILE_MAX_SIZE > 0
                        and f.stat().st_size
                        > self.config.ARTIFACT_FILE_MAX_SIZE
                    ):
                        omitted_file = PlaceholderFile(
                            "Omitted due to file-size constraint. File "
                            + "exceeds limit of "
                            + f"{self.config.ARTIFACT_FILE_MAX_SIZE} bytes.",
                            str(output_path),
                        )
                        info.report.log.log(
                            Context.WARNING,
                            body=(
                                f"File '{omitted_file.original_name}' exceeds "
                                + "limit for size of individual files of "
                                + f"{self.config.ARTIFACT_FILE_MAX_SIZE} bytes"
                                + " and a placeholder will be added instead."
                            ),
                        )
                        context.push()
                        a.writestr(
                            omitted_file.original_name + ".omitted.txt",
                            str(omitted_file),
                        )
                    else:
                        a.write(f, str(output_path))
                # check current size of bundle
                if (
                    self.config.ARTIFACT_BUNDLE_MAX_SIZE > 0
                    and destination.stat().st_size
                    > self.config.ARTIFACT_BUNDLE_MAX_SIZE
                ):
                    info.report.log.log(
                        Context.ERROR,
                        body=(
                            "Requested artifacts exceed the maximum allowed "
                            + "bundle size of "
                            + f"{self.config.ARTIFACT_BUNDLE_MAX_SIZE} bytes."
                        ),
                    )
                    info.report.log.log(Context.INFO, body="Bundling failed.")
                    context.push()
                    destination.unlink()
                    return False

        # success
        info.report.log.log(
            Context.INFO,
            body=(
                f"Successfully bundled a total of {total_files} file(s) from "
                + f"{len(bundle_config.targets)} target(s)."
            ),
        )
        context.push()
        return True

    def bundle(self, context: JobContext, info: JobInfo):
        """Job instructions for the 'POST-/artifact' endpoint."""
        bundle_config = BundleConfig.from_json(
            info.config.request_body["bundle"]
        )
        info.report.log.set_default_origin("Backend")

        # validate targets
        info.report.progress.verbose = "validating request"
        context.push()
        if not self.validate_bundle_targets(context, info, bundle_config):
            info.report.data.success = False
            info.report.progress.complete()
            context.push()
            self._run_callback(
                context, info, info.config.request_body.get("callback_url")
            )
            return

        # set output-destination
        destination: Path = (
            self.config.FS_MOUNT_POINT
            / self.config.ARTIFACT_BUNDLE_DESTINATION
            / str(uuid4())
        )

        # bundle
        if not self._bundle(context, info, bundle_config, destination):
            info.report.data.success = False
            info.report.progress.complete()
            context.push()
            self._run_callback(
                context, info, info.config.request_body.get("callback_url")
            )
            return

        # finalize job data
        info.report.data.success = True
        info.report.data.bundle = BundleInfo(
            destination.name, destination.stat().st_size / 1024 / 1024
        )
        info.report.log.log(
            Context.INFO, body="Bundling of artifacts completed."
        )
        info.report.progress.complete()
        context.push()
        self._run_callback(
            context, info, info.config.request_body.get("callback_url")
        )
