"""Input handlers for the 'DCM Backend'-app."""

from typing import Any
from pathlib import Path

from data_plumber import Pipeline
from data_plumber_http import (
    Object,
    Property,
    String,
    Url,
    Boolean,
    Integer,
    Array,
    FileSystemObject,
)
from data_plumber_http.settings import Responses
from dcm_common.services import UUID, TargetPath

from dcm_backend.models import (
    IngestConfig,
    BundleConfig,
    BundleTarget,
    RosettaTarget,
    JobConfig,
    UserConfig,
    UserCredentials,
    WorkspaceConfig,
    TemplateConfig,
)


class ConditionalPipeline:
    """
    Wrapper for conditional Object-handlers.

    On execution, calls either `on_ok` or `on_draft`, depending on
    `json.status`.
    """

    PREREQUISITES = Object(
        properties={
            Property("status", required=True): String(enum=["ok", "draft"])
        }
    ).assemble()

    def __init__(self, on_ok: Pipeline, on_draft: Pipeline):
        self._on_ok = on_ok
        self._on_draft = on_draft

    def run(self, json) -> tuple[Any, str, int]:
        """Run corresponding pipeline"""
        prerequisites = self.PREREQUISITES.run(json=json)
        if prerequisites.last_status != Responses().GOOD.status:
            return prerequisites

        status = (json or {}).get("status")

        match status:
            case "ok":
                return self._on_ok.run(json=json or {})
            case "draft":
                return self._on_draft.run(json=json or {})

        raise ValueError(f"Uncaught unknown status '{status}'.")


post_ingest_rosetta_target_handler = Object(
    model=RosettaTarget,
    properties={
        Property("subdirectory", required=True): String(),
    },
    accept_only=["subdirectory"],
).assemble(".ingest.target")


post_ingest_handler = Object(
    properties={
        Property("ingest", required=True): Object(
            model=IngestConfig,
            properties={
                Property("archiveId", "archive_id", required=True): String(),
                Property("target", required=True): Object(
                    # proper validation of this object requires the
                    # contents of the archive configuration (archiveId)
                    # from the database; validation is therefore
                    # postponed until the job is executed
                    free_form=True
                ),
            },
            accept_only=["archiveId", "target"],
        ),
        Property("token"): UUID(),
        Property("callbackUrl", name="callback_url"): Url(
            schemes=["http", "https"]
        ),
    },
    accept_only=["ingest", "token", "callbackUrl"],
).assemble()


get_ingest_handler = Object(
    properties={
        Property("archiveId", "archive_id", required=True): String(
            pattern=r".+"
        ),
        Property("depositId", "deposit_id", required=True): String(
            pattern=r".+"
        ),
    },
    accept_only=["archiveId", "depositId"],
).assemble()


def get_post_artifact_handler(file_storage: Path):
    """Returns parameterized handler (based on file_storage)."""
    return Object(
        properties={
            Property("bundle", required=True): Object(
                model=BundleConfig,
                properties={
                    Property("targets", required=True): Array(
                        items=Object(
                            model=BundleTarget,
                            properties={
                                Property("path", required=True): TargetPath(
                                    _relative_to=file_storage,
                                    cwd=file_storage,
                                    exists=True,
                                ),
                                Property(
                                    "asPath", "as_path"
                                ): FileSystemObject(),
                            },
                            accept_only=["path", "asPath"],
                        )
                    ),
                },
                accept_only=["targets"],
            ),
            Property("token"): UUID(),
            Property("callbackUrl", name="callback_url"): Url(
                schemes=["http", "https"]
            ),
        },
        accept_only=["bundle", "token", "callbackUrl"],
    ).assemble()


def get_config_id_handler(required: bool = True, also_allow: list[str] = None):
    """
    Returns parameterized handler

    Keyword arguments
    required -- whether field 'id' is required
                (default True)
    """
    return Object(
        properties={
            Property("id", "id_", required=required): String(pattern=r".+")
        },
        accept_only=["id"] + (also_allow or []),
    ).assemble()


post_job_handler = Object(
    properties={
        Property("id", "id_", required=True): String(),
        Property("token"): UUID(),
        Property(
            "userTriggered",
            "user_triggered",
        ): String(),
    },
    accept_only=["id", "token", "userTriggered"],
).assemble()


ISODateTime = String(
    pattern=r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}"
)

ConfigurationMetadataCreated = {
    Property("userCreated"): String(),
    Property("datetimeCreated"): ISODateTime,
}

ConfigurationMetadataModified = {
    Property("userModified"): String(),
    Property("datetimeModified"): ISODateTime,
}


def get_job_config_handler(
    require_id: bool,
    accept_creation_md: bool = False,
    accept_modification_md: bool = False,
) -> ConditionalPipeline:
    """
    Returns a `JobConfig`-handler.

    Keyword arguments:
    require_id -- if `True`, `id_` is required
    accept_creation_md -- whether to accept creation-metadata
    accept_modification_md -- whether to accept modification-metadata
    """
    return ConditionalPipeline(
        on_ok=Object(
            model=lambda **kwargs: {"config": JobConfig.from_json(kwargs)},
            properties={
                Property("templateId", required=True): String(),
                Property("status", required=True): String(
                    enum=["draft", "ok"]
                ),
                Property("id", required=require_id): String(),
                Property("name", required=True): String(),
                Property("description"): String(),
                Property("contactInfo"): String(),
                Property("dataSelection"): Object(
                    # plugin
                    properties={},
                    accept_only=[],
                )
                | Object(
                    # hotfolder
                    properties={Property("path"): String()},
                    accept_only=["path"],
                )
                | Object(
                    # oai
                    properties={
                        Property("identifiers"): Array(items=String()),
                        Property("sets"): Array(items=String()),
                        Property("from"): String(
                            pattern=r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
                        ),
                        Property("until"): String(
                            pattern=r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
                        ),
                    },
                    accept_only=["identifiers", "sets", "from", "until"],
                ),
                Property("dataProcessing"): Object(
                    properties={
                        Property("mapping"): Object(
                            properties={
                                Property("type", required=True): String(
                                    enum=["plugin", "xslt", "python"]
                                ),
                                Property("data", required=True): Object(
                                    properties={
                                        Property("contents"): String(),
                                        Property("name"): String(),
                                        Property("datetimeUploaded"): String(),
                                    },
                                    accept_only=[
                                        "contents",
                                        "name",
                                        "datetimeUploaded",
                                    ],
                                )
                                | Object(
                                    properties={
                                        Property("plugin"): String(),
                                        Property("args"): Object(
                                            free_form=True
                                        ),
                                    },
                                    accept_only=["plugin", "args"],
                                ),
                            },
                            accept_only=["type", "data"],
                        ),
                        Property("preparation"): Object(
                            properties={
                                Property("rightsOperations"): Array(
                                    items=Object(free_form=True)
                                ),
                                Property("sigPropOperations"): Array(
                                    items=Object(free_form=True)
                                ),
                                Property("preservationOperations"): Array(
                                    items=Object(free_form=True)
                                ),
                            },
                            accept_only=[
                                "rightsOperations",
                                "sigPropOperations",
                                "preservationOperations",
                            ],
                        ),
                    },
                    accept_only=["mapping", "preparation"],
                ),
                Property("schedule"): Object(
                    properties={
                        Property("active", required=True): Boolean(),
                        Property("start"): ISODateTime,
                        Property("end"): ISODateTime,
                        Property("repeat"): Object(
                            properties={
                                Property("unit", required=True): String(
                                    enum=["day", "week", "month"]
                                ),
                                Property("interval", required=True): Integer(
                                    min_value_inclusive=1,
                                    max_value_inclusive=999999,
                                ),
                            },
                            accept_only=["unit", "interval"],
                        ),
                    },
                    accept_only=["active", "start", "end", "repeat"],
                ),
            }
            | ConfigurationMetadataCreated
            | ConfigurationMetadataModified,
            accept_only=[
                "templateId",
                "status",
                "id",
                "name",
                "description",
                "contactInfo",
                "dataSelection",
                "dataProcessing",
                "schedule",
            ]
            + (
                ["userCreated", "datetimeCreated"]
                if accept_creation_md
                else []
            )
            + (
                ["userModified", "datetimeModified"]
                if accept_modification_md
                else []
            ),
        ).assemble(),
        on_draft=Object(
            model=lambda **kwargs: {"config": JobConfig.from_json(kwargs)},
            properties={
                Property("templateId", required=True): String(),
                Property("status", required=True): String(
                    enum=["draft", "ok"]
                ),
                Property("id", required=require_id): String(),
                Property("name"): String(),
                Property("description"): String(),
                Property("contactInfo"): String(),
                Property("dataSelection"): Object(free_form=True),
                Property("dataProcessing"): Object(free_form=True),
                Property("schedule"): Object(free_form=True),
            }
            | ConfigurationMetadataCreated
            | ConfigurationMetadataModified,
            accept_only=[
                "templateId",
                "status",
                "id",
                "name",
                "description",
                "contactInfo",
                "dataSelection",
                "dataProcessing",
                "schedule",
            ]
            + (
                ["userCreated", "datetimeCreated"]
                if accept_creation_md
                else []
            )
            + (
                ["userModified", "datetimeModified"]
                if accept_modification_md
                else []
            ),
        ).assemble(),
    )


get_job_handler = Object(
    properties={
        Property("token", required=True): String(),
        # to simplify implementation, accept anything and check during
        # running job
        Property("keys"): String(pattern=r"^[a-zA-Z]+(,[a-zA-Z]+)*$"),
    },
    accept_only=["token", "keys"],
).assemble()


list_users_handler = Object(
    properties={
        # to simplify implementation, accept anything and check during
        # running job
        Property("group", "groups"): String(pattern=r"^[^,\s]+(,[^,\s]+)*$"),
    },
    accept_only=["group"],
).assemble()


list_job_configs_handler = Object(
    properties={
        Property("templateId", "template_id"): String(),
    },
    accept_only=["templateId"],
).assemble()


# the associated method in the JobView uses a custom command via the
# database adapter which needs to be done very cautiously
list_jobs_handler = Object(
    properties={
        Property("id", "id_"): String(),
        # to simplify implementation, accept anything and check during
        # running job
        Property("status"): String(pattern=r"^[a-z]+(,[a-z]+)*$"),
        Property("from", "from_"): String(
            # this pattern is required by the associated method to be safe
            # change with care!
            pattern=r"^[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}(:[0-9]{2}(:[0-9]{2}(\.[0-9]{6}([+-][0-9]{2}:[0-9]{2})?)?)?)?)?)?)?$"
        ),
        Property("to"): String(
            # this pattern is required by the associated method to be safe
            # change with care!
            pattern=r"^[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}(:[0-9]{2}(:[0-9]{2}(\.[0-9]{6}([+-][0-9]{2}:[0-9]{2})?)?)?)?)?)?)?$"
        ),
        Property("success"): String(enum=["true", "false"]),
    },
    accept_only=["id", "status", "from", "to", "success"],
).assemble()


get_ies_handler = Object(
    properties={
        Property("jobConfigId", "job_config_id", required=True): String(),
        Property("filterByStatus", "filter_by_status"): String(
            enum=[
                "complete",
                "inProcess",
                "validationError",
                "error",
                "ignored",
            ]
        ),
        Property("filterByText", "filter_by_text"): String(),
        Property("sort", default="datetimeChanged"): String(
            enum=[
                "datetimeChanged",
                "originSystemId",
                "externalId",
                "archiveIeId",
                "archiveSipId",
                "status",
            ]
        ),
        Property("range", "range_"): String(pattern=r"([0-9]+)\.\.([0-9]+)"),
        Property("count", default="false"): String(enum=["true", "false"]),
    },
    accept_only=[
        "jobConfigId",
        "filterByStatus",
        "filterByText",
        "sort",
        "range",
        "count",
    ],
).assemble()


post_ie_plan_handler = Object(
    properties={
        Property("id", "id_", required=True): String(),
        Property("clear"): Boolean(),
        Property("ignore"): Boolean(),
        Property("planAsBitstream", "plan_as_bitstream"): Boolean(),
        Property(
            "planToSkipObjectValidation", "plan_to_skip_object_validation"
        ): Boolean(),
    },
    accept_only=[
        "id",
        "clear",
        "ignore",
        "planAsBitstream",
        "planToSkipObjectValidation",
    ],
).assemble()


post_job_completion_handler = Object(
    properties={
        Property("jobConfigId", "job_config_id"): String(),
        Property("token", required=True): String(),
        Property("report", required=True): Object(free_form=True),
    },
    accept_only=["jobConfigId", "token", "report"],
).assemble()


def get_user_config_handler(
    require_id: bool,
    accept_creation_md: bool = False,
    accept_modification_md: bool = False,
) -> ConditionalPipeline:
    """
    Returns a `UserConfig`-handler.

    Keyword arguments:
    require_id -- if `True`, `id_` is required
    accept_creation_md -- whether to accept creation-metadata
    accept_modification_md -- whether to accept modification-metadata
    """

    class _ConditionalPipeline:
        PREREQUISITES = Object(
            properties={
                Property("status"): String(enum=["ok", "inactive", "deleted"])
            }
        ).assemble()

        def __init__(self, on_ok_or_inactive: Pipeline, on_deleted: Pipeline):
            self._on_ok_or_inactive = on_ok_or_inactive
            self._on_deleted = on_deleted

        def run(self, json) -> tuple[Any, str, int]:
            """Run corresponding pipeline"""
            prerequisites = self.PREREQUISITES.run(json=json)
            if prerequisites.last_status != Responses().GOOD.status:
                return prerequisites

            status = (json or {}).get("status", "ok")

            match status:
                case "ok" | "inactive":
                    return self._on_ok_or_inactive.run(json=json or {})
                case "deleted":
                    return self._on_deleted.run(json=json or {})

            raise ValueError(f"Uncaught unknown status '{status}'.")

    return _ConditionalPipeline(
        on_ok_or_inactive=Object(
            model=lambda **kwargs: {"config": UserConfig.from_json(kwargs)},
            properties={
                Property("id", required=require_id): String(),
                Property("status"): String(enum=["inactive", "ok"]),
                Property("username", required=True): String(),
                Property("externalId"): String(),
                Property("firstname"): String(),
                Property("lastname"): String(),
                Property("email", required=True): String(
                    pattern=r"[a-zA-Z0-9+._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+"
                ),
                Property("groups"): Array(
                    items=Object(
                        properties={
                            Property("id", required=True): String(),
                            Property("workspace"): String(),
                        },
                        accept_only=["id", "workspace"],
                    )
                ),
                Property("widgetConfig"): Object(free_form=True),
            }
            | ConfigurationMetadataCreated
            | ConfigurationMetadataModified
            | {},
            accept_only=[
                "id",
                "externalId",
                "username",
                "status",
                "firstname",
                "lastname",
                "email",
                "groups",
                "widgetConfig",
            ]
            + (
                ["userCreated", "datetimeCreated"]
                if accept_creation_md
                else []
            )
            + (
                ["userModified", "datetimeModified"]
                if accept_modification_md
                else []
            ),
        ).assemble(),
        on_deleted=Object(
            model=lambda **kwargs: {"config": UserConfig.from_json(kwargs)},
            properties={
                Property("id", required=require_id): String(),
                Property("status"): String(enum=["deleted"]),
                Property("username", required=True): String(),
                Property("firstname"): String(),
                Property("lastname"): String(),
            },
            accept_only=["id", "status", "username", "firstname", "lastname"],
        ).assemble(),
    )


user_login_handler = Object(
    model=lambda **kwargs: {"credentials": UserCredentials(**kwargs)},
    properties={
        Property("username", required=True): String(),
        Property("password", required=True): String(),
    },
    accept_only=["username", "password"],
).assemble()


user_change_password_handler = Object(
    model=lambda **kwargs: {
        "credentials": UserCredentials(kwargs["username"], kwargs["password"]),
        "new_password": kwargs["new_password"],
    },
    properties={
        Property("username", required=True): String(),
        Property("password", required=True): String(),
        Property("newPassword", "new_password", required=True): String(),
    },
    accept_only=["username", "password", "newPassword"],
).assemble()


def get_workspace_config_handler(
    require_id: bool,
    accept_creation_md: bool = False,
    accept_modification_md: bool = False,
) -> Pipeline:
    """
    Returns a `WorkspaceConfig`-handler.

    Keyword arguments:
    require_id -- if `True`, `id_` is required
    accept_creation_md -- whether to accept creation-metadata
    accept_modification_md -- whether to accept modification-metadata
    """
    return Object(
        model=lambda **kwargs: {"config": WorkspaceConfig.from_json(kwargs)},
        properties={
            Property("id", required=require_id): String(),
            Property("name", required=True): String(),
        }
        | ConfigurationMetadataCreated
        | ConfigurationMetadataModified,
        accept_only=["id", "name"]
        + (["userCreated", "datetimeCreated"] if accept_creation_md else [])
        + (
            ["userModified", "datetimeModified"]
            if accept_modification_md
            else []
        ),
    ).assemble()


def get_template_config_handler(
    require_id: bool,
    accept_creation_md: bool = False,
    accept_modification_md: bool = False,
) -> ConditionalPipeline:
    """
    Returns a `Template`-handler.

    Keyword arguments:
    require_id -- if `True`, `id_` is required
    accept_creation_md -- whether to accept creation-metadata
    accept_modification_md -- whether to accept modification-metadata
    """
    return ConditionalPipeline(
        on_ok=Object(
            model=lambda **kwargs: {
                "config": TemplateConfig.from_json(kwargs)
            },
            properties={
                Property("id", required=require_id): String(),
                Property("status", required=True): String(
                    enum=["draft", "ok"]
                ),
                Property("workspaceId"): String(),
                Property("name", required=True): String(),
                Property("description"): String(),
                Property("type", required=True): String(
                    enum=["plugin", "oai", "hotfolder"]
                ),
                Property("additionalInformation", required=True): Object(
                    properties={
                        Property("plugin", required=True): String(),
                        Property("args", required=True): Object(
                            free_form=True
                        ),
                    },
                    accept_only=["plugin", "args"],
                )
                | Object(
                    properties={
                        Property("sourceId", required=True): String(),
                    },
                    accept_only=["sourceId"],
                )
                | Object(
                    properties={
                        Property("url", required=True): Url(),
                        Property("metadataPrefix", required=True): String(),
                        Property(
                            "transferUrlFilters",
                            required=True,
                        ): Array(
                            items=Object(
                                properties={
                                    Property("regex", required=True): String(),
                                    Property("path"): String(),
                                },
                                accept_only=["regex", "path"],
                            )
                        ),
                    },
                    accept_only=[
                        "url",
                        "metadataPrefix",
                        "transferUrlFilters",
                    ],
                ),
                Property("targetArchive"): Object(
                    properties={Property("id", required=True): String()},
                    accept_only=["id"],
                ),
            }
            | ConfigurationMetadataCreated
            | ConfigurationMetadataModified,
            accept_only=[
                "id",
                "status",
                "workspaceId",
                "name",
                "description",
                "type",
                "additionalInformation",
                "targetArchive",
            ]
            + (
                ["userCreated", "datetimeCreated"]
                if accept_creation_md
                else []
            )
            + (
                ["userModified", "datetimeModified"]
                if accept_modification_md
                else []
            ),
        ).assemble(),
        on_draft=Object(
            model=lambda **kwargs: {
                "config": TemplateConfig.from_json(kwargs)
            },
            properties={
                Property("id", required=require_id): String(),
                Property("status", required=True): String(
                    enum=["draft", "ok"]
                ),
                Property("workspaceId"): String(),
                Property("name"): String(),
                Property("description"): String(),
                Property("type"): String(enum=["plugin", "oai", "hotfolder"]),
                Property("additionalInformation"): Object(free_form=True),
                Property("targetArchive"): Object(free_form=True),
            }
            | ConfigurationMetadataCreated
            | ConfigurationMetadataModified,
            accept_only=[
                "id",
                "status",
                "workspaceId",
                "name",
                "description",
                "type",
                "additionalInformation",
                "targetArchive",
            ]
            + (
                ["userCreated", "datetimeCreated"]
                if accept_creation_md
                else []
            )
            + (
                ["userModified", "datetimeModified"]
                if accept_modification_md
                else []
            ),
        ).assemble(),
    )


template_hotfolder_new_directory_handler = Object(
    properties={
        Property("id", "id_", required=True): String(),
        Property("name", required=True): FileSystemObject(),
    },
    accept_only=["id", "name"],
).assemble()
