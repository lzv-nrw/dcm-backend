"""Input handlers for the 'DCM Backend'-app."""

from data_plumber_http import (
    Object, Property, String, Url, Boolean, Integer, Array
)

from dcm_backend.models import (
    IngestConfig,
    RosettaBody,
    JobConfig,
    Schedule,
    Repeat,
    UserConfig,
    UserCredentials,
)


def get_ingest_handler(
    default_producer: str,
    default_material_flow: str
):
    """
    Returns parameterized handler

    Keyword arguments
    default_producer -- default value for the ID referencing a producer
                        in Rosetta, when value is not set in the request
    default_material_flow -- default value for he ID referencing a Material
                             Flow in Rosetta, when value is not set in the
                             request
    """
    return Object(
        properties={
            Property("ingest", required=True): Object(
                model=IngestConfig,
                properties={
                    Property(
                        "archive_identifier", required=True
                    ): String(
                        enum=["rosetta"]
                    ),
                    Property("rosetta"): Object(
                        model=RosettaBody,
                        properties={
                            Property(
                                "subdir",
                                required=True
                            ): String(),
                            Property(
                                "producer",
                                default=default_producer
                            ): String(),
                            Property(
                                "material_flow",
                                default=default_material_flow
                            ): String()
                        },
                        accept_only=["subdir", "producer", "material_flow"]
                    )
                },
                accept_only=[
                    "archive_identifier", "rosetta"
                ]
            ),
            Property("callbackUrl", name="callback_url"):
                Url(schemes=["http", "https"])
        },
        accept_only=["ingest", "callbackUrl"]
    ).assemble()


deposit_id_handler = Object(
    properties={
        Property("id", "id_", required=True): String(pattern=r".+")
    },
    accept_only=["id"]
).assemble()


def get_config_id_handler(
    required: bool = True, also_allow: list[str] = None
):
    """
    Returns parameterized handler

    Keyword arguments
    required -- whether field 'id' is required
                (default True)
    """
    return Object(
        properties={
            Property("id", "id_", required=required): String()
        },
        accept_only=["id"] + (also_allow or [])
    ).assemble()


ISODateTime = String(
    pattern=r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}"
)


job_config_post_handler = Object(
    model=lambda **kwargs: {"config": JobConfig(**kwargs)},
    properties={
        Property("id", "id_"): String(),
        Property("name"): String(),
        Property("last_modified"): ISODateTime,
        Property("job", required=True): Object(free_form=True),
        Property("schedule"): Object(
            model=Schedule,
            properties={
                Property("active", required=True): Boolean(),
                Property("start"): ISODateTime,
                Property("end"): ISODateTime,
                Property("repeat"): Object(
                    model=Repeat,
                    properties={
                        Property("unit", required=True): String(
                            enum=[
                                "second", "minute", "hour", "day",
                                "week", "monday", "tuesday",
                                "wednesday", "thursday", "friday",
                                "saturday", "sunday"
                            ]
                        ),
                        Property("interval", required=True): Integer(
                            min_value_inclusive=1,
                            max_value_inclusive=999999
                        )
                    },
                    accept_only=["unit", "interval"]
                ),
            },
            accept_only=["active", "start", "end", "repeat"]
        )
    },
    accept_only=["id", "name", "last_modified", "job", "schedule"]
).assemble()


_STATUS_FILTER_OPTIONS = "scheduled|queued|running|completed|aborted"


def _initialize_filter_list(status):
    if status is None:
        return _STATUS_FILTER_OPTIONS.split("|")
    if status == "":
        return []
    return status.split(",")


job_status_filter_handler = Object(
    model=lambda status=None: {"status": _initialize_filter_list(status)},
    properties={
        Property("status"): String(
            pattern=fr"(({_STATUS_FILTER_OPTIONS})*)(,({_STATUS_FILTER_OPTIONS})+)*"
        )
    },
    accept_only=["status", "id"]
).assemble()


user_config_post_handler = Object(
    model=lambda **kwargs: {"config": UserConfig(**kwargs)},
    properties={
        Property("userId", "user_id", required=True): String(),
        Property("externalId", "external_id"): String(),
        Property("firstname"): String(),
        Property("lastname"): String(),
        Property("email"): String(
            pattern=r"[a-zA-Z0-9+._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+"
        ),
        Property("roles"): Array(
            items=String()
        ),
    },
    accept_only=[
        "userId",
        "externalId",
        "firstname",
        "lastname",
        "email",
        "roles",
    ],
).assemble()


user_login_handler = Object(
    model=lambda **kwargs: {"credentials": UserCredentials(**kwargs)},
    properties={
        Property("userId", "user_id", required=True): String(),
        Property("password", required=True): String(),
    },
    accept_only=["userId", "password"],
).assemble()


user_change_password_handler = Object(
    model=lambda **kwargs: {
        "credentials": UserCredentials(kwargs["user_id"], kwargs["password"]),
        "new_password": kwargs["new_password"],
    },
    properties={
        Property("userId", "user_id", required=True): String(),
        Property("password", required=True): String(),
        Property("newPassword", "new_password", required=True): String(),
    },
    accept_only=["userId", "password", "newPassword"],
).assemble()
