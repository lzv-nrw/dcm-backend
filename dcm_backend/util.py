"""Utility definitions."""

from typing import Callable
import sys
from pathlib import Path
from json import loads, JSONDecodeError
from uuid import uuid3, UUID
import string
from random import choice
from datetime import timedelta

from dcm_common.util import now
from dcm_common.db import SQLAdapter

from dcm_backend.models import (
    UserConfig,
    GroupMembership,
    WorkspaceConfig,
    PluginInfo,
    TemplateConfig,
    JobConfig,
    TriggerType,
    Hotfolder,
)


def load_hotfolders_from_string(json: str) -> dict[str, Hotfolder]:
    """Loads hotfolders from the given JSON-string."""

    try:
        hotfolders_json = loads(json)
    except JSONDecodeError as exc_info:
        raise ValueError(
            f"Invalid hotfolder-configuration: {exc_info}."
        ) from exc_info

    if not isinstance(hotfolders_json, list):
        raise ValueError(
            "Invalid hotfolder-configuration: Expected list of hotfolders but "
            + f"got '{type(hotfolders_json).__name__}'."
        )

    hotfolders = {}
    for hotfolder in hotfolders_json:
        if not isinstance(hotfolder.get("id"), str):
            raise ValueError(
                f"Bad hotfolder id '{hotfolder.get('id')}' (bad type)."
            )
        if hotfolder["id"] in hotfolders:
            raise ValueError(
                f"Non-unique hotfolder id '{hotfolder['id']}'."
            )
        try:
            hotfolders[hotfolder["id"]] = Hotfolder.from_json(hotfolder)
        except (TypeError, ValueError) as exc_info:
            raise ValueError(
                f"Unable to deserialize hotfolder: {hotfolder}."
            ) from exc_info

    for hotfolder in hotfolders.values():
        if not hotfolder.mount.is_dir():
            print(
                "\033[1;33m"
                + f"WARNING: Mount point '{hotfolder.mount}' for hotfolder "
                + f"'{hotfolder.id_}' ({hotfolder.name}) is invalid."
                + "\033[0m",
                file=sys.stderr,
            )

    return hotfolders


def load_hotfolders_from_file(path: Path) -> dict[str, Hotfolder]:
    """Loads hotfolders from the given `path` (JSON-file)."""
    return load_hotfolders_from_string(path.read_text(encoding="utf-8"))


uuid_namespace = UUID("96ee5d00-d6fe-4993-9a2d-49670a65f2cf")
default_admin_password = "".join(
    [
        choice(string.ascii_letters + string.digits + """!"§$%&.,+-""")
        for _ in range(15)
    ]
)


class DemoData:
    """Generated demo-data uuids."""

    admin_password = default_admin_password
    user0 = str(uuid3(uuid_namespace, name="user0"))
    user1 = str(uuid3(uuid_namespace, name="user1"))
    user2 = str(uuid3(uuid_namespace, name="user2"))
    user3 = str(uuid3(uuid_namespace, name="user3"))
    workspace1 = str(uuid3(uuid_namespace, name="workspace1"))
    workspace2 = str(uuid3(uuid_namespace, name="workspace2"))
    template1 = str(uuid3(uuid_namespace, name="template1"))
    template2 = str(uuid3(uuid_namespace, name="template2"))
    template3 = str(uuid3(uuid_namespace, name="template3"))
    job_config1 = str(uuid3(uuid_namespace, name="job_config1"))
    token1 = str(uuid3(uuid_namespace, name="token1"))
    record1 = str(uuid3(uuid_namespace, name="record1"))

    @classmethod
    def print(cls, user: bool, other: bool):
        """Print relevant DemoData to stdout."""
        if not user and not other:
            return
        # define printed attributes
        user_attributes = [
            (
                "admin_password",
                (
                    "***"
                    if cls.admin_password != default_admin_password
                    else cls.admin_password
                ),
            ),
            ("user0", cls.user0),
            ("user1", cls.user1),
            ("user2", cls.user2),
            ("user3", cls.user3),
        ]
        other_attributes = [
            ("workspace1", cls.workspace1),
            ("workspace2", cls.workspace2),
            ("template1", cls.template1),
            ("template2", cls.template2),
            ("template3", cls.template3),
            ("job_config1", cls.job_config1),
            ("token1", cls.token1),
            ("record1", cls.record1),
        ]
        # calculate required space
        line_length = (
            max(
                *map(len, user_attributes[0] if user else [""]),
                *map(len, other_attributes[0] if other else [""]),
            )
            + 26
        )
        # output sections
        print("# " + "#" * line_length + " #")
        if user:
            print("# # UserDemoData" + " " * (line_length - 14) + " #")
            for x in user_attributes:
                y = f"{x[0]}: {x[1]}"
                print(
                    f"# {y}"
                    + " " * max(0, line_length - len(y))  # pad line length
                    + " #"
                )
        if other:
            print("# # OtherDemoData" + " " * (line_length - 15) + " #")
            for x in other_attributes:
                y = f"{x[0]}: {x[1]}"
                print(
                    f"# {y}"
                    + " " * max(0, line_length - len(y))  # pad line length
                    + " #"
                )
        print("# " + "#" * line_length + " #")


def create_demo_users(db: SQLAdapter, user_create: Callable):
    """
    Creates a set of demo-users.

    Keyword arguments:
    db -- database that should be written to
    user_create -- function that generates the credentials for a
                   user-configuration
    """
    for user in [
        user_create(
            config=UserConfig(
                id_=DemoData.user0,
                username="admin",
                firstname="Andi",
                lastname="Administrator",
                email="admin@lzv.nrw",
            ),
            password=DemoData.admin_password,
        ),
        user_create(
            config=UserConfig(
                id_=DemoData.user1,
                external_id="albert",
                username="einstein",
                firstname="Albert",
                lastname="Einstein",
                email="einstein@lzv.nrw",
                user_created=DemoData.user0,
                datetime_created=now().isoformat(),
            ),
            password="relativity",
        ),
        user_create(
            config=UserConfig(
                id_=DemoData.user2,
                external_id="maria",
                username="curie",
                firstname="Maria",
                lastname="Skłodowska-Curie",
                email="curie@lzv.nrw",
                user_created=DemoData.user0,
                datetime_created=now().isoformat(),
            ),
            password="radioactivity",
        ),
        user_create(
            config=UserConfig(
                id_=DemoData.user3,
                external_id="richard",
                username="feynman",
                firstname="Richard",
                lastname="Feynman",
                email="feynman@lzv.nrw",
                user_created=DemoData.user0,
                datetime_created=now().isoformat(),
            ),
            password="superfluidity",
        ),
    ]:
        user.secrets.user_id = db.insert(
            "user_configs", user.config.row
        ).eval()
        db.insert("user_secrets", user.secrets.row).eval()
        for group in user.config.groups:
            db.insert(
                "user_groups",
                {
                    "group_id": group.id_,
                    "user_id": user.config.id_,
                    "workspace_id": group.workspace,
                },
            ).eval()


def setup_demo_user_groups(db: SQLAdapter):
    """
    Sets up demo-user groups.

    Keyword arguments:
    db -- database that should be written to
    user_create -- function that generates the credentials for a
                   user-configuration
    """

    for user_id, groups in {
        DemoData.user0: [GroupMembership("admin")],
        DemoData.user1: [GroupMembership("curator", DemoData.workspace1)],
        DemoData.user2: [GroupMembership("curator", DemoData.workspace2)],
    }.items():
        for group in groups:
            db.insert(
                "user_groups",
                {
                    "group_id": group.id_,
                    "user_id": user_id,
                    "workspace_id": group.workspace,
                },
            ).eval()


def create_demo_workspaces(db: SQLAdapter):
    """
    Creates a set of demo-workspaces.

    Keyword arguments:
    db -- database that should be written to
    """
    for ws in [
        WorkspaceConfig(
            id_=DemoData.workspace1,
            name="Arbeitsbereich 1",
            user_created=DemoData.user0,
            datetime_created=now().isoformat(),
        ),
        WorkspaceConfig(
            id_=DemoData.workspace2,
            name="Arbeitsbereich 2",
            user_created=DemoData.user0,
            datetime_created=now().isoformat(),
        ),
    ]:
        db.insert("workspaces", ws.row).eval()


def create_demo_templates(db: SQLAdapter):
    """
    Creates a set of demo-job templates.

    Keyword arguments:
    db -- database that should be written to
    """
    for t in [
        TemplateConfig(
            id_=DemoData.template1,
            status="ok",
            workspace_id=DemoData.workspace1,
            name="Template 1",
            description=(
                "Dies ist ein Template für Jobs, die mit Testdaten arbeiten."
            ),
            type_="plugin",
            additional_information=PluginInfo(
                "demo", {"number": 2, "randomize": True}
            ),
            user_created=DemoData.user0,
            datetime_created=now().isoformat(),
        ),
        TemplateConfig(
            id_=DemoData.template2,
            status="draft",
            workspace_id=DemoData.workspace2,
            name="Template 2",
            description=(
                "Dies ist ein Template für Jobs, "
                + "die IPs aus einem Hotfolder importieren."
            ),
            type_="hotfolder",
            user_created=DemoData.user0,
            datetime_created=now().isoformat(),
        ),
        TemplateConfig(
            id_=DemoData.template3,
            status="draft",
            name="Template 3",
            description=(
                "Dies ist ein Template für Jobs, die OAI-PMH verwenden."
            ),
            type_="oai",
            user_created=DemoData.user0,
            datetime_created=now().isoformat(),
        ),
    ]:
        db.insert("templates", t.row).eval()


def create_demo_job_configs(db: SQLAdapter):
    """
    Creates a demo-job configuration.

    Keyword arguments:
    db -- database that should be written to
    """
    for job in [
        #  JobConfig based on a template associated with workspace1
        JobConfig.from_json(
            {
                "id": DemoData.job_config1,
                "status": "ok",
                "templateId": DemoData.template1,
                "latest_exec": DemoData.token1,
                "name": "Demo-Job 1",
                "description": "Demo-Plugin Import-Job",
                "contactInfo": "einstein@lzv.nrw",
                "dataProcessing": {
                    "mapping": {
                        "type": "plugin",
                        "data": {"plugin": "demo", "args": {}},
                    },
                },
                "schedule": {
                    "active": True,
                    "start": (now() + timedelta(days=1)).isoformat(),
                    "end": "2099-01-01T00:00:00+01:00",
                    "repeat": {"unit": "day", "interval": 1},
                },
                "userCreated": DemoData.user1,
                "datetimeCreated": now().isoformat(),
            }
        ),
    ]:
        db.insert("job_configs", job.row).eval()


def create_demo_jobs(db: SQLAdapter):
    """
    Creates a set of demo-job reports.

    Keyword arguments:
    db -- database (-table) that should be written to
    """
    report1 = {
        "args": {
            "id": "demo",
            "process": {
                "args": {
                    "build_ip": {
                        "build": {
                            "mappingPlugin": {"args": {}, "plugin": "demo"}
                        }
                    },
                    "import_ies": {
                        "import": {
                            "args": {
                                "bad_ies": False,
                                "number": 1,
                                "randomize": True,
                            },
                            "plugin": "demo",
                        }
                    },
                    "ingest": {"ingest": {"archiveId": "", "target": {}}},
                    "validation_payload": {
                        "validation": {
                            "plugins": {
                                "format": {
                                    "args": {},
                                    "plugin": "jhove-fido-mimetype-bagit",
                                },
                                "integrity": {
                                    "args": {},
                                    "plugin": "integrity-bagit",
                                },
                            }
                        }
                    },
                },
                "from": "import_ies",
                "to": "ingest",
            },
        },
        "children": {
            "09e48f2f-eef5-4193-87da-4d408f0abcba-0@build_ip": {
                "args": {
                    "build": {
                        "mappingPlugin": {"args": {}, "plugin": "demo"},
                        "target": {
                            "path": "ie/c21b24db-b7ed-48bb-82f5-06fb5fcbf4c2"
                        },
                    }
                },
                "data": {
                    "build_plugin": "bagit_bag_builder",
                    "details": {
                        "bagit-profile": {
                            "log": {
                                "INFO": [
                                    {
                                        "body": "Loading profile from 'https://lzv.nrw/bagit_profile.json'.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "BagIt-Validation-Plugin",
                                    },
                                    {
                                        "body": "'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d': Validating bag.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "BagIt-Validation-Plugin",
                                    },
                                    {
                                        "body": "'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d': Bag is valid.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "BagIt-Validation-Plugin",
                                    },
                                ],
                                "WARNING": [
                                    {
                                        "body": "Failed to retrieve the profile from the bag's bag-info from 'https://lzv.nrw/bagit_profile.json'. Using the default BagIt-profile from 'https://lzv.nrw/bagit_profile.json'.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "BagIt-Validation-Plugin",
                                    }
                                ],
                            },
                            "success": True,
                            "valid": True,
                        },
                        "build": {
                            "log": {
                                "INFO": [
                                    {
                                        "body": "Making bag from 'ie/c21b24db-b7ed-48bb-82f5-06fb5fcbf4c2'.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "BagIt Bag Builder",
                                    },
                                    {
                                        "body": "Successfully created bag.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "BagIt Bag Builder",
                                    },
                                ]
                            },
                            "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d",
                            "success": True,
                        },
                        "mapping": {
                            "log": {
                                "INFO": [
                                    {
                                        "body": "Loading XML-metadata from file at 'ie/c21b24db-b7ed-48bb-82f5-06fb5fcbf4c2/meta/source_metadata.xml'",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "Demo-Mapper-Plugin",
                                    }
                                ]
                            },
                            "metadata": {
                                "Bag-Software-Agent": "dcm-ip-builder v5.0.0",
                                "BagIt-Payload-Profile-Identifier": "https://lzv.nrw/payload_profile.json",
                                "BagIt-Profile-Identifier": "https://lzv.nrw/bagit_profile.json",
                                "Bagging-Date": "2025-03-17",
                                "DC-Creator": ["Thistlethwaite, Percival"],
                                "DC-Rights": [
                                    "CC BY-NC 4.0",
                                    "info:eu-repo/semantics/openAccess",
                                ],
                                "DC-Title": [
                                    "Unmasking the Fractals: Understanding the Enigmas of Law"
                                ],
                                "External-Identifier": "e827ab70-bb58-4da0-9faa-a0f5bc2d5795",
                                "Origin-System-Identifier": "test:oai_dc",
                                "Payload-Oxum": "63.1",
                                "Source-Organization": "https://d-nb.info/gnd/0",
                            },
                            "success": True,
                        },
                        "payload-structure": {
                            "log": {
                                "INFO": [
                                    {
                                        "body": "Loading profile from 'https://lzv.nrw/payload_profile.json'.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "Payload Structure Validation",
                                    },
                                    {
                                        "body": "'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d': Validating directory.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "Payload Structure Validation",
                                    },
                                    {
                                        "body": "'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d': Directory is valid.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "Payload Structure Validation",
                                    },
                                ],
                                "WARNING": [
                                    {
                                        "body": "Failed to retrieve the profile from the bag's bag-info from 'https://lzv.nrw/payload_profile.json': HTTP Error 404: Not Found Using the default BagIt-payload-profile from 'https://lzv.nrw/payload_profile.json'.",
                                        "datetime": "2025-03-17T10:04:46+00:00",
                                        "origin": "Payload Structure Validation",
                                    }
                                ],
                            },
                            "success": True,
                            "valid": True,
                        },
                    },
                    "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d",
                    "success": True,
                    "valid": True,
                },
                "host": "http://ip-builder/",
                "log": {
                    "EVENT": [
                        {
                            "body": "Produced at 2025-03-17T10:04:45.780962+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Consumed at 2025-03-17T10:04:46.640293+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Start executing Job.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Job terminated normally.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                    ],
                    "INFO": [
                        {
                            "body": "Building IP from IE 'ie/c21b24db-b7ed-48bb-82f5-06fb5fcbf4c2'.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Building IP at 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d'.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Loading XML-metadata from file at 'ie/c21b24db-b7ed-48bb-82f5-06fb5fcbf4c2/meta/source_metadata.xml'",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "Demo-Mapper-Plugin",
                        },
                        {
                            "body": "DC-Metadata detected, 'meta/dc.xml' written.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Successfully assembled IP at 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d'.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Validating IP 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d'.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Calling plugin 'BagIt-Validation-Plugin'",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Calling plugin 'Payload Structure Validation'",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Target is valid.",
                            "datetime": "2025-03-17T10:04:46+00:00",
                            "origin": "IP Builder",
                        },
                    ],
                },
                "progress": {
                    "numeric": 100,
                    "status": "completed",
                    "verbose": "shutting down after success",
                },
                "token": {
                    "expires": True,
                    "expires_at": "2025-03-17T11:04:45+00:00",
                    "value": "09e48f2f-eef5-4193-87da-4d408f0abcba",
                },
            },
            "16b4ecc0-3c06-4daf-976c-f47ebbdbabae-0@transfer": {
                "args": {
                    "transfer": {
                        "target": {
                            "path": "sip/747e89a3-288d-4adf-b17b-a3518b838b17"
                        }
                    }
                },
                "data": {"success": True},
                "host": "http://transfer-module/",
                "log": {
                    "EVENT": [
                        {
                            "body": "Produced at 2025-03-17T10:04:51.840536+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:52+00:00",
                            "origin": "Transfer Module",
                        },
                        {
                            "body": "Consumed at 2025-03-17T10:04:52.642814+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:52+00:00",
                            "origin": "Transfer Module",
                        },
                        {
                            "body": "Start executing Job.",
                            "datetime": "2025-03-17T10:04:52+00:00",
                            "origin": "Transfer Module",
                        },
                        {
                            "body": "Attempting transfer of SIP 'sip/747e89a3-288d-4adf-b17b-a3518b838b17'.",
                            "datetime": "2025-03-17T10:04:52+00:00",
                            "origin": "Transfer Module",
                        },
                        {
                            "body": "Starting transfer of 'sip/747e89a3-288d-4adf-b17b-a3518b838b17'.",
                            "datetime": "2025-03-17T10:04:52+00:00",
                            "origin": "Transfer Manager",
                        },
                        {
                            "body": "Transfer complete.",
                            "datetime": "2025-03-17T10:04:53+00:00",
                            "origin": "Transfer Manager",
                        },
                        {
                            "body": "Job terminated normally.",
                            "datetime": "2025-03-17T10:04:53+00:00",
                            "origin": "Transfer Module",
                        },
                    ],
                    "INFO": [
                        {
                            "body": "SIP transfer complete.",
                            "datetime": "2025-03-17T10:04:53+00:00",
                            "origin": "Transfer Module",
                        }
                    ],
                },
                "progress": {
                    "numeric": 100,
                    "status": "completed",
                    "verbose": "shutting down after success",
                },
                "token": {
                    "expires": True,
                    "expires_at": "2025-03-17T11:04:51+00:00",
                    "value": "16b4ecc0-3c06-4daf-976c-f47ebbdbabae",
                },
            },
            "58eca129-52f4-4592-abe6-df3af5547ff1-0@ingest": {
                "args": {
                    "ingest": {
                        "archiveId": "",
                        "target": {
                            "subdirectory": "747e89a3-288d-4adf-b17b-a3518b838b17"
                        },
                    }
                },
                "data": {
                    "deposit": {
                        "id": "1078",
                        "sip_reason": None,
                        "status": "INPROCESS",
                    },
                    "success": True,
                },
                "host": "http://backend/",
                "log": {
                    "EVENT": [
                        {
                            "body": "Produced at 2025-03-17T10:04:53.756166+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:54+00:00",
                            "origin": "Backend",
                        },
                        {
                            "body": "Consumed at 2025-03-17T10:04:54.650025+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:54+00:00",
                            "origin": "Backend",
                        },
                        {
                            "body": "Start executing Job.",
                            "datetime": "2025-03-17T10:04:54+00:00",
                            "origin": "Backend",
                        },
                        {
                            "body": "Attempting ingest of '747e89a3-288d-4adf-b17b-a3518b838b17' in archive system 'rosetta'.",
                            "datetime": "2025-03-17T10:04:54+00:00",
                            "origin": "Backend",
                        },
                        {
                            "body": "Attempting to retrieve actual ingest status of submitted deposit activity with id '1078'.",
                            "datetime": "2025-03-17T10:04:54+00:00",
                            "origin": "Backend",
                        },
                        {
                            "body": "Job terminated normally.",
                            "datetime": "2025-03-17T10:04:54+00:00",
                            "origin": "Backend",
                        },
                    ],
                    "INFO": [
                        {
                            "body": "Ingest triggered successfully.",
                            "datetime": "2025-03-17T10:04:54+00:00",
                            "origin": "Backend",
                        },
                        {
                            "body": "Actual ingest status retrieved successfully.",
                            "datetime": "2025-03-17T10:04:54+00:00",
                            "origin": "Backend",
                        },
                    ],
                },
                "progress": {
                    "numeric": 100,
                    "status": "completed",
                    "verbose": "shutting down after success",
                },
                "token": {
                    "expires": True,
                    "expires_at": "2025-03-17T11:04:53+00:00",
                    "value": "58eca129-52f4-4592-abe6-df3af5547ff1",
                },
            },
            "8864e196-4144-4f4b-8c92-9019971ddc1d-0@validation_payload": {
                "args": {
                    "validation": {
                        "plugins": {
                            "format": {
                                "args": {},
                                "plugin": "jhove-fido-mimetype-bagit",
                            },
                            "integrity": {
                                "args": {},
                                "plugin": "integrity-bagit",
                            },
                        },
                        "target": {
                            "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d"
                        },
                    }
                },
                "data": {
                    "details": {
                        "format": {
                            "records": {
                                "0": {
                                    "log": {
                                        "INFO": [
                                            {
                                                "body": "Calling JHOVE on file 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/data/preservation_master/payload.txt'.",
                                                "datetime": "2025-03-17T10:04:48+00:00",
                                                "origin": "JHOVE-Plugin",
                                            },
                                            {
                                                "body": "Identified file 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/data/preservation_master/payload.txt' as '['text/plain', 'None']'.",
                                                "datetime": "2025-03-17T10:04:48+00:00",
                                                "origin": "fido/MIME-Plugin",
                                            },
                                            {
                                                "body": "Well-Formed and valid (file 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/data/preservation_master/payload.txt', module 'ASCII-hul')",
                                                "datetime": "2025-03-17T10:04:49+00:00",
                                                "origin": "JHOVE-Plugin",
                                            },
                                        ]
                                    },
                                    "module": "ASCII-hul",
                                    "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/data/preservation_master/payload.txt",
                                    "raw": {
                                        "jhove": {
                                            "date": "2023-05-18",
                                            "executionTime": "2025-03-17T11:04:49+01:00",
                                            "name": "Jhove",
                                            "release": "1.28.0",
                                            "repInfo": [
                                                {
                                                    "format": "ASCII",
                                                    "lastModified": "2025-03-17T11:04:44+01:00",
                                                    "mimeType": "text/plain; charset=US-ASCII",
                                                    "reportingModule": {
                                                        "date": "2022-04-22",
                                                        "name": "ASCII-hul",
                                                        "release": "1.4.2",
                                                    },
                                                    "size": 63,
                                                    "status": "Well-Formed and valid",
                                                    "uri": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/data/preservation_master/payload.txt",
                                                }
                                            ],
                                        }
                                    },
                                    "success": True,
                                    "valid": True,
                                }
                            },
                            "success": True,
                            "valid": True,
                        },
                        "integrity": {
                            "records": {
                                "0": {
                                    "log": {
                                        "INFO": [
                                            {
                                                "body": "Checksum of file 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/data/preservation_master/payload.txt' is good.",
                                                "datetime": "2025-03-17T10:04:49+00:00",
                                                "origin": "Integrity-Plugin",
                                            }
                                        ]
                                    },
                                    "method": "sha512",
                                    "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/data/preservation_master/payload.txt",
                                    "success": True,
                                    "valid": True,
                                },
                                "1": {
                                    "log": {
                                        "INFO": [
                                            {
                                                "body": "Checksum of file 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/meta/source_metadata.xml' is good.",
                                                "datetime": "2025-03-17T10:04:49+00:00",
                                                "origin": "Integrity-Plugin",
                                            }
                                        ]
                                    },
                                    "method": "sha512",
                                    "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/meta/source_metadata.xml",
                                    "success": True,
                                    "valid": True,
                                },
                                "2": {
                                    "log": {
                                        "INFO": [
                                            {
                                                "body": "Checksum of file 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/manifest-sha512.txt' is good.",
                                                "datetime": "2025-03-17T10:04:49+00:00",
                                                "origin": "Integrity-Plugin",
                                            }
                                        ]
                                    },
                                    "method": "sha512",
                                    "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/manifest-sha512.txt",
                                    "success": True,
                                    "valid": True,
                                },
                                "3": {
                                    "log": {
                                        "INFO": [
                                            {
                                                "body": "Checksum of file 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/bag-info.txt' is good.",
                                                "datetime": "2025-03-17T10:04:49+00:00",
                                                "origin": "Integrity-Plugin",
                                            }
                                        ]
                                    },
                                    "method": "sha512",
                                    "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/bag-info.txt",
                                    "success": True,
                                    "valid": True,
                                },
                                "4": {
                                    "log": {
                                        "INFO": [
                                            {
                                                "body": "Checksum of file 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/bagit.txt' is good.",
                                                "datetime": "2025-03-17T10:04:49+00:00",
                                                "origin": "Integrity-Plugin",
                                            }
                                        ]
                                    },
                                    "method": "sha512",
                                    "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/bagit.txt",
                                    "success": True,
                                    "valid": True,
                                },
                                "5": {
                                    "log": {
                                        "INFO": [
                                            {
                                                "body": "Checksum of file 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/manifest-sha256.txt' is good.",
                                                "datetime": "2025-03-17T10:04:49+00:00",
                                                "origin": "Integrity-Plugin",
                                            }
                                        ]
                                    },
                                    "method": "sha512",
                                    "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d/manifest-sha256.txt",
                                    "success": True,
                                    "valid": True,
                                },
                            },
                            "success": True,
                            "valid": True,
                        },
                    },
                    "success": True,
                    "valid": True,
                },
                "host": "http://object-validator/",
                "log": {
                    "EVENT": [
                        {
                            "body": "Produced at 2025-03-17T10:04:47.807080+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "Object Validator",
                        },
                        {
                            "body": "Consumed at 2025-03-17T10:04:48.643263+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "Object Validator",
                        },
                        {
                            "body": "Start executing Job.",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "Object Validator",
                        },
                        {
                            "body": "Job terminated normally.",
                            "datetime": "2025-03-17T10:04:49+00:00",
                            "origin": "Object Validator",
                        },
                    ],
                    "INFO": [
                        {
                            "body": "Calling plugin 'JHOVE-Plugin'",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "Object Validator",
                        },
                        {
                            "body": "Calling plugin 'Integrity-Plugin'",
                            "datetime": "2025-03-17T10:04:49+00:00",
                            "origin": "Object Validator",
                        },
                        {
                            "body": "Target is valid.",
                            "datetime": "2025-03-17T10:04:49+00:00",
                            "origin": "Object Validator",
                        },
                    ],
                },
                "progress": {
                    "numeric": 100,
                    "status": "completed",
                    "verbose": "shutting down after success",
                },
                "token": {
                    "expires": True,
                    "expires_at": "2025-03-17T11:04:47+00:00",
                    "value": "8864e196-4144-4f4b-8c92-9019971ddc1d",
                },
            },
            "9e2c3ff7-58e2-4fe3-aaef-93941e8c7e2f-0@build_sip": {
                "args": {
                    "build": {
                        "target": {
                            "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d"
                        }
                    }
                },
                "data": {
                    "path": "sip/747e89a3-288d-4adf-b17b-a3518b838b17",
                    "success": True,
                },
                "host": "http://sip-builder/",
                "log": {
                    "EVENT": [
                        {
                            "body": "Produced at 2025-03-17T10:04:49.821016+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:50+00:00",
                            "origin": "SIP Builder",
                        },
                        {
                            "body": "Consumed at 2025-03-17T10:04:50.641972+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:50+00:00",
                            "origin": "SIP Builder",
                        },
                        {
                            "body": "Start executing Job.",
                            "datetime": "2025-03-17T10:04:50+00:00",
                            "origin": "SIP Builder",
                        },
                        {
                            "body": "Job terminated normally.",
                            "datetime": "2025-03-17T10:04:50+00:00",
                            "origin": "SIP Builder",
                        },
                    ],
                    "INFO": [
                        {
                            "body": "Validation of 'ie.xml' with schema 'Ex Libris, Rosetta METS v7.3' returns VALID.",
                            "datetime": "2025-03-17T10:04:50+00:00",
                            "origin": "XML Schema Validator",
                        },
                        {
                            "body": "Validation of 'dc.xml' with schema 'LZV.nrw, dc.xml schema v2.0.1' returns VALID.",
                            "datetime": "2025-03-17T10:04:50+00:00",
                            "origin": "XML Schema Validator",
                        },
                        {
                            "body": "Successfully assembled SIP at 'sip/747e89a3-288d-4adf-b17b-a3518b838b17'.",
                            "datetime": "2025-03-17T10:04:50+00:00",
                            "origin": "SIP Builder",
                        },
                    ],
                },
                "progress": {
                    "numeric": 100,
                    "status": "completed",
                    "verbose": "shutting down after success",
                },
                "token": {
                    "expires": True,
                    "expires_at": "2025-03-17T11:04:49+00:00",
                    "value": "9e2c3ff7-58e2-4fe3-aaef-93941e8c7e2f",
                },
            },
            "bac20bdf-9b36-4fc9-82df-db500d31197d-0@import_ies": {
                "args": {
                    "import": {
                        "args": {
                            "bad_ies": False,
                            "number": 1,
                            "randomize": True,
                        },
                        "plugin": "demo",
                    }
                },
                "data": {
                    "IEs": {
                        "test:oai_dc:e827ab70-bb58-4da0-9faa-a0f5bc2d5795": {
                            "IPIdentifier": None,
                            "fetchedPayload": True,
                            "path": "ie/c21b24db-b7ed-48bb-82f5-06fb5fcbf4c2",
                            "sourceIdentifier": "test:oai_dc:e827ab70-bb58-4da0-9faa-a0f5bc2d5795",
                        }
                    },
                    "success": True,
                },
                "host": "http://import-module/",
                "log": {
                    "EVENT": [
                        {
                            "body": "Produced at 2025-03-17T10:04:43.661929+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:44+00:00",
                            "origin": "Import Module",
                        },
                        {
                            "body": "Consumed at 2025-03-17T10:04:44.640264+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:44+00:00",
                            "origin": "Import Module",
                        },
                        {
                            "body": "Start executing Job.",
                            "datetime": "2025-03-17T10:04:44+00:00",
                            "origin": "Import Module",
                        },
                        {
                            "body": "Job terminated normally.",
                            "datetime": "2025-03-17T10:04:44+00:00",
                            "origin": "Import Module",
                        },
                    ],
                    "INFO": [
                        {
                            "body": "Starting to generate IEs.",
                            "datetime": "2025-03-17T10:04:44+00:00",
                            "origin": "Import Module",
                        },
                        {
                            "body": "Created IE in 'ie/c21b24db-b7ed-48bb-82f5-06fb5fcbf4c2'.",
                            "datetime": "2025-03-17T10:04:44+00:00",
                            "origin": "demo",
                        },
                        {
                            "body": "Collected 1 IE(s) with 0 error(s).",
                            "datetime": "2025-03-17T10:04:44+00:00",
                            "origin": "Import Module",
                        },
                        {
                            "body": "Skip building IPs (request does not contain build-information).",
                            "datetime": "2025-03-17T10:04:44+00:00",
                            "origin": "Import Module",
                        },
                    ],
                },
                "progress": {
                    "numeric": 100,
                    "status": "completed",
                    "verbose": "shutting down after success",
                },
                "token": {
                    "expires": True,
                    "expires_at": "2025-03-17T11:04:43+00:00",
                    "value": "bac20bdf-9b36-4fc9-82df-db500d31197d",
                },
            },
            "cbbe07e2-e37b-46b2-a106-ad7be2cdfbf4-0@validation_metadata": {
                "args": {
                    "validation": {
                        "target": {
                            "path": "ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d"
                        }
                    }
                },
                "data": {
                    "details": {
                        "bagit-profile": {
                            "log": {
                                "INFO": [
                                    {
                                        "body": "Loading profile from 'https://lzv.nrw/bagit_profile.json'.",
                                        "datetime": "2025-03-17T10:04:48+00:00",
                                        "origin": "BagIt-Validation-Plugin",
                                    },
                                    {
                                        "body": "'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d': Validating bag.",
                                        "datetime": "2025-03-17T10:04:48+00:00",
                                        "origin": "BagIt-Validation-Plugin",
                                    },
                                    {
                                        "body": "'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d': Bag is valid.",
                                        "datetime": "2025-03-17T10:04:48+00:00",
                                        "origin": "BagIt-Validation-Plugin",
                                    },
                                ],
                                "WARNING": [
                                    {
                                        "body": "Failed to retrieve the profile from the bag's bag-info from 'https://lzv.nrw/bagit_profile.json'. Using the default BagIt-profile from 'https://lzv.nrw/bagit_profile.json'.",
                                        "datetime": "2025-03-17T10:04:48+00:00",
                                        "origin": "BagIt-Validation-Plugin",
                                    }
                                ],
                            },
                            "success": True,
                            "valid": True,
                        },
                        "payload-structure": {
                            "log": {
                                "INFO": [
                                    {
                                        "body": "Loading profile from 'https://lzv.nrw/payload_profile.json'.",
                                        "datetime": "2025-03-17T10:04:48+00:00",
                                        "origin": "Payload Structure Validation",
                                    },
                                    {
                                        "body": "'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d': Validating directory.",
                                        "datetime": "2025-03-17T10:04:48+00:00",
                                        "origin": "Payload Structure Validation",
                                    },
                                    {
                                        "body": "'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d': Directory is valid.",
                                        "datetime": "2025-03-17T10:04:48+00:00",
                                        "origin": "Payload Structure Validation",
                                    },
                                ],
                                "WARNING": [
                                    {
                                        "body": "Failed to retrieve the profile from the bag's bag-info from 'https://lzv.nrw/payload_profile.json': HTTP Error 404: Not Found Using the default BagIt-payload-profile from 'https://lzv.nrw/payload_profile.json'.",
                                        "datetime": "2025-03-17T10:04:48+00:00",
                                        "origin": "Payload Structure Validation",
                                    }
                                ],
                            },
                            "success": True,
                            "valid": True,
                        },
                    },
                    "success": True,
                    "valid": True,
                },
                "host": "http://ip-builder/",
                "log": {
                    "EVENT": [
                        {
                            "body": "Produced at 2025-03-17T10:04:47.802049+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Consumed at 2025-03-17T10:04:48.643818+00:00 by 'UBPORT277'.",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Start executing Job.",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Job terminated normally.",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "IP Builder",
                        },
                    ],
                    "INFO": [
                        {
                            "body": "Validating IP 'ip/ac9cb7ff-2bd9-4e99-abb1-0b3c931e855d'.",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Calling plugin 'BagIt-Validation-Plugin'",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Calling plugin 'Payload Structure Validation'",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "IP Builder",
                        },
                        {
                            "body": "Target is valid.",
                            "datetime": "2025-03-17T10:04:48+00:00",
                            "origin": "IP Builder",
                        },
                    ],
                },
                "progress": {
                    "numeric": 100,
                    "status": "completed",
                    "verbose": "shutting down after success",
                },
                "token": {
                    "expires": True,
                    "expires_at": "2025-03-17T11:04:47+00:00",
                    "value": "cbbe07e2-e37b-46b2-a106-ad7be2cdfbf4",
                },
            },
        },
        "data": {
            "records": {
                "test:oai_dc:e827ab70-bb58-4da0-9faa-a0f5bc2d5795": {
                    "completed": True,
                    "stages": {
                        "build_ip": {
                            "completed": True,
                            "logId": "09e48f2f-eef5-4193-87da-4d408f0abcba-0@build_ip",
                            "success": True,
                        },
                        "build_sip": {
                            "completed": True,
                            "logId": "9e2c3ff7-58e2-4fe3-aaef-93941e8c7e2f-0@build_sip",
                            "success": True,
                        },
                        "import_ies": {
                            "completed": True,
                            "logId": "bac20bdf-9b36-4fc9-82df-db500d31197d-0@import_ies",
                            "success": True,
                        },
                        "ingest": {
                            "completed": True,
                            "logId": "58eca129-52f4-4592-abe6-df3af5547ff1-0@ingest",
                            "success": True,
                        },
                        "transfer": {
                            "completed": True,
                            "logId": "16b4ecc0-3c06-4daf-976c-f47ebbdbabae-0@transfer",
                            "success": True,
                        },
                        "validation_metadata": {
                            "completed": True,
                            "logId": "cbbe07e2-e37b-46b2-a106-ad7be2cdfbf4-0@validation_metadata",
                            "success": True,
                        },
                        "validation_payload": {
                            "completed": True,
                            "logId": "8864e196-4144-4f4b-8c92-9019971ddc1d-0@validation_payload",
                            "success": True,
                        },
                    },
                    "success": True,
                }
            },
            "success": True,
        },
        "host": "http://job-processor/",
        "log": {
            "EVENT": [
                {
                    "body": "Produced at 2025-03-17T10:04:43.390526+00:00 by 'UBPORT277'.",
                    "datetime": "2025-03-17T10:04:43+00:00",
                    "origin": "Job Processor",
                },
                {
                    "body": "Consumed at 2025-03-17T10:04:43.648112+00:00 by 'UBPORT277'.",
                    "datetime": "2025-03-17T10:04:43+00:00",
                    "origin": "Job Processor",
                },
                {
                    "body": "Start executing Job.",
                    "datetime": "2025-03-17T10:04:43+00:00",
                    "origin": "Job Processor",
                },
                {
                    "body": "Starting processor for job 'import_ies > ingest'.",
                    "datetime": "2025-03-17T10:04:43+00:00",
                    "origin": "Job Processor",
                },
                {
                    "body": "Job terminated normally.",
                    "datetime": "2025-03-17T10:04:55+00:00",
                    "origin": "Job Processor",
                },
            ],
            "INFO": [
                {
                    "body": "Job has been successful.",
                    "datetime": "2025-03-17T10:04:55+00:00",
                    "origin": "Job Processor",
                }
            ],
        },
        "progress": {
            "numeric": 100,
            "status": "completed",
            "verbose": "shutting down after success",
        },
        "token": {
            "expires": True,
            "expires_at": "2025-03-17T11:04:43+00:00",
            "value": DemoData.token1,
        },
    }

    db.insert(
        "jobs",
        {
            "token": DemoData.token1,
            "job_config_id": DemoData.job_config1,
            "datetime_triggered": "2025-03-17T10:00:00+00:00",
            "trigger_type": TriggerType.MANUAL.value,
            "status": "completed",
            "success": True,
            "datetime_started": "2025-03-17T10:00:00.000000+00:00",
            "datetime_ended": "2025-03-17T10:01:00.000000+00:00",
            "report": report1,
        },
    )
    for report_id, record in report1["data"]["records"].items():
        # only single record expected, set id explicitly
        db.insert(
            "records",
            {
                "id": DemoData.record1,
                "report_id": report_id,
                "job_token": DemoData.token1,
                "success": record["success"],
                "external_id": DemoData.record1,
                "origin_system_id": "oai:demo",
                "sip_id": "SIP0",
                "ie_id": "IE0",
                "datetime_processed": now().isoformat()
            },
        )
