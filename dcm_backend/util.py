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
    Hotfolder,
    ArchiveConfiguration,
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
                f"Unable to deserialize hotfolder '{hotfolder['id']}': "
                + f"{exc_info}"
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


def load_archive_configurations_from_string(
    json: str,
) -> dict[str, ArchiveConfiguration]:
    """Loads archive configurations from the given JSON-string."""

    try:
        archives_json = loads(json)
    except JSONDecodeError as exc_info:
        raise ValueError(
            f"Invalid archive configuration: {exc_info}."
        ) from exc_info

    if not isinstance(archives_json, list):
        raise ValueError(
            "Invalid archive configuration: Expected list of archive "
            + f"configurations but got '{type(archives_json).__name__}'."
        )

    archives = {}
    for archive in archives_json:
        if not isinstance(archive.get("id"), str):
            raise ValueError(
                f"Bad archive id '{archive.get('id')}' (bad type)."
            )
        if archive["id"] in archives:
            raise ValueError(f"Non-unique archive id '{archive['id']}'.")
        try:
            archives[archive["id"]] = ArchiveConfiguration.from_json(archive)
        except (TypeError, ValueError, KeyError) as exc_info:
            raise ValueError(
                "Unable to deserialize archive configuration "
                + f"'{archive['id']}' ({type(exc_info).__name__}): "
                + f"{exc_info}"
            ) from exc_info

    return archives


def load_archive_configurations_from_file(
    path: Path,
) -> dict[str, ArchiveConfiguration]:
    """Loads archive configurations from the given `path` (JSON-file)."""
    return load_archive_configurations_from_string(
        path.read_text(encoding="utf-8")
    )


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
