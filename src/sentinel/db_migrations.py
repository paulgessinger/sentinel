from __future__ import annotations

from contextlib import ExitStack
from importlib import resources
from pathlib import Path

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig


_MIGRATIONS_PACKAGE = "sentinel.db_migration_scripts"


def _build_alembic_config(db_path: Path, script_location: Path) -> AlembicConfig:
    alembic_cfg = AlembicConfig()
    alembic_cfg.set_main_option("script_location", str(script_location))
    alembic_cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    return alembic_cfg


def migrate_webhook_db(path: str | Path, revision: str = "head") -> None:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        script_location = stack.enter_context(
            resources.as_file(resources.files(_MIGRATIONS_PACKAGE))
        )
        alembic_cfg = _build_alembic_config(db_path, script_location)
        alembic_command.upgrade(alembic_cfg, revision)
