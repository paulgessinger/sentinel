from __future__ import annotations

from pathlib import Path
from typing import Union

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _build_alembic_config(db_path: Path):
    config_path = _repo_root() / "alembic.ini"
    alembic_cfg = AlembicConfig(str(config_path))
    alembic_cfg.set_main_option("script_location", str(_repo_root() / "db_migrations"))
    alembic_cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    return alembic_cfg


def migrate_webhook_db(path: Union[str, Path], revision: str = "head") -> None:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    alembic_cfg = _build_alembic_config(db_path)
    alembic_command.upgrade(alembic_cfg, revision)
