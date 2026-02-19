import sqlite3

from sentinel.db_migrations import migrate_webhook_db


def test_migrate_webhook_db_runs_from_packaged_scripts(tmp_path, monkeypatch):
    db_path = tmp_path / "runtime" / "webhooks.sqlite3"
    monkeypatch.chdir(tmp_path)

    migrate_webhook_db(db_path, revision="head")

    with sqlite3.connect(str(db_path)) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        revision = conn.execute("SELECT version_num FROM alembic_version").fetchone()[0]

    assert "webhook_events" in tables
    assert "pr_heads_current" in tables
    assert revision == "0005_add_pr_draft_to_pr_heads_current"
