from pathlib import Path

from sentinel.metric import sqlite_db_total_size_bytes


def test_sqlite_db_total_size_bytes_includes_wal_and_shm(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")

    db_path.write_bytes(b"a" * 10)
    wal_path.write_bytes(b"b" * 20)
    shm_path.write_bytes(b"c" * 30)

    assert sqlite_db_total_size_bytes(db_path) == 60.0
