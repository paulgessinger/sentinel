from pathlib import Path

from sentinel.metric import (
    api_call_count,
    record_api_call,
    sqlite_db_total_size_bytes,
    view_response_latency_seconds,
    observe_view_response_latency,
)


def test_sqlite_db_total_size_bytes_includes_wal_and_shm(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")

    db_path.write_bytes(b"a" * 10)
    wal_path.write_bytes(b"b" * 20)
    shm_path.write_bytes(b"c" * 30)

    assert sqlite_db_total_size_bytes(db_path) == 60.0


def test_record_api_call_tracks_endpoint_label():
    endpoint = "/repos/org/repo/pulls/42"
    before = api_call_count.labels(endpoint=endpoint)._value.get()
    record_api_call(endpoint=endpoint)
    after = api_call_count.labels(endpoint=endpoint)._value.get()
    assert after == before + 1


def test_observe_view_response_latency_tracks_path_method_status():
    labels = {
        "path": "state/pr/<repo_id:int>/<pr_number:int>",
        "method": "GET",
        "status": "200",
    }
    metric = view_response_latency_seconds.labels(**labels)
    before_sum = metric._sum.get()
    before_bucket_total = sum(bucket.get() for bucket in metric._buckets)
    observe_view_response_latency(
        path=labels["path"],
        method=labels["method"],
        status=int(labels["status"]),
        seconds=0.25,
    )
    after_sum = metric._sum.get()
    after_bucket_total = sum(bucket.get() for bucket in metric._buckets)
    assert after_sum > before_sum
    assert after_bucket_total == before_bucket_total + 1
