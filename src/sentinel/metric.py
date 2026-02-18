from pathlib import Path

from prometheus_client import core, Counter, Gauge

request_counter = Counter(
    "sentinel_num_req", "Total number of requests", labelnames=["path"]
)
webhook_counter = Counter(
    "sentinel_num_webhook", "Total number of webhook", labelnames=["event", "name"]
)
webhook_skipped_counter = Counter(
    "sentinel_num_webhook_skipped",
    "Total number of skipped webhooks",
    labelnames=["event", "name"],
)

pr_update_trigger_counter = Counter(
    "sentinel_num_pr_update_triggered",
    "Number of PR updates triggered",
    labelnames=["event", "name"],
)

pr_update_accept_counter = Counter(
    "sentinel_num_pr_update_accepted",
    "Number of PR updates accepted into queue",
    labelnames=["event", "name"],
)

pr_update_duplicate = Counter(
    "sentinel_pr_update_duplicate",
    "Number of duplicate items received",
    labelnames=["event", "name"],
)

queue_size = Gauge("sentinel_queue_size", "Size of the PR queue")

error_counter = Counter(
    "sentinel_error_counter", "Total number of errors", labelnames=["context"]
)

api_call_count = Counter("sentinel_num_api_calls", "Total number of GitHub API calls")

check_run_post = Counter(
    "sentinel_check_run_post",
    "Number of check run update posts",
    labelnames=["skipped", "difference"],
)

worker_error_count = Counter(
    "sentinel_num_worker_error",
    "Number of errors encountered by the update worker",
)

webhook_persist_total = Counter(
    "sentinel_webhook_persist_total",
    "Webhook persistence outcomes",
    labelnames=["event", "result"],
)

webhook_project_total = Counter(
    "sentinel_webhook_project_total",
    "Webhook projection outcomes",
    labelnames=["projection", "result"],
)

webhook_event_pruned_total = Counter(
    "sentinel_webhook_event_pruned_total",
    "Number of old webhook_events rows pruned",
)

webhook_projection_pruned_total = Counter(
    "sentinel_webhook_projection_pruned_total",
    "Number of old projection rows pruned",
    labelnames=["table", "kind"],
)

sentinel_projection_eval_total = Counter(
    "sentinel_projection_eval_total",
    "Projection evaluation outcomes",
    labelnames=["result"],
)

sentinel_projection_debounce_total = Counter(
    "sentinel_projection_debounce_total",
    "Projection debounce scheduling outcomes",
    labelnames=["result"],
)

sentinel_projection_publish_total = Counter(
    "sentinel_projection_publish_total",
    "Projection publish outcomes",
    labelnames=["result"],
)

sentinel_projection_lookup_total = Counter(
    "sentinel_projection_lookup_total",
    "Projection check run lookup outcomes",
    labelnames=["result"],
)

sentinel_projection_fallback_total = Counter(
    "sentinel_projection_fallback_total",
    "Projection fallback cache/read outcomes",
    labelnames=["kind", "result"],
)

webhook_db_size_bytes = Gauge(
    "sentinel_webhook_db_size_bytes",
    "Total SQLite webhook DB size on disk in bytes (db + wal + shm)",
)


def sqlite_db_total_size_bytes(db_path: Path) -> float:
    total_size = 0
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{db_path}{suffix}")
        try:
            if candidate.is_file():
                total_size += candidate.stat().st_size
        except OSError:
            continue
    return float(total_size)


def configure_webhook_db_size_metric(db_path: str) -> None:
    db_path_value = Path(db_path)
    webhook_db_size_bytes.set_function(
        lambda: sqlite_db_total_size_bytes(db_path_value)
    )
