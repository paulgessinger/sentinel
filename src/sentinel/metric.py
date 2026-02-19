from pathlib import Path
import re
from urllib.parse import urlparse

from prometheus_client import Counter, Gauge, Histogram

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

api_call_count = Counter(
    "sentinel_num_api_calls",
    "Total number of GitHub API calls",
    labelnames=["endpoint"],
)

view_response_latency_seconds = Histogram(
    "sentinel_view_response_latency_seconds",
    "Latency in seconds for HTML view responses",
    labelnames=["path", "method", "status"],
)

webhook_processing_seconds = Histogram(
    "sentinel_webhook_processing_seconds",
    "Background webhook processing duration in seconds",
    labelnames=["event", "result"],
)

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


def record_api_call(endpoint: str, count: int = 1) -> None:
    if count <= 0:
        return
    api_call_count.labels(endpoint=_normalize_api_endpoint(endpoint)).inc(count)


def observe_view_response_latency(
    *,
    path: str,
    method: str,
    status: int,
    seconds: float,
) -> None:
    if seconds < 0:
        return
    view_response_latency_seconds.labels(
        path=path,
        method=method,
        status=str(status),
    ).observe(seconds)


def observe_webhook_processing_latency(
    *, event: str, result: str, seconds: float
) -> None:
    if seconds < 0:
        return
    webhook_processing_seconds.labels(event=event, result=result).observe(seconds)


def _normalize_api_endpoint(endpoint: str) -> str:
    value = endpoint.strip()
    if not value:
        return "unknown"

    # Allow explicit semantic names to pass through.
    if "/" not in value and " " not in value and ":" not in value:
        return value

    parsed = urlparse(value)
    path = parsed.path or value.split("?", 1)[0]
    path = path.rstrip("/") or "/"

    if path == "/app":
        return "app"
    if "/app/installations/" in path and path.endswith("/access_tokens"):
        return "installation_token"
    if "/contents/" in path:
        return "contents/xxx"
    if "/pulls" in path:
        return "pulls"
    if "/commits/" in path and path.endswith("/check-runs"):
        return "commits/check-runs"
    if "/check-runs" in path:
        return "check-runs"
    if "/commits/" in path and path.endswith("/check-suites"):
        return "commits/check-suites"
    if "/check-suites" in path:
        return "check-suites"
    if "/actions/jobs/" in path:
        return "actions/jobs"
    if "/actions/runs" in path:
        return "actions/runs"
    if "/commits/" in path and path.endswith("/status"):
        return "commits/status"
    if path.startswith("/repos/"):
        return "repos"

    # Fallback: collapse obvious high-cardinality tokens.
    collapsed = re.sub(r"/[0-9]+", "/:id", path)
    collapsed = re.sub(r"/[0-9a-f]{40}", "/:sha", collapsed)
    return collapsed
