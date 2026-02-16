from prometheus_client import core, Counter, Gauge, CollectorRegistry

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

push_registry = CollectorRegistry()

api_call_count = Counter(
    "sentinel_num_api_calls", "Total number of GitHub API calls", registry=push_registry
)

check_run_post = Counter(
    "sentinel_check_run_post",
    "Number of check run update posts",
    labelnames=["skipped", "difference"],
    registry=push_registry,
)

worker_error_count = Counter(
    "sentinel_num_worker_error",
    "Number of errors encountered by the update worker",
    registry=push_registry,
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
