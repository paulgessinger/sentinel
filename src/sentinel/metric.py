from cProfile import label
from prometheus_client import core, Counter, Gauge

request_counter = Counter(
    "sentinel_num_req", "Total number of requests", labelnames=["path"]
)
webhook_counter = Counter(
    "sentinel_num_webhook", "Total number of webhook", labelnames=["event"]
)
webhook_skipped_counter = Counter(
    "sentinel_num_webhook_skipped",
    "Total number of skipped webhooks",
    labelnames=["event", "name"],
)


queue_size = Gauge("sentinel_queue_size", "Size of the PR queue")

error_counter = Counter(
    "sentinel_error_counter", "Total number of errors", labelnames=["context"]
)


api_call_count = Counter("sentinel_num_api_calls", "Total number of GitHub API calls")
