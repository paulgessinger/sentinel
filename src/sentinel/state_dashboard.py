import asyncio
import json
from math import ceil
from typing import Any, Dict, Optional

from sanic import Request, Sanic, response


DEFAULT_STATE_PAGE_SIZE = 100
MAX_STATE_PAGE_SIZE = 200


class StateUpdateBroadcaster:
    def __init__(self, queue_size: int = 100):
        self.queue_size = max(1, int(queue_size))
        self._lock = asyncio.Lock()
        self._subscribers: set[asyncio.Queue] = set()

    async def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)
        async with self._lock:
            self._subscribers.add(queue)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue) -> None:
        async with self._lock:
            self._subscribers.discard(queue)

    async def publish(self, payload: Dict[str, Any]) -> None:
        async with self._lock:
            subscribers = list(self._subscribers)

        for queue in subscribers:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                pass


def _arg_value(request: Request, key: str, default: Optional[str] = None) -> Optional[str]:
    value = request.args.get(key, default)
    if isinstance(value, list):
        return value[0] if value else default
    return value


def _parse_int(value: Optional[str], default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _state_query_params(request: Request) -> tuple[int, int, Optional[str]]:
    page = max(1, _parse_int(_arg_value(request, "page"), 1))
    page_size = _parse_int(_arg_value(request, "page_size"), DEFAULT_STATE_PAGE_SIZE)
    page_size = min(MAX_STATE_PAGE_SIZE, max(1, page_size))
    repo = _arg_value(request, "repo")
    if repo is not None:
        repo = repo.strip() or None
    return page, page_size, repo


def _github_pr_url(repo_full_name: Optional[str], pr_number: Optional[int]) -> Optional[str]:
    if not repo_full_name or pr_number is None:
        return None
    return f"https://github.com/{repo_full_name}/pull/{pr_number}"


def _github_commit_url(repo_full_name: Optional[str], head_sha: Optional[str]) -> Optional[str]:
    if not repo_full_name or not head_sha:
        return None
    return f"https://github.com/{repo_full_name}/commit/{head_sha}"


def _github_check_run_url(
    repo_full_name: Optional[str], check_run_id: Optional[int]
) -> Optional[str]:
    if not repo_full_name or check_run_id is None:
        return None
    return f"https://github.com/{repo_full_name}/runs/{check_run_id}?check_suite_focus=true"


def _state_dashboard_context(
    app: Sanic,
    *,
    page: int,
    page_size: int,
    repo_filter: Optional[str],
) -> Dict[str, Any]:
    total = app.ctx.webhook_store.count_pr_dashboard_rows(repo_full_name=repo_filter)
    total_pages = max(1, ceil(total / page_size)) if total > 0 else 1
    page = min(page, total_pages)

    rows = app.ctx.webhook_store.list_pr_dashboard_rows(
        app_id=app.config.GITHUB_APP_ID,
        check_name=app.config.PROJECTION_CHECK_RUN_NAME,
        page=page,
        page_size=page_size,
        repo_full_name=repo_filter,
    )
    enriched_rows = []
    for row in rows:
        repo_full_name = row.get("repo_full_name")
        pr_number = row.get("pr_number")
        head_sha = row.get("head_sha")
        check_run_id = row.get("sentinel_check_run_id")
        enriched_rows.append(
            {
                **row,
                "short_sha": head_sha[:12] if head_sha else "",
                "pr_url": _github_pr_url(repo_full_name, pr_number),
                "commit_url": _github_commit_url(repo_full_name, head_sha),
                "check_run_url": _github_check_run_url(repo_full_name, check_run_id),
            }
        )

    return {
        "rows": enriched_rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "repo_filter": repo_filter or "",
    }


def register_state_routes(app: Sanic) -> None:
    @app.get("/state")
    @app.ext.template("state.html.j2")
    async def state(_request):
        page, page_size, repo_filter = _state_query_params(_request)
        return {
            "app": app,
            **_state_dashboard_context(
                app,
                page=page,
                page_size=page_size,
                repo_filter=repo_filter,
            ),
        }

    @app.get("/state/table")
    @app.ext.template("state_table.html.j2")
    async def state_table(_request):
        page, page_size, repo_filter = _state_query_params(_request)
        return {
            "app": app,
            **_state_dashboard_context(
                app,
                page=page,
                page_size=page_size,
                repo_filter=repo_filter,
            ),
        }

    @app.get("/state/stream")
    async def state_stream(_request):
        queue = await app.ctx.state_broadcaster.subscribe()

        async def stream_fn(stream_response):
            try:
                await stream_response.write("event: ready\ndata: {}\n\n")
                while True:
                    try:
                        payload = await asyncio.wait_for(queue.get(), timeout=20.0)
                    except asyncio.TimeoutError:
                        await stream_response.write(": keepalive\n\n")
                        continue
                    body = json.dumps(payload, separators=(",", ":"))
                    await stream_response.write(f"event: state_update\ndata: {body}\n\n")
            except asyncio.CancelledError:
                raise
            finally:
                await app.ctx.state_broadcaster.unsubscribe(queue)

        return response.ResponseStream(
            stream_fn,
            content_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
