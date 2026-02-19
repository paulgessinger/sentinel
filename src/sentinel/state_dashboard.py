import asyncio
import json
from math import ceil
from typing import Any, Dict

import emoji
from markdown_it import MarkdownIt
from sanic import Request, Sanic, response
from sanic.exceptions import NotFound


DEFAULT_STATE_PAGE_SIZE = 100
MAX_STATE_PAGE_SIZE = 200
MARKDOWN_RENDERER = MarkdownIt("default", {"html": False})


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


def _arg_value(request: Request, key: str, default: str | None = None) -> str | None:
    values = _arg_values(request, key)
    if not values:
        return default
    return values[-1]


def _arg_values(request: Request, key: str) -> list[str]:
    args = request.args
    values: list[Any]

    if hasattr(args, "getlist"):
        values = list(args.getlist(key))
    elif hasattr(args, "getall"):
        values = list(args.getall(key))
    else:
        value = args.get(key)
        if value is None:
            values = []
        elif isinstance(value, list):
            values = value
        else:
            values = [value]

    normalized: list[str] = []
    for value in values:
        if value is None:
            continue
        normalized.append(str(value))
    return normalized


def _parse_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except TypeError, ValueError:
        return default


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _state_query_params(request: Request) -> tuple[int, int, str | None, bool]:
    page = max(1, _parse_int(_arg_value(request, "page"), 1))
    page_size = _parse_int(_arg_value(request, "page_size"), DEFAULT_STATE_PAGE_SIZE)
    page_size = min(MAX_STATE_PAGE_SIZE, max(1, page_size))
    repo = _arg_value(request, "repo")
    if repo is not None:
        repo = repo.strip() or None
    include_closed_values = _arg_values(request, "include_closed")
    if include_closed_values:
        include_closed = any(
            _parse_bool(value, False) for value in include_closed_values
        )
    else:
        include_closed = True
    return page, page_size, repo, include_closed


def _github_pr_url(repo_full_name: str | None, pr_number: int | None) -> str | None:
    if not repo_full_name or pr_number is None:
        return None
    return f"https://github.com/{repo_full_name}/pull/{pr_number}"


def _github_commit_url(repo_full_name: str | None, head_sha: str | None) -> str | None:
    if not repo_full_name or not head_sha:
        return None
    return f"https://github.com/{repo_full_name}/commit/{head_sha}"


def _github_check_run_url(
    repo_full_name: str | None, check_run_id: int | None
) -> str | None:
    if not repo_full_name or check_run_id is None:
        return None
    return f"https://github.com/{repo_full_name}/runs/{check_run_id}?check_suite_focus=true"


def _pr_state_display(pr_state: str | None, pr_merged: bool | None) -> str:
    if pr_state == "closed":
        if pr_merged is True:
            return "merged"
        if pr_merged is False:
            return "closed (not merged)"
        return "closed"
    if not pr_state:
        return "unknown"
    return pr_state


def _pr_state_chip_class(pr_state_display: str) -> str:
    if pr_state_display == "merged":
        return "chip-bg-green"
    if pr_state_display.startswith("closed"):
        return "chip-bg-red"
    return ""


def _pr_draft_chip_class(is_draft: bool) -> str:
    if is_draft:
        return "chip-bg-amber"
    return ""


def _row_update_signature(row: Any) -> str:
    values = (
        row.get("pr_updated_at"),
        row.get("pr_state"),
        row.get("pr_merged"),
        row.get("pr_is_draft"),
        row.get("head_sha"),
        row.get("sentinel_status"),
        row.get("sentinel_conclusion"),
        row.get("last_eval_at"),
        row.get("last_publish_at"),
        row.get("last_publish_result"),
        row.get("last_publish_error"),
        row.get("output_summary_hash"),
        row.get("output_text_hash"),
        row.get("sentinel_check_run_id"),
    )
    return "|".join("" if value is None else str(value) for value in values)


def _state_dashboard_context(
    app: Sanic,
    *,
    page: int,
    page_size: int,
    repo_filter: str | None,
    include_closed: bool,
) -> Dict[str, Any]:
    total = app.ctx.webhook_store.count_pr_dashboard_rows(
        repo_full_name=repo_filter,
        include_closed=include_closed,
    )
    total_pages = max(1, ceil(total / page_size)) if total > 0 else 1
    page = min(page, total_pages)

    rows = app.ctx.webhook_store.list_pr_dashboard_rows(
        app_id=app.config.GITHUB_APP_ID,
        check_name=app.config.PROJECTION_CHECK_RUN_NAME,
        page=page,
        page_size=page_size,
        repo_full_name=repo_filter,
        include_closed=include_closed,
    )
    enriched_rows = []
    for row in rows:
        row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
        repo_full_name = row.get("repo_full_name")
        pr_number = row.get("pr_number")
        head_sha = row.get("head_sha")
        check_run_id = row.get("sentinel_check_run_id")
        repo_id = row.get("repo_id")
        row_key = (
            f"{repo_id}:{pr_number}"
            if repo_id is not None and pr_number is not None
            else ""
        )
        pr_state_display = _pr_state_display(
            row.get("pr_state"),
            row.get("pr_merged"),
        )
        pr_is_draft = bool(row.get("pr_is_draft"))
        pr_title_raw = row.get("pr_title") or (
            f"PR #{pr_number}" if pr_number is not None else "PR"
        )
        enriched_rows.append(
            {
                **row_dict,
                "short_sha": head_sha[:8] if head_sha else "",
                "row_key": row_key,
                "row_update_signature": _row_update_signature(row_dict),
                "pr_state_display": pr_state_display,
                "pr_state_class": _pr_state_chip_class(pr_state_display),
                "pr_is_draft": pr_is_draft,
                "pr_draft_class": _pr_draft_chip_class(pr_is_draft),
                "pr_url": _github_pr_url(repo_full_name, pr_number),
                "commit_url": _github_commit_url(repo_full_name, head_sha),
                "check_run_url": _github_check_run_url(repo_full_name, check_run_id),
                "sentinel_status_class": _status_chip_class(row.get("sentinel_status")),
                "sentinel_conclusion_class": _conclusion_chip_class(
                    row.get("sentinel_conclusion")
                ),
                "publish_result_display": _publish_result_display(
                    publish_result=row.get("last_publish_result"),
                    status=row.get("sentinel_status"),
                    conclusion=row.get("sentinel_conclusion"),
                ),
                "details_url": (
                    f"/state/pr/{row.get('repo_id')}/{pr_number}"
                    if row.get("repo_id") is not None and pr_number is not None
                    else None
                ),
                "rendered_pr_title": _render_markdown_inline(pr_title_raw),
            }
        )

    return {
        "rows": enriched_rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "repo_filter": repo_filter or "",
        "include_closed": include_closed,
    }


def _publish_result_display(
    *, publish_result: str | None, status: str | None, conclusion: str | None
) -> str:
    if not publish_result:
        return "-"
    if publish_result != "dry_run":
        if publish_result != "unchanged":
            return publish_result
    would_post = status or "unknown"
    if conclusion:
        would_post = f"{would_post}/{conclusion}"
    if publish_result == "dry_run":
        return f"{would_post} (dry-run)"
    return f"{would_post} (unchanged)"


def _status_chip_class(status: str | None) -> str:
    if status == "failure":
        return "chip-bg-red"
    return ""


def _conclusion_chip_class(conclusion: str | None) -> str:
    if conclusion == "success":
        return "chip-bg-green"
    if conclusion == "failure":
        return "chip-bg-red"
    return ""


def _render_markdown(markdown_text: str | None) -> str:
    if markdown_text is None or markdown_text.strip() == "":
        return '<p class="muted">Not available yet</p>'
    emojized = emoji.emojize(markdown_text, language="alias")
    rendered = MARKDOWN_RENDERER.render(emojized)
    return rendered.replace("<table>", '<table class="rendered-check-table">')


def _render_markdown_inline(text: str | None) -> str:
    """Render short inline text (e.g. PR titles) as markdown without block wrappers."""
    if text is None or not text.strip():
        return ""
    emojized = emoji.emojize(text, language="alias")
    return MARKDOWN_RENDERER.renderInline(emojized)


def _check_status_display(status: str | None) -> Dict[str, Any]:
    normalized = (status or "unknown").strip().lower()
    if normalized == "success":
        return {
            "label": "success",
            "symbol": "✓",
            "class_name": "check-status-success",
            "is_running": False,
        }
    if normalized == "failure":
        return {
            "label": "failure",
            "symbol": "✕",
            "class_name": "check-status-failure",
            "is_running": False,
        }
    if normalized in {"pending", "in_progress", "queued"}:
        return {
            "label": "running",
            "symbol": "…",
            "class_name": "check-status-running",
            "is_running": True,
        }
    if normalized == "missing":
        return {
            "label": "missing",
            "symbol": "!",
            "class_name": "check-status-missing",
            "is_running": False,
        }
    return {
        "label": normalized,
        "symbol": "•",
        "class_name": "check-status-neutral",
        "is_running": False,
    }


def _parse_output_checks(output_checks_json: str | None) -> list[Dict[str, Any]]:
    if output_checks_json is None or output_checks_json.strip() == "":
        return []

    try:
        parsed = json.loads(output_checks_json)
    except ValueError:
        return []
    if not isinstance(parsed, list):
        return []

    rows: list[Dict[str, Any]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        name_value = item.get("name")
        if not isinstance(name_value, str) or not name_value.strip():
            continue
        status_value = item.get("status")
        required = bool(item.get("required"))
        html_url_value = item.get("html_url")
        html_url = (
            html_url_value
            if isinstance(html_url_value, str)
            and html_url_value.startswith(("https://", "http://"))
            else None
        )
        source_value = item.get("source")
        source = source_value if isinstance(source_value, str) else "derived"
        status_display = _check_status_display(
            status_value if isinstance(status_value, str) else None
        )
        rows.append(
            {
                "name": name_value,
                "status": status_display["label"],
                "status_symbol": status_display["symbol"],
                "status_class": status_display["class_name"],
                "is_running": status_display["is_running"],
                "required": required,
                "html_url": html_url,
                "source": source,
            }
        )

    return rows


def _state_pr_detail_context(
    app: Sanic,
    *,
    repo_id: int,
    pr_number: int,
) -> Dict[str, Any] | None:
    row = app.ctx.webhook_store.get_pr_dashboard_row(
        app_id=app.config.GITHUB_APP_ID,
        check_name=app.config.PROJECTION_CHECK_RUN_NAME,
        repo_id=repo_id,
        pr_number=pr_number,
    )
    if row is None:
        return None

    repo_full_name = row.get("repo_full_name")
    head_sha = row.get("head_sha")
    check_run_id = row.get("sentinel_check_run_id")
    output_summary = row.get("output_summary")
    output_text = row.get("output_text")
    output_checks_json = row.get("output_checks_json")

    events = app.ctx.webhook_store.list_pr_related_events(
        repo_id=repo_id,
        pr_number=pr_number,
        head_sha=head_sha,
        limit=250,
    )
    for event in events:
        event["details_url"] = None
        delivery_id = event.get("delivery_id")
        if (
            delivery_id
            and isinstance(delivery_id, str)
            and not delivery_id.startswith("activity-")
        ):
            event["details_url"] = (
                f"/state/event/{delivery_id}?repo_id={repo_id}&pr_number={pr_number}"
            )
    event_rows = [
        event.to_dict() if hasattr(event, "to_dict") else dict(event)
        for event in events
    ]

    pr_state_display = _pr_state_display(
        row.get("pr_state"),
        row.get("pr_merged"),
    )
    pr_is_draft = bool(row.get("pr_is_draft"))
    row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    return {
        "row": {
            **row_dict,
            "short_sha": head_sha[:8] if head_sha else "",
            "pr_url": _github_pr_url(repo_full_name, pr_number),
            "commit_url": _github_commit_url(repo_full_name, head_sha),
            "check_run_url": _github_check_run_url(repo_full_name, check_run_id),
            "pr_state_display": pr_state_display,
            "pr_state_class": _pr_state_chip_class(pr_state_display),
            "pr_is_draft": pr_is_draft,
            "pr_draft_class": _pr_draft_chip_class(pr_is_draft),
            "sentinel_status_class": _status_chip_class(row.get("sentinel_status")),
            "sentinel_conclusion_class": _conclusion_chip_class(
                row.get("sentinel_conclusion")
            ),
            "publish_result_display": _publish_result_display(
                publish_result=row.get("last_publish_result"),
                status=row.get("sentinel_status"),
                conclusion=row.get("sentinel_conclusion"),
            ),
            "output_checks": _parse_output_checks(
                output_checks_json if isinstance(output_checks_json, str) else None
            ),
            "rendered_output_summary": _render_markdown(output_summary),
            "rendered_output_text": _render_markdown(output_text),
        },
        "events": event_rows,
    }


def _state_event_detail_context(
    app: Sanic,
    *,
    delivery_id: str,
    repo_id: int | None = None,
    pr_number: int | None = None,
) -> Dict[str, Any] | None:
    event = app.ctx.webhook_store.get_webhook_event(delivery_id)
    if event is None:
        return None

    event["payload_pretty"] = json.dumps(
        event.get("payload") or {}, indent=2, sort_keys=True
    )
    if repo_id is not None and pr_number is not None:
        event["pr_detail_url"] = f"/state/pr/{repo_id}/{pr_number}"
    else:
        event["pr_detail_url"] = None

    event_row = event.to_dict() if hasattr(event, "to_dict") else dict(event)
    return {
        "event": event_row,
    }


def register_state_routes(app: Sanic) -> None:
    @app.get("/state")
    @app.ext.template("state.html.j2")
    async def state(_request):
        page, page_size, repo_filter, include_closed = _state_query_params(_request)
        return {
            "app": app,
            **_state_dashboard_context(
                app,
                page=page,
                page_size=page_size,
                repo_filter=repo_filter,
                include_closed=include_closed,
            ),
        }

    @app.get("/state/table")
    @app.ext.template("state_table.html.j2")
    async def state_table(_request):
        page, page_size, repo_filter, include_closed = _state_query_params(_request)
        return {
            "app": app,
            **_state_dashboard_context(
                app,
                page=page,
                page_size=page_size,
                repo_filter=repo_filter,
                include_closed=include_closed,
            ),
        }

    @app.get("/state/pr/<repo_id:int>/<pr_number:int>")
    @app.ext.template("state_pr_detail.html.j2")
    async def state_pr_detail(_request, repo_id: int, pr_number: int):
        context = _state_pr_detail_context(app, repo_id=repo_id, pr_number=pr_number)
        if context is None:
            raise NotFound(f"PR not found: repo_id={repo_id} pr_number={pr_number}")
        return {
            "app": app,
            **context,
        }

    @app.get("/state/pr/<repo_id:int>/<pr_number:int>/content")
    @app.ext.template("state_pr_detail_content.html.j2")
    async def state_pr_detail_content(_request, repo_id: int, pr_number: int):
        context = _state_pr_detail_context(app, repo_id=repo_id, pr_number=pr_number)
        if context is None:
            raise NotFound(f"PR not found: repo_id={repo_id} pr_number={pr_number}")
        return {
            "app": app,
            **context,
        }

    @app.get("/state/event/<delivery_id:str>")
    @app.ext.template("state_event_detail.html.j2")
    async def state_event_detail(_request, delivery_id: str):
        repo_id = _parse_int(_arg_value(_request, "repo_id"), 0)
        pr_number = _parse_int(_arg_value(_request, "pr_number"), 0)
        context = _state_event_detail_context(
            app,
            delivery_id=delivery_id,
            repo_id=repo_id if repo_id > 0 else None,
            pr_number=pr_number if pr_number > 0 else None,
        )
        if context is None:
            raise NotFound(f"Webhook event not found: delivery_id={delivery_id}")
        return {
            "app": app,
            **context,
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
                    await stream_response.write(
                        f"event: state_update\ndata: {body}\n\n"
                    )
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
