"""Pace every HTTP request, including pagination, across local processes.

The web server and operator CLI share the persistent cache directory. Advisory
file locks serialize requests across the app; rate state survives restarts and
token rotation. Deployments on separate volumes do not share this budget.
"""

import asyncio
from contextlib import asynccontextmanager
import fcntl
import json
from pathlib import Path
import time

from gidgethub.aiohttp import GitHubAPI as BaseGitHubAPI

from sentinel.config import SETTINGS


class RateLimitDeferred(RuntimeError):
    def __init__(self, retry_at: float):
        self.retry_at = retry_at
        super().__init__(f"GitHub API deferred until {retry_at:.0f} (Unix time)")


class GitHubAPI(BaseGitHubAPI):
    def __init__(
        self, *args, installation_id: int | None = None, rate_directory=None, **kwargs
    ):
        super().__init__(*args, **kwargs)
        directory = Path(rate_directory or Path(SETTINGS.DISKCACHE_DIR) / "github-rate")
        directory.mkdir(parents=True, exist_ok=True)
        key = f"{SETTINGS.GITHUB_APP_ID}-{installation_id or 'app'}"
        self._state_path = directory / f"{key}.json"
        self._global_path = directory / f"{SETTINGS.GITHUB_APP_ID}-pacing.json"
        self._lock_path = directory / f"{SETTINGS.GITHUB_APP_ID}.lock"

    @asynccontextmanager
    async def _locked(self):
        with self._lock_path.open("a") as lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.1)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _save(self, state, path=None):
        path = path or self._state_path
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state))
        temporary.replace(path)

    async def _request(self, method, url, headers, body=b""):
        async with self._locked():
            # Invalid state fails closed rather than silently resetting a budget.
            state = (
                json.loads(self._state_path.read_text())
                if self._state_path.exists()
                else {}
            )
            pacing = (
                json.loads(self._global_path.read_text())
                if self._global_path.exists()
                else {}
            )
            now = time.time()
            blocked_until = max(
                state.get("blocked_until", 0), pacing.get("blocked_until", 0)
            )
            if state.get("reset", 0) > now and state.get("remaining", 5000) <= 100:
                blocked_until = max(blocked_until, state["reset"] + 1)
            if blocked_until > now:
                raise RateLimitDeferred(blocked_until)

            mutating = method.upper() not in {"GET", "HEAD", "OPTIONS"}
            ready = max(
                state.get("next_request", 0),
                pacing.get("next_request", 0),
                pacing.get("next_write", 0) if mutating else 0,
            )
            # Don't hold a task (or its installation token) for an hour. The next
            # reconciliation or webhook retries evaluation after this deadline.
            if ready - now > 10:
                raise RateLimitDeferred(ready)
            if ready > now:
                await asyncio.sleep(ready - now)
            now = time.time()
            interval = 1.0
            if state.get("reset", 0) > now:
                interval = max(
                    interval,
                    (state["reset"] - now) / max(1, state.get("remaining", 5000) - 100),
                )
                state["remaining"] = state.get("remaining", 5000) - 1
            # Reserve before sending, so a crash still consumes local budget.
            state["next_request"] = now + interval
            pacing["next_request"] = now + 1
            if mutating:
                pacing["next_write"] = now + 8
            self._save(pacing, self._global_path)
            self._save(state)

            response = await super()._request(method, url, headers, body)
            status, response_headers, response_body = response
            received = time.time()
            values = {key.lower(): value for key, value in response_headers.items()}
            if "x-ratelimit-remaining" in values:
                state["remaining"] = int(values["x-ratelimit-remaining"])
            if "x-ratelimit-reset" in values:
                state["reset"] = float(values["x-ratelimit-reset"])
            limited = status == 429 or (
                status == 403
                and (
                    "retry-after" in values
                    or state.get("remaining") == 0
                    or b"rate limit" in response_body.lower()
                    or b"abuse" in response_body.lower()
                )
            )
            if limited:
                failures = state.get("limit_failures", 0) + 1
                state["limit_failures"] = failures
                delay = max(
                    float(values.get("retry-after", 0)),
                    min(3600, 60 * 2 ** min(failures - 1, 6)),
                )
                state["blocked_until"] = received + delay + 1
                if state.get("remaining") == 0:
                    state["blocked_until"] = max(
                        state["blocked_until"], state.get("reset", received) + 1
                    )
                else:
                    # Secondary limits may span installations. Stop the whole
                    # app rather than continuing with another token.
                    pacing["blocked_until"] = state["blocked_until"]
                    self._save(pacing, self._global_path)
            elif status < 400:
                state["limit_failures"] = 0
            self._save(state)
            if limited:
                raise RateLimitDeferred(state["blocked_until"])
            return response
