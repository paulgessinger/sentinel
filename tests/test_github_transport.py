import asyncio
import json
from types import SimpleNamespace

import pytest

from sentinel.github import transport


@pytest.fixture
def clock(monkeypatch):
    clock = SimpleNamespace(now=1000.0)

    async def sleep(seconds):
        clock.now += seconds
        await asyncio.sleep(0)

    monkeypatch.setattr(transport, "time", SimpleNamespace(time=lambda: clock.now))
    monkeypatch.setattr(transport, "asyncio", SimpleNamespace(sleep=sleep))
    return clock


def client(tmp_path, installation=1):
    return transport.GitHubAPI(
        None, "test", installation_id=installation, rate_directory=tmp_path
    )


@pytest.mark.asyncio
async def test_headroom_shared_by_new_clients_and_not_other_installations(
    tmp_path, monkeypatch, clock
):
    calls = []

    async def request(*args):
        calls.append(args)
        return 200, {"x-ratelimit-remaining": "100", "x-ratelimit-reset": "4600"}, b"{}"

    monkeypatch.setattr(transport.BaseGitHubAPI, "_request", request)
    await client(tmp_path)._request("GET", "/test", {})
    with pytest.raises(transport.RateLimitDeferred) as exc:
        await client(tmp_path)._request("GET", "/test", {})
    assert exc.value.retry_at == 4601
    assert len(calls) == 1

    await client(tmp_path, installation=2)._request("GET", "/test", {})
    assert len(calls) == 2
    clock.now = 4602
    await client(tmp_path)._request("GET", "/test", {})
    assert len(calls) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,headers,body,deadline",
    [
        (429, {"retry-after": "120"}, b"{}", 1121),
        (403, {}, b'{"message":"secondary rate limit"}', 1061),
        (403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "4600"}, b"{}", 4601),
    ],
)
async def test_rate_limit_stops_other_clients(
    tmp_path, monkeypatch, clock, status, headers, body, deadline
):
    calls = []

    async def request(*args):
        calls.append(args)
        return status, headers, body

    monkeypatch.setattr(transport.BaseGitHubAPI, "_request", request)
    for _ in range(2):
        with pytest.raises(transport.RateLimitDeferred) as exc:
            await client(tmp_path)._request("GET", "/test", {})
        assert exc.value.retry_at == deadline
    assert len(calls) == 1
    if status == 429 or b"secondary" in body:
        with pytest.raises(transport.RateLimitDeferred):
            await client(tmp_path, installation=2)._request("GET", "/test", {})
        assert len(calls) == 1


@pytest.mark.asyncio
async def test_secondary_limit_exponential_backoff(tmp_path, monkeypatch, clock):
    async def request(*args):
        return 429, {}, b"{}"

    monkeypatch.setattr(transport.BaseGitHubAPI, "_request", request)
    with pytest.raises(transport.RateLimitDeferred):
        await client(tmp_path)._request("GET", "/test", {})
    clock.now = 1062
    with pytest.raises(transport.RateLimitDeferred) as exc:
        await client(tmp_path)._request("GET", "/test", {})
    assert exc.value.retry_at == 1183


@pytest.mark.asyncio
async def test_pagination_uses_guard_before_each_page(tmp_path, monkeypatch, clock):
    calls = []

    async def request(*args):
        calls.append(args)
        return (
            200,
            {
                "content-type": "application/json",
                "x-ratelimit-limit": "5000",
                "x-ratelimit-remaining": "100",
                "x-ratelimit-reset": "4600",
                "link": '<https://api.github.com/items?page=2>; rel="next"',
            },
            b'[{"id":1}]',
        )

    monkeypatch.setattr(transport.BaseGitHubAPI, "_request", request)
    items = []
    with pytest.raises(transport.RateLimitDeferred):
        async for item in client(tmp_path).getiter("/items"):
            items.append(item)
    assert items == [{"id": 1}]
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_serializes_clients_and_spaces_writes(tmp_path, monkeypatch, clock):
    calls = []
    active = 0

    async def request(*args):
        nonlocal active
        active += 1
        assert active == 1
        calls.append(clock.now)
        await asyncio.sleep(0)
        active -= 1
        return 200, {}, b"{}"

    monkeypatch.setattr(transport.BaseGitHubAPI, "_request", request)
    await asyncio.gather(
        *(client(tmp_path)._request("POST", "/test", {}) for _ in range(3))
    )
    assert len(calls) == 3
    assert all(b - a >= 8 for a, b in zip(calls, calls[1:]))


@pytest.mark.asyncio
async def test_ordinary_forbidden_does_not_set_limit_backoff(
    tmp_path, monkeypatch, clock
):
    async def request(*args):
        return 403, {}, b'{"message":"Resource not accessible by integration"}'

    monkeypatch.setattr(transport.BaseGitHubAPI, "_request", request)
    gh = client(tmp_path)
    response = await gh._request("GET", "/test", {})
    assert response[0] == 403
    assert "blocked_until" not in json.loads(gh._state_path.read_text())
