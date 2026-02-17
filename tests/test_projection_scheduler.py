import asyncio

import pytest

from sentinel.projection import ProjectionStatusScheduler, ProjectionTrigger


@pytest.mark.asyncio
async def test_scheduler_coalesces_multiple_triggers_for_same_key():
    seen = []

    async def handler(trigger: ProjectionTrigger):
        seen.append(trigger.delivery_id)

    scheduler = ProjectionStatusScheduler(debounce_seconds=0.02, handler=handler)

    trigger = ProjectionTrigger(
        repo_id=10,
        repo_full_name="org/repo",
        head_sha="a" * 40,
        installation_id=111,
        delivery_id="d1",
        event="check_run",
    )

    await scheduler.enqueue(trigger)
    await scheduler.enqueue(
        ProjectionTrigger(
            repo_id=10,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=111,
            delivery_id="d2",
            event="status",
        )
    )
    await scheduler.enqueue(
        ProjectionTrigger(
            repo_id=10,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=111,
            delivery_id="d3",
            event="pull_request",
        )
    )

    await asyncio.sleep(0.08)
    await scheduler.shutdown()

    assert seen == ["d3"]
