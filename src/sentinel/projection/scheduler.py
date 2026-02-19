from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Awaitable, Callable, Dict

from sentinel.metric import sentinel_projection_debounce_total
from sentinel.projection.types import ProjectionTrigger


@dataclass
class _KeyState:
    trigger: ProjectionTrigger
    dirty: bool = False
    debounce_until: float = 0.0
    pull_request_delay_until: float = 0.0
    pre_delay_pending_emitted: bool = False


class ProjectionStatusScheduler:
    _PULL_REQUEST_DELAY_ACTIONS = frozenset({"synchronize", "opened", "reopened"})

    def __init__(
        self,
        *,
        debounce_seconds: float,
        pull_request_synchronize_delay_seconds: float = 0.0,
        handler: Callable[[ProjectionTrigger], Awaitable[None]],
    ):
        self.debounce_seconds = max(0.0, float(debounce_seconds))
        self.pull_request_synchronize_delay_seconds = max(
            0.0, float(pull_request_synchronize_delay_seconds)
        )
        self.handler = handler
        self._tasks: Dict[str, asyncio.Task] = {}
        self._states: Dict[str, _KeyState] = {}
        self._lock = asyncio.Lock()

    async def enqueue(self, trigger: ProjectionTrigger) -> None:
        key = trigger.key
        now = asyncio.get_running_loop().time()
        debounce_until = now + self.debounce_seconds
        pull_request_delay_until = 0.0
        if self._should_apply_pull_request_delay(trigger):
            pull_request_delay_until = now + self.pull_request_synchronize_delay_seconds
        async with self._lock:
            if key in self._tasks:
                state = self._states[key]
                state.trigger = trigger
                state.dirty = True
                state.debounce_until = max(state.debounce_until, debounce_until)
                previous_delay_until = state.pull_request_delay_until
                state.pull_request_delay_until = max(
                    state.pull_request_delay_until,
                    pull_request_delay_until,
                )
                if state.pull_request_delay_until > previous_delay_until:
                    state.pre_delay_pending_emitted = False
                sentinel_projection_debounce_total.labels(result="coalesced").inc()
                return

            self._states[key] = _KeyState(
                trigger=trigger,
                dirty=False,
                debounce_until=debounce_until,
                pull_request_delay_until=pull_request_delay_until,
                pre_delay_pending_emitted=False,
            )
            sentinel_projection_debounce_total.labels(result="scheduled").inc()
            self._tasks[key] = asyncio.create_task(self._run_key(key))

    async def shutdown(self) -> None:
        async with self._lock:
            tasks = list(self._tasks.values())
            self._tasks.clear()
            self._states.clear()

        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_key(self, key: str) -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                emit_pre_delay_pending = False
                pre_delay_trigger: ProjectionTrigger | None = None
                async with self._lock:
                    state = self._states.get(key)
                    if state is None:
                        return
                    now = loop.time()
                    if (
                        not state.pre_delay_pending_emitted
                        and state.pull_request_delay_until > now
                    ):
                        state.pre_delay_pending_emitted = True
                        emit_pre_delay_pending = True
                        pre_delay_trigger = replace(
                            state.trigger, pre_delay_pending=True
                        )
                    execute_after = max(
                        state.debounce_until,
                        state.pull_request_delay_until,
                    )
                if emit_pre_delay_pending and pre_delay_trigger is not None:
                    await self.handler(pre_delay_trigger)
                    continue
                sleep_for = execute_after - loop.time()
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
                    continue

                async with self._lock:
                    state = self._states.get(key)
                    if state is None:
                        return
                    trigger = state.trigger
                    state.dirty = False

                sentinel_projection_debounce_total.labels(result="executed").inc()
                await self.handler(trigger)

                async with self._lock:
                    latest = self._states.get(key)
                    if latest is None:
                        return
                    if latest.dirty:
                        continue
                    self._states.pop(key, None)
                    self._tasks.pop(key, None)
                    return
        except asyncio.CancelledError:
            raise
        finally:
            async with self._lock:
                self._states.pop(key, None)
                self._tasks.pop(key, None)

    def _should_apply_pull_request_delay(self, trigger: ProjectionTrigger) -> bool:
        if (
            trigger.event == "pull_request"
            and trigger.action in self._PULL_REQUEST_DELAY_ACTIONS
            and self.pull_request_synchronize_delay_seconds > 0
        ):
            return True
        return False
