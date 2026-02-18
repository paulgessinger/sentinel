from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict

from sentinel.metric import sentinel_projection_debounce_total
from sentinel.projection.types import ProjectionTrigger


@dataclass
class _KeyState:
    trigger: ProjectionTrigger
    dirty: bool = False


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
        async with self._lock:
            if key in self._tasks:
                state = self._states[key]
                state.trigger = trigger
                state.dirty = True
                sentinel_projection_debounce_total.labels(result="coalesced").inc()
                return

            self._states[key] = _KeyState(trigger=trigger, dirty=False)
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
        try:
            while True:
                if self.debounce_seconds > 0:
                    await asyncio.sleep(self.debounce_seconds)

                async with self._lock:
                    state = self._states.get(key)
                    if state is None:
                        return
                    trigger = state.trigger

                extra_delay = self._extra_delay_seconds(trigger)
                if extra_delay > 0:
                    await asyncio.sleep(extra_delay)

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

    def _extra_delay_seconds(self, trigger: ProjectionTrigger) -> float:
        if (
            trigger.event == "pull_request"
            and trigger.action in self._PULL_REQUEST_DELAY_ACTIONS
            and self.pull_request_synchronize_delay_seconds > 0
        ):
            return self.pull_request_synchronize_delay_seconds
        return 0.0
