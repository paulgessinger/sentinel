import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Set
import diskcache
import logging

from sentinel.config import SETTINGS
from sentinel.github.model import PullRequest

logger = logging.getLogger("sentinel")


@dataclass
class QueueItem:
    pr: PullRequest
    installation_id: int


class Cache(diskcache.Cache):
    lock: asyncio.Lock
    # queue_key: str = "pr_queue"
    pr_key: str = "prs"
    pr_cooldown_key: str = "pr_cooldown"

    deque: diskcache.Deque

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lock = asyncio.Lock()
        self.deque = diskcache.Deque(directory=self.directory + "/dequeue")

    def in_queue(self, pr: PullRequest) -> bool:
        return pr.id in self.get(f"{self.pr_key}", set())

    async def push_pr(self, item: QueueItem) -> bool:
        async with self.lock:
            if self.in_queue(item.pr):
                logger.info("%s already in queue, skipping", item.pr)
                return False

            with self.transact():
                prs = self.get(self.pr_key, set())
                prs.add(item.pr.id)
                self.set(self.pr_key, prs)

            self.set(
                f"{self.pr_cooldown_key}_{item.pr.id}",
                datetime.now(),
                expire=60 * 60,
            )
            # self.push(item, prefix=self.queue_key)
            self.deque.append(item)
            logger.info("Pushing %s", item.pr)
            return True

    async def pull_pr(self) -> QueueItem | None:
        async with self.lock:
            logger.debug("Queue size is %d", len(self.deque))
            if len(self.deque) == 0:
                logger.debug("Empty queue")
                return None

            cooldown = timedelta(seconds=SETTINGS.PR_TIMEOUT)

            # find first element that is not in cooldown
            for _ in range(len(self.deque)):
                candidate = self.deque.popleft()
                last_dt = self.get(f"{self.pr_cooldown_key}_{candidate.pr.id}")

                if last_dt is not None and datetime.now() - last_dt < cooldown:
                    logger.debug(
                        "%s is in cooldown (%s), putting back onto queue",
                        candidate.pr,
                        cooldown - (datetime.now() - last_dt),
                    )
                    self.deque.append(candidate)
                else:
                    logger.debug("%s is good, returning", candidate.pr)
                    # good, remove from set, return
                    with self.transact():
                        prs: Set[int] = self.get(self.pr_key, set())
                        if candidate.pr.id in prs:
                            prs.remove(candidate.pr.id)
                        self.set(self.pr_key, prs)

                    return candidate

            return None


def get_cache():
    # logger.info("Opening cache dir: %s", SETTINGS.DISKCACHE_DIR)
    return Cache(SETTINGS.DISKCACHE_DIR)
