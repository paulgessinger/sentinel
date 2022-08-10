import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import diskcache
import logging

from sentinel import config
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

    async def in_queue(self, pr: PullRequest) -> bool:
        async with self.lock:
            return pr.id in self.get(f"{self.pr_key}", set())

    async def push_pr(self, item: QueueItem) -> None:
        if await self.in_queue(item.pr):
            logger.info("%s already in queue, skipping", item.pr)
            return
        async with self.lock:
            prs = self.get(self.pr_key, set())
            prs.add(item.pr.id)
            self.set(self.pr_key, prs)
            self.set(
                f"{self.pr_cooldown_key}_{item.pr.id}",
                datetime.now(),
                expire=config.PR_TIMEOUT * 5,
            )
            # self.push(item, prefix=self.queue_key)
            self.deque.append(item)
            logger.info("Pushing %s", item.pr)

    async def pull_pr(self) -> Optional[QueueItem]:
        async with self.lock:
            logger.debug("Queue size is %d", len(self.deque))
            if len(self.deque) == 0:
                logger.debug("Empty queue")
                return None

            # value = self.deque.popleft()

            # if value is None:
            #     return None

            # find first element that is not in cooldown
            for _ in range(len(self.deque)):
                candidate = self.deque.popleft()
                if last_dt := self.get(f"{self.pr_cooldown_key}_{candidate.pr.id}"):
                    delta = datetime.now() - last_dt
                    cooldown = timedelta(seconds=config.PR_TIMEOUT)
                    if delta < cooldown:
                        logger.debug(
                            "%s is in cooldown (%s), putting back onto queue",
                            candidate.pr,
                            cooldown - delta,
                        )
                        self.deque.append(candidate)
                    else:
                        logger.debug("%s is good, returning", candidate.pr)
                        # good, remove from set, return
                        prs = self.get(self.pr_key, set())
                        prs.remove(candidate.pr.id)
                        self.set(self.pr_key, prs)

                        return candidate

            return None


def get_cache():
    logger.info("Opening cache dir: %s", config.DISKCACHE_DIR)
    return Cache(config.DISKCACHE_DIR)
