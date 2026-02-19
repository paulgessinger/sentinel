import logging

import notifiers.logging

from sentinel.config import SETTINGS


def get_log_handlers(logger):
    if SETTINGS.TELEGRAM_TOKEN is None:
        return []
    handler = notifiers.logging.NotificationHandler(
        "telegram",
        defaults={
            "token": SETTINGS.TELEGRAM_TOKEN,
            "chat_id": SETTINGS.TELEGRAM_CHAT_ID,
        },
    )
    handler.setLevel(logging.WARNING)
    logger.addHandler(handler)
    return [handler]
