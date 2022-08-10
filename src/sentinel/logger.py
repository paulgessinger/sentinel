import logging

import notifiers.logging

from sentinel import config


def get_log_handlers(logger):
    if config.TELEGRAM_TOKEN is None:
        return []
    handler = notifiers.logging.NotificationHandler(
        "telegram",
        defaults={
            "token": config.TELEGRAM_TOKEN,
            "chat_id": config.TELEGRAM_CHAT_ID,
        },
    )
    handler.setLevel(logging.WARNING)
    logger.addHandler(handler)
    return [handler]
