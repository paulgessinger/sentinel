import os
import dotenv
import logging

from sentinel.github.model import Repository

dotenv.load_dotenv()

GITHUB_WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET")
GITHUB_PRIVATE_KEY = os.environ.get("GITHUB_PRIVATE_KEY")
GITHUB_APP_ID = int(os.environ.get("GITHUB_APP_ID"))

OVERRIDE_LOGGING = logging.getLevelName(os.environ.get("OVERRIDE_LOGGING", "WARNING"))

REPO_ALLOWLIST = os.environ.get("REPO_ALLOWLIST")
if REPO_ALLOWLIST is not None:
    REPO_ALLOWLIST = REPO_ALLOWLIST.split(",")


MAX_PR_FREQUENCY = float(os.environ.get("MAX_PR_FREQUENCY", 1 / 10.0))

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

OVERRIDE_CONFIG = os.environ.get("OVERRIDE_CONFIG")

PROCESS_START_PAUSE = float(os.environ.get("PROCESS_START_PAUSE", 5))

CHECK_RUN_NAME_FILTER = os.environ.get("CHECK_RUN_NAME_FILTER")
