import os
import dotenv
import logging

dotenv.load_dotenv()

GITHUB_WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET")
GITHUB_PRIVATE_KEY = os.environ.get("GITHUB_PRIVATE_KEY")
GITHUB_APP_ID = int(os.environ.get("GITHUB_APP_ID"))

OVERRIDE_LOGGING = logging.getLevelName(os.environ.get("OVERRIDE_LOGGING", "WARNING"))

REPO_ALLOWLIST = os.environ.get("REPO_ALLOWLIST")
if REPO_ALLOWLIST is not None:
    REPO_ALLOWLIST = REPO_ALLOWLIST.split(",")


TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

OVERRIDE_CONFIG = os.environ.get("OVERRIDE_CONFIG")

PROCESS_START_PAUSE = float(os.environ.get("PROCESS_START_PAUSE", 5))

CHECK_RUN_NAME_FILTER = os.environ.get("CHECK_RUN_NAME_FILTER")

DISKCACHE_DIR = os.environ["DISKCACHE_DIR"]

PR_TIMEOUT = float(os.environ.get("PR_TIMEOUT", 10))

WORKER_SLEEP = float(os.environ.get("WORKER_SLEEP", 1))

ACCESS_TOKEN_TTL = float(os.environ.get("ACCESS_TOKEN_TTL", 300))

PRS_TTL = float(os.environ.get("PRS_TTL", 60))

DRY_RUN = os.environ.get("DRY_RUN", "false") == "true"

PUSH_GATEWAY = os.environ.get("PUSH_GATEWAY")
