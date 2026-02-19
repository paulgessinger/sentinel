from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from fnmatch import fnmatch
import hashlib
import io
import json
import time
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Protocol

from sanic.log import logger
import yaml

from sentinel.github import determine_rules
from sentinel.github.api import API
from sentinel.github.model import (
    CheckRun,
    CheckRunAction,
    CheckRunOutput,
)
from sentinel.metric import (
    sentinel_projection_eval_total,
    sentinel_projection_fallback_total,
    sentinel_projection_lookup_total,
    sentinel_projection_publish_total,
)
from sentinel.model import Config
from sentinel.projection.types import EvaluationResult, ProjectionTrigger
from sentinel.storage import WebhookStore
from sentinel.storage.types import (
    CheckRunRow,
    CommitStatusRow,
    PullRequestHeadRow,
    WorkflowRunRow,
)
from sentinel.storage.webhook_store import utcnow_iso


@dataclass(frozen=True)
class _ResultItem:
    name: str
    status: str
    required: bool = False


@dataclass(frozen=True)
class _RuleBaseRef:
    ref: str


@dataclass(frozen=True)
class _RulePullRequestLike:
    base: _RuleBaseRef


@dataclass(frozen=True)
class _ComputedEvaluation:
    check_by_name: Dict[str, CheckRunRow]
    result_items: Dict[str, _ResultItem]
    status_by_name: Dict[str, CommitStatusRow]
    failures: list[_ResultItem]
    explicit_failures: list[_ResultItem]
    in_progress: list[_ResultItem]
    required_successes: list[_ResultItem]
    missing_pattern_failures: list[str]


class ProjectionEvaluatorConfig(Protocol):
    GITHUB_APP_ID: int
    PROJECTION_CHECK_RUN_NAME: str
    PROJECTION_PUBLISH_ENABLED: bool
    PROJECTION_MANUAL_REFRESH_ACTION_ENABLED: bool
    PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER: str
    PROJECTION_MANUAL_REFRESH_ACTION_LABEL: str
    PROJECTION_MANUAL_REFRESH_ACTION_DESCRIPTION: str
    PROJECTION_PATH_RULE_FALLBACK_ENABLED: bool
    PROJECTION_AUTO_REFRESH_ON_MISSING_ENABLED: bool
    PROJECTION_AUTO_REFRESH_ON_MISSING_STALE_SECONDS: int
    PROJECTION_AUTO_REFRESH_ON_MISSING_COOLDOWN_SECONDS: int
    PROJECTION_CONFIG_CACHE_SECONDS: int
    PROJECTION_PR_FILES_CACHE_SECONDS: int


class ProjectionEvaluator:
    def __init__(
        self,
        *,
        store: WebhookStore,
        config: ProjectionEvaluatorConfig,
        api_factory: Callable[[int], Awaitable[API]],
    ):
        self.store = store
        self.config = config
        self.api_factory = api_factory
        self._config_cache: Dict[int, tuple[float, Config | None]] = {}
        self._pr_files_cache: Dict[tuple[int, int, str], tuple[float, List[str]]] = {}
        self._auto_refresh_missing_cooldown: Dict[str, float] = {}

    async def evaluate_and_publish(
        self, trigger: ProjectionTrigger
    ) -> EvaluationResult:
        started = time.monotonic()
        logger.info(
            "Projection eval start repo=%s sha=%s event=%s delivery=%s",
            trigger.repo_full_name,
            trigger.head_sha,
            trigger.event,
            trigger.delivery_id,
        )
        try:
            result = await self._evaluate_and_publish(trigger)
            logger.info(
                "Projection eval done repo=%s sha=%s result=%s changed=%s check_run_id=%s duration_ms=%.1f",
                trigger.repo_full_name,
                trigger.head_sha,
                result.result,
                result.changed,
                result.check_run_id,
                (time.monotonic() - started) * 1000.0,
            )
            return result
        except Exception as exc:  # noqa: BLE001
            sentinel_projection_eval_total.labels(result="error").inc()
            logger.error(
                "Projection evaluation failed repo=%s repo_id=%s sha=%s delivery=%s",
                trigger.repo_full_name,
                trigger.repo_id,
                trigger.head_sha,
                trigger.delivery_id,
                exc_info=True,
            )
            return EvaluationResult(result="error", error=str(exc))

    async def _evaluate_and_publish(
        self, trigger: ProjectionTrigger
    ) -> EvaluationResult:
        repo_id = trigger.repo_id
        head_sha = trigger.head_sha

        open_prs = self.store.get_open_pr_candidates(repo_id, head_sha)
        if not open_prs:
            logger.info(
                "Projection eval skip repo=%s sha=%s reason=no_open_pr",
                trigger.repo_full_name,
                head_sha,
            )
            sentinel_projection_eval_total.labels(result="no_pr").inc()
            return EvaluationResult(result="no_pr")

        if len(open_prs) > 1:
            logger.info(
                "Projection eval ambiguous repo=%s sha=%s open_prs=%d using_pr=%s",
                trigger.repo_full_name,
                head_sha,
                len(open_prs),
                open_prs[0].pr_number,
            )
            sentinel_projection_eval_total.labels(result="ambiguous_pr").inc()
        pr_row = open_prs[0]
        if pr_row.pr_number is None:
            raise ValueError("Projected PR row is missing pr_number")
        pr_number = int(pr_row.pr_number)
        if len(open_prs) > 1:
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="pr_selection",
                result="ambiguous",
                detail="Multiple open PRs matched this head SHA",
                metadata={"count": len(open_prs)},
            )

        api = await self.api_factory(trigger.installation_id)
        repo_url = f"/repos/{trigger.repo_full_name}"
        config = await self._get_repo_config(api, trigger, pr_number=pr_number)
        if config is None:
            logger.info(
                "Projection eval skip repo=%s sha=%s pr=%s reason=no_config",
                trigger.repo_full_name,
                head_sha,
                pr_number,
            )
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="config_fetch",
                result="no_config",
                detail="No .merge-sentinel.yml found",
            )
            sentinel_projection_eval_total.labels(result="no_config").inc()
            return EvaluationResult(result="no_config")

        changed_files: List[str] = []
        has_path_rules = any(
            rule.paths is not None or rule.paths_ignore is not None
            for rule in config.rules
        )
        logger.debug(
            "Projection eval inputs repo=%s sha=%s pr=%s rules=%d has_path_rules=%s",
            trigger.repo_full_name,
            head_sha,
            pr_number,
            len(config.rules),
            has_path_rules,
        )
        if has_path_rules and self.config.PROJECTION_PATH_RULE_FALLBACK_ENABLED:
            changed_files = await self._get_pr_files(
                api=api,
                trigger=trigger,
                pr_number=pr_number,
            )
            logger.debug(
                "Projection eval path fallback repo=%s sha=%s pr=%s changed_files=%d",
                trigger.repo_full_name,
                head_sha,
                pr_number,
                len(changed_files),
            )

        pr_like = _RulePullRequestLike(base=_RuleBaseRef(ref=pr_row.base_ref or ""))
        rules = determine_rules(changed_files, pr_like, config.rules)
        logger.debug(
            "Projection eval selected rules repo=%s sha=%s pr=%s selected=%d",
            trigger.repo_full_name,
            head_sha,
            pr_number,
            len(rules),
        )

        if trigger.force_api_refresh:
            logger.info(
                "Projection eval forcing API refresh repo=%s sha=%s",
                trigger.repo_full_name,
                head_sha,
            )
            (
                check_rows,
                workflow_rows,
                status_rows,
            ) = await self._load_head_rows_from_api(
                api=api,
                trigger=trigger,
                pr_number=pr_number,
            )
        else:
            check_rows = self.store.get_check_runs_for_head(repo_id, head_sha)
            workflow_rows = self.store.get_workflow_runs_for_head(repo_id, head_sha)
            status_rows = self.store.get_commit_statuses_for_sha(repo_id, head_sha)
        logger.debug(
            "Projection eval projections repo=%s sha=%s checks=%d workflows=%d statuses=%d",
            trigger.repo_full_name,
            head_sha,
            len(check_rows),
            len(workflow_rows),
            len(status_rows),
        )

        computed = self._evaluate_rows(
            check_rows=check_rows,
            workflow_rows=workflow_rows,
            status_rows=status_rows,
            rules=rules,
        )
        auto_refresh_kind: str | None = None
        if self._should_auto_refresh_on_missing(
            trigger=trigger,
            pr_row=pr_row,
            pr_number=pr_number,
            missing_pattern_failures=computed.missing_pattern_failures,
            explicit_failures=computed.explicit_failures,
        ):
            auto_refresh_kind = "missing_refresh"
        elif self._should_auto_refresh_on_stale_running(
            trigger=trigger,
            pr_number=pr_number,
            computed=computed,
        ):
            auto_refresh_kind = "stale_running_refresh"

        if auto_refresh_kind is not None:
            logger.info(
                "Projection eval auto-refreshing from API repo=%s sha=%s kind=%s missing_patterns=%d",
                trigger.repo_full_name,
                head_sha,
                auto_refresh_kind,
                len(computed.missing_pattern_failures),
            )
            sentinel_projection_fallback_total.labels(
                kind=auto_refresh_kind, result="triggered"
            ).inc()
            (
                check_rows,
                workflow_rows,
                status_rows,
            ) = await self._load_head_rows_from_api(
                api=api,
                trigger=trigger,
                pr_number=pr_number,
            )
            computed = self._evaluate_rows(
                check_rows=check_rows,
                workflow_rows=workflow_rows,
                status_rows=status_rows,
                rules=rules,
            )

        failures = computed.failures
        in_progress = computed.in_progress
        required_successes = computed.required_successes
        logger.debug(
            "Projection eval aggregate repo=%s sha=%s items=%d failures=%d required_pending=%d required_success=%d",
            trigger.repo_full_name,
            head_sha,
            len(computed.result_items),
            len(failures),
            len(in_progress),
            len(required_successes),
        )

        if failures:
            new_status = "completed"
            new_conclusion = "failure"
            title = (
                "1 job has failed"
                if len(failures) == 1
                else f"{len(failures)} jobs have failed"
            )
        elif in_progress:
            new_status = "in_progress"
            new_conclusion = None
            title = (
                "Waiting for 1 job"
                if len(in_progress) == 1
                else f"Waiting for {len(in_progress)} jobs"
            )
            if len(in_progress) <= 3:
                title += ": " + ", ".join(sorted(item.name for item in in_progress))
        else:
            new_status = "completed"
            new_conclusion = "success"
            title = f"All {len(required_successes)} required jobs successful"

        summary_parts: List[str] = []
        if failures:
            summary_parts.append(
                ":x: failed: " + ", ".join(sorted(item.name for item in failures))
            )
        if in_progress:
            summary_parts.append(
                ":yellow_circle: waiting for: "
                + ", ".join(sorted(item.name for item in in_progress))
            )
        if required_successes:
            summary_parts.append(
                ":white_check_mark: successful required checks: "
                + ", ".join(sorted(item.name for item in required_successes))
            )

        text_lines = [
            "# Checks",
            "",
            "| Check | Status | Required? |",
            "| --- | --- | --- |",
        ]
        for item in sorted(computed.result_items.values(), key=lambda item: item.name):
            text_lines.append(
                f"| {item.name} | {item.status} | {'yes' if item.required else 'no'} |"
            )
        output_text = "\n".join(text_lines)
        output_summary = "\n".join(summary_parts)

        summary_hash = hashlib.sha256(output_summary.encode("utf-8")).hexdigest()
        text_hash = hashlib.sha256(output_text.encode("utf-8")).hexdigest()
        output_checks_json = self._build_output_checks_json(computed)

        sentinel_row = self.store.get_sentinel_check_run(
            repo_id=repo_id,
            head_sha=head_sha,
            check_name=self.config.PROJECTION_CHECK_RUN_NAME,
            app_id=self.config.GITHUB_APP_ID,
        )

        if not self.store.get_open_pr_candidates(repo_id, head_sha):
            logger.info(
                "Projection eval skip repo=%s sha=%s reason=no_open_pr_before_publish",
                trigger.repo_full_name,
                head_sha,
            )
            sentinel_projection_eval_total.labels(result="no_pr").inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="publish",
                result="skip_no_open_pr",
                detail="PR closed before publish",
            )
            return EvaluationResult(result="no_pr")

        previous_status = sentinel_row.status if sentinel_row else None
        existing_id = sentinel_row.check_run_id if sentinel_row else None
        check_run_id = (
            existing_id if isinstance(existing_id, int) and existing_id > 0 else None
        )

        if new_status == "in_progress" and previous_status != "in_progress":
            check_run_id = None

        unchanged = bool(
            sentinel_row
            and sentinel_row.status == new_status
            and sentinel_row.conclusion == new_conclusion
            and sentinel_row.output_title == title
            and sentinel_row.output_summary_hash == summary_hash
            and sentinel_row.output_text_hash == text_hash
        )
        logger.debug(
            "Projection eval diff repo=%s sha=%s unchanged=%s prev_status=%s new_status=%s prev_conclusion=%s new_conclusion=%s",
            trigger.repo_full_name,
            head_sha,
            unchanged,
            sentinel_row.status if sentinel_row else None,
            new_status,
            sentinel_row.conclusion if sentinel_row else None,
            new_conclusion,
        )

        now_iso = utcnow_iso()
        started_at_dt = self._min_started_at(computed.check_by_name.values())
        completed_at_dt = (
            self._max_completed_at(computed.check_by_name.values())
            if new_status == "completed"
            else None
        )
        started_at = self._dt_to_iso(started_at_dt)
        completed_at = self._dt_to_iso(completed_at_dt)

        if unchanged:
            logger.info(
                "Projection eval unchanged repo=%s sha=%s pr=%s check_run_id=%s",
                trigger.repo_full_name,
                head_sha,
                pr_number,
                check_run_id,
            )
            sentinel_projection_publish_total.labels(result="unchanged").inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="publish",
                result="unchanged",
                detail=f"{new_status}/{new_conclusion or '-'}",
            )
            self.store.upsert_sentinel_check_run(
                repo_id=repo_id,
                repo_full_name=trigger.repo_full_name,
                check_run_id=check_run_id,
                head_sha=head_sha,
                name=self.config.PROJECTION_CHECK_RUN_NAME,
                status=new_status,
                conclusion=new_conclusion,
                app_id=self.config.GITHUB_APP_ID,
                started_at=started_at,
                completed_at=completed_at,
                output_title=title,
                output_summary=output_summary,
                output_text=output_text,
                output_checks_json=output_checks_json,
                output_summary_hash=summary_hash,
                output_text_hash=text_hash,
                last_eval_at=now_iso,
                last_publish_at=(
                    self._dt_to_iso(sentinel_row.last_publish_at)
                    if sentinel_row
                    else None
                ),
                last_publish_result="unchanged",
                last_publish_error=None,
                last_delivery_id=trigger.delivery_id,
            )
            sentinel_projection_eval_total.labels(result="unchanged").inc()
            return EvaluationResult(
                result="unchanged", check_run_id=check_run_id, changed=False
            )

        published_id: int | None = None
        publish_result = "dry_run"
        publish_error: str | None = None
        publish_at: str | None = None

        if self.config.PROJECTION_PUBLISH_ENABLED and bool(pr_row.pr_draft):
            logger.info(
                "Projection publish skipped (draft_pr) repo=%s sha=%s pr=%s status=%s conclusion=%s",
                trigger.repo_full_name,
                head_sha,
                pr_number,
                new_status,
                new_conclusion,
            )
            publish_result = "skipped_draft"
            sentinel_projection_publish_total.labels(result="skipped_draft").inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="publish",
                result="skipped_draft",
                detail=f"Draft PR, would publish {new_status}/{new_conclusion or '-'}",
            )
        elif self.config.PROJECTION_PUBLISH_ENABLED:
            logger.info(
                "Projection publish attempt repo=%s sha=%s pr=%s current_id=%s status=%s conclusion=%s",
                trigger.repo_full_name,
                head_sha,
                pr_number,
                check_run_id,
                new_status,
                new_conclusion,
            )
            check_run = CheckRun.make_app_check_run(
                id=check_run_id,
                head_sha=head_sha,
                status=new_status,
                conclusion=new_conclusion,
                started_at=started_at_dt or datetime.now(timezone.utc),
                completed_at=completed_at_dt,
                output=CheckRunOutput(
                    title=title,
                    summary=output_summary,
                    text=output_text,
                ),
                actions=self._build_manual_refresh_actions(new_status),
            )

            if check_run.id is None:
                sentinel_projection_lookup_total.labels(result="local_id_miss").inc()
                self._record_activity(
                    trigger=trigger,
                    pr_number=pr_number,
                    activity_type="publish_lookup",
                    result="local_id_miss",
                    detail="No local check_run_id; lookup on GitHub",
                )
                logger.debug(
                    "Projection lookup local miss repo=%s sha=%s",
                    trigger.repo_full_name,
                    head_sha,
                )
                existing = await api.find_existing_sentinel_check_run(
                    repo_url=repo_url,
                    head_sha=head_sha,
                    check_name=self.config.PROJECTION_CHECK_RUN_NAME,
                    app_id=self.config.GITHUB_APP_ID,
                )
                if existing is not None and existing.id is not None:
                    sentinel_projection_lookup_total.labels(
                        result="gh_lookup_hit"
                    ).inc()
                    check_run.id = int(existing.id)
                    self._record_activity(
                        trigger=trigger,
                        pr_number=pr_number,
                        activity_type="publish_lookup",
                        result="gh_lookup_hit",
                        detail=f"Found existing check_run_id={check_run.id}",
                    )
                    logger.info(
                        "Projection lookup github hit repo=%s sha=%s found_id=%s",
                        trigger.repo_full_name,
                        head_sha,
                        check_run.id,
                    )
                else:
                    sentinel_projection_lookup_total.labels(
                        result="gh_lookup_miss"
                    ).inc()
                    self._record_activity(
                        trigger=trigger,
                        pr_number=pr_number,
                        activity_type="publish_lookup",
                        result="gh_lookup_miss",
                        detail="No existing check run found on GitHub",
                    )
                    logger.info(
                        "Projection lookup github miss repo=%s sha=%s",
                        trigger.repo_full_name,
                        head_sha,
                    )
            else:
                sentinel_projection_lookup_total.labels(result="local_id_hit").inc()
                self._record_activity(
                    trigger=trigger,
                    pr_number=pr_number,
                    activity_type="publish_lookup",
                    result="local_id_hit",
                    detail=f"Using local check_run_id={check_run.id}",
                )
                logger.debug(
                    "Projection lookup local hit repo=%s sha=%s id=%s",
                    trigger.repo_full_name,
                    head_sha,
                    check_run.id,
                )

            try:
                posted_id = await api.post_check_run(repo_url, check_run)
                published_id = posted_id or check_run.id
                publish_result = "published"
                publish_at = now_iso
                sentinel_projection_publish_total.labels(result="published").inc()
                self._record_activity(
                    trigger=trigger,
                    pr_number=pr_number,
                    activity_type="publish",
                    result="published",
                    detail=f"Published check_run_id={published_id}",
                    metadata={
                        "status": new_status,
                        "conclusion": new_conclusion,
                    },
                )
                logger.info(
                    "Projection publish success repo=%s sha=%s published_id=%s",
                    trigger.repo_full_name,
                    head_sha,
                    published_id,
                )
            except Exception as exc:  # noqa: BLE001
                publish_result = "error"
                publish_error = str(exc)
                sentinel_projection_publish_total.labels(result="error").inc()
                self._record_activity(
                    trigger=trigger,
                    pr_number=pr_number,
                    activity_type="publish",
                    result="error",
                    detail=publish_error,
                )
                logger.error(
                    "Publishing projection check run failed repo=%s sha=%s pr=%s",
                    trigger.repo_full_name,
                    head_sha,
                    pr_number,
                    exc_info=True,
                )
        else:
            logger.info(
                "Projection publish skipped (dry_run) repo=%s sha=%s pr=%s status=%s conclusion=%s",
                trigger.repo_full_name,
                head_sha,
                pr_number,
                new_status,
                new_conclusion,
            )
            sentinel_projection_publish_total.labels(result="dry_run").inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="publish",
                result="dry_run",
                detail=f"Would publish {new_status}/{new_conclusion or '-'}",
            )

        final_id = int(published_id) if published_id is not None else check_run_id
        self.store.upsert_sentinel_check_run(
            repo_id=repo_id,
            repo_full_name=trigger.repo_full_name,
            check_run_id=final_id,
            head_sha=head_sha,
            name=self.config.PROJECTION_CHECK_RUN_NAME,
            status=new_status,
            conclusion=new_conclusion,
            app_id=self.config.GITHUB_APP_ID,
            started_at=started_at,
            completed_at=completed_at,
            output_title=title,
            output_summary=output_summary,
            output_text=output_text,
            output_checks_json=output_checks_json,
            output_summary_hash=summary_hash,
            output_text_hash=text_hash,
            last_eval_at=now_iso,
            last_publish_at=publish_at,
            last_publish_result=publish_result,
            last_publish_error=publish_error,
            last_delivery_id=trigger.delivery_id,
        )

        sentinel_projection_eval_total.labels(result="evaluated").inc()
        logger.debug(
            "Projection eval state persisted repo=%s sha=%s check_run_id=%s publish_result=%s",
            trigger.repo_full_name,
            head_sha,
            final_id,
            publish_result,
        )
        return EvaluationResult(
            result=publish_result,
            check_run_id=final_id,
            changed=True,
            error=publish_error,
        )

    async def _get_repo_config(
        self, api: API, trigger: ProjectionTrigger, *, pr_number: int
    ) -> Config | None:
        now = time.monotonic()
        cached = self._config_cache.get(trigger.repo_id)
        if cached and cached[0] > now:
            sentinel_projection_fallback_total.labels(
                kind="config", result="cache_hit"
            ).inc()
            logger.debug(
                "Projection config cache hit repo=%s repo_id=%s",
                trigger.repo_full_name,
                trigger.repo_id,
            )
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="config_fetch",
                result="cache_hit",
                detail="Using cached .merge-sentinel.yml",
            )
            return cached[1]

        sentinel_projection_fallback_total.labels(
            kind="config", result="cache_miss"
        ).inc()
        logger.debug(
            "Projection config cache miss repo=%s repo_id=%s",
            trigger.repo_full_name,
            trigger.repo_id,
        )
        self._record_activity(
            trigger=trigger,
            pr_number=pr_number,
            activity_type="config_fetch",
            result="cache_miss",
            detail="Fetching .merge-sentinel.yml from GitHub API",
        )
        try:
            content = await api.get_content(
                f"/repos/{trigger.repo_full_name}", ".merge-sentinel.yml"
            )
            decoded = content.decoded_content()
            data = yaml.safe_load(io.StringIO(decoded))
            loaded = Config() if data is None else Config.model_validate(data)
            logger.debug(
                "Projection config loaded repo=%s repo_id=%s rules=%d",
                trigger.repo_full_name,
                trigger.repo_id,
                len(loaded.rules),
            )
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="config_fetch",
                result="loaded",
                detail=f"Loaded config with {len(loaded.rules)} rules",
            )
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "status_code", None) == 404:
                loaded = None
                logger.info(
                    "Projection config missing repo=%s repo_id=%s",
                    trigger.repo_full_name,
                    trigger.repo_id,
                )
                self._record_activity(
                    trigger=trigger,
                    pr_number=pr_number,
                    activity_type="config_fetch",
                    result="missing",
                    detail="Config not found (404)",
                )
            else:
                self._record_activity(
                    trigger=trigger,
                    pr_number=pr_number,
                    activity_type="config_fetch",
                    result="error",
                    detail=str(exc),
                )
                raise

        self._config_cache[trigger.repo_id] = (
            now + self.config.PROJECTION_CONFIG_CACHE_SECONDS,
            loaded,
        )
        return loaded

    async def _get_pr_files(
        self,
        *,
        api: API,
        trigger: ProjectionTrigger,
        pr_number: int,
    ) -> List[str]:
        key = (trigger.repo_id, pr_number, trigger.head_sha)
        now = time.monotonic()
        cached = self._pr_files_cache.get(key)
        if cached and cached[0] > now:
            sentinel_projection_fallback_total.labels(
                kind="pr_files", result="cache_hit"
            ).inc()
            logger.debug(
                "Projection pr_files cache hit repo=%s sha=%s pr=%s count=%d",
                trigger.repo_full_name,
                trigger.head_sha,
                pr_number,
                len(cached[1]),
            )
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="pr_files_fetch",
                result="cache_hit",
                detail=f"Using cached changed files ({len(cached[1])})",
            )
            return list(cached[1])

        sentinel_projection_fallback_total.labels(
            kind="pr_files", result="cache_miss"
        ).inc()
        logger.debug(
            "Projection pr_files cache miss repo=%s sha=%s pr=%s",
            trigger.repo_full_name,
            trigger.head_sha,
            pr_number,
        )
        self._record_activity(
            trigger=trigger,
            pr_number=pr_number,
            activity_type="pr_files_fetch",
            result="cache_miss",
            detail="Fetching PR changed files from GitHub API",
        )
        pull = await api.get_pull(f"/repos/{trigger.repo_full_name}", pr_number)
        files = [f.filename async for f in api.get_pull_request_files(pull)]
        self._pr_files_cache[key] = (
            now + self.config.PROJECTION_PR_FILES_CACHE_SECONDS,
            list(files),
        )
        logger.debug(
            "Projection pr_files fetched repo=%s sha=%s pr=%s count=%d",
            trigger.repo_full_name,
            trigger.head_sha,
            pr_number,
            len(files),
        )
        self._record_activity(
            trigger=trigger,
            pr_number=pr_number,
            activity_type="pr_files_fetch",
            result="fetched",
            detail=f"Fetched changed files ({len(files)})",
        )
        return files

    def _build_manual_refresh_actions(self, status: str) -> List[CheckRunAction]:
        if not self.config.PROJECTION_MANUAL_REFRESH_ACTION_ENABLED:
            return []
        if status != "completed":
            return []
        return [
            CheckRunAction(
                label=self.config.PROJECTION_MANUAL_REFRESH_ACTION_LABEL,
                description=self.config.PROJECTION_MANUAL_REFRESH_ACTION_DESCRIPTION,
                identifier=self.config.PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER,
            )
        ]

    def _evaluate_rows(
        self,
        *,
        check_rows: list[CheckRunRow],
        workflow_rows: list[WorkflowRunRow],
        status_rows: list[CommitStatusRow],
        rules: list[Any],
    ) -> _ComputedEvaluation:
        filtered_check_rows = [
            row
            for row in check_rows
            if not (
                row.app_id == self.config.GITHUB_APP_ID
                and row.name == self.config.PROJECTION_CHECK_RUN_NAME
            )
        ]
        workflow_name_by_suite = {
            row.check_suite_id: row.name
            for row in workflow_rows
            if row.check_suite_id is not None and row.name
        }

        check_by_name: Dict[str, CheckRunRow] = {}
        for row in filtered_check_rows:
            name = row.name
            if not name:
                continue
            check_suite_id = row.check_suite_id
            workflow_name = workflow_name_by_suite.get(check_suite_id)
            if workflow_name:
                name = f"{workflow_name} / {name}"
            existing = check_by_name.get(name)
            if existing is None:
                check_by_name[name] = row
                continue
            row_completed = row.completed_at
            existing_completed = existing.completed_at
            if row_completed is None or existing_completed is None:
                check_by_name[name] = row
                continue
            if row_completed > existing_completed:
                check_by_name[name] = row

        result_items: Dict[str, _ResultItem] = {}
        for normalized_name, row in check_by_name.items():
            item = _ResultItem(
                name=normalized_name,
                status=self._status_from_check_run(row),
                required=False,
            )
            result_items[item.name] = item

        for row in status_rows:
            context = row.context
            if context == self.config.PROJECTION_CHECK_RUN_NAME:
                continue
            if context is None:
                continue
            state = row.state
            if state is None:
                continue
            status = "failure" if state in ("failure", "error") else state
            if status not in ("success", "pending", "failure"):
                continue
            result_items[context] = _ResultItem(
                name=context, status=status, required=False
            )

        status_by_name = {
            str(row.context): row for row in status_rows if row.context is not None
        }

        observed_names = set(result_items.keys())
        missing_pattern_failures: list[str] = []
        for rule in rules:
            if rule.required_checks:
                seen = set()
                for name, item in list(result_items.items()):
                    if name in rule.required_checks:
                        result_items[name] = _ResultItem(
                            name=item.name, status=item.status, required=True
                        )
                        seen.add(name)
                for missing in rule.required_checks - seen:
                    result_items[missing] = _ResultItem(
                        name=missing, status="missing", required=True
                    )

            for pattern in rule.required_pattern:
                matched = False
                for name, item in list(result_items.items()):
                    if fnmatch(name, pattern):
                        result_items[name] = _ResultItem(
                            name=item.name, status=item.status, required=True
                        )
                        matched = True
                if not matched:
                    result_items[pattern] = _ResultItem(
                        name=pattern, status="failure", required=True
                    )
                    missing_pattern_failures.append(pattern)

        failures = [item for item in result_items.values() if item.status == "failure"]
        explicit_failures = [item for item in failures if item.name in observed_names]
        in_progress = [
            item
            for item in result_items.values()
            if item.required and item.status in ("pending", "missing")
        ]
        required_successes = [
            item
            for item in result_items.values()
            if item.required and item.status == "success"
        ]
        return _ComputedEvaluation(
            check_by_name=check_by_name,
            result_items=result_items,
            status_by_name=status_by_name,
            failures=failures,
            explicit_failures=explicit_failures,
            in_progress=in_progress,
            required_successes=required_successes,
            missing_pattern_failures=missing_pattern_failures,
        )

    @staticmethod
    def _build_output_checks_json(computed: _ComputedEvaluation) -> str:
        checks: list[Dict[str, Any]] = []
        for item in sorted(
            computed.result_items.values(), key=lambda value: value.name
        ):
            check_row = computed.check_by_name.get(item.name)
            status_row = computed.status_by_name.get(item.name)
            html_url = None
            source = "derived"
            if check_row is not None:
                html_url = check_row.html_url
                source = "check_run"
            elif status_row is not None:
                html_url = status_row.url
                source = "status"
            checks.append(
                {
                    "name": item.name,
                    "status": item.status,
                    "required": item.required,
                    "html_url": html_url,
                    "source": source,
                }
            )
        return json.dumps(checks, separators=(",", ":"), sort_keys=True)

    def _should_auto_refresh_on_missing(
        self,
        *,
        trigger: ProjectionTrigger,
        pr_row: PullRequestHeadRow,
        pr_number: int,
        missing_pattern_failures: list[str],
        explicit_failures: list[_ResultItem],
    ) -> bool:
        if trigger.force_api_refresh:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_force_api_refresh"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="missing_refresh",
                result="skip_force_api_refresh",
                detail="Manual force refresh already requested",
            )
            return False
        if not self.config.PROJECTION_AUTO_REFRESH_ON_MISSING_ENABLED:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_disabled"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="missing_refresh",
                result="skip_disabled",
            )
            return False
        if not missing_pattern_failures:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_no_missing"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="missing_refresh",
                result="skip_no_missing",
            )
            return False
        if explicit_failures:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_explicit_failure"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="missing_refresh",
                result="skip_explicit_failure",
            )
            return False

        pr_updated_at = pr_row.updated_at
        if pr_updated_at is None:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_no_pr_updated_at"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="missing_refresh",
                result="skip_no_pr_updated_at",
            )
            return False
        pr_age_seconds = (datetime.now(timezone.utc) - pr_updated_at).total_seconds()
        stale_seconds = max(
            0, int(self.config.PROJECTION_AUTO_REFRESH_ON_MISSING_STALE_SECONDS)
        )
        if pr_age_seconds < stale_seconds:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_recent_pr"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="missing_refresh",
                result="skip_recent_pr",
                detail=f"pr_age_seconds={int(pr_age_seconds)}",
            )
            return False

        now = time.monotonic()
        retry_after = self._auto_refresh_missing_cooldown.get(trigger.key, 0.0)
        if retry_after > now:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_cooldown"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="missing_refresh",
                result="skip_cooldown",
            )
            return False
        self._auto_refresh_missing_cooldown[trigger.key] = now + max(
            0,
            int(self.config.PROJECTION_AUTO_REFRESH_ON_MISSING_COOLDOWN_SECONDS),
        )
        self._record_activity(
            trigger=trigger,
            pr_number=pr_number,
            activity_type="missing_refresh",
            result="triggered",
            detail="Refreshing projections from GitHub API",
            metadata={"missing_patterns": missing_pattern_failures},
        )
        return True

    def _should_auto_refresh_on_stale_running(
        self,
        *,
        trigger: ProjectionTrigger,
        pr_number: int,
        computed: _ComputedEvaluation,
    ) -> bool:
        if trigger.force_api_refresh:
            sentinel_projection_fallback_total.labels(
                kind="stale_running_refresh", result="skip_force_api_refresh"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="stale_running_refresh",
                result="skip_force_api_refresh",
                detail="Manual force refresh already requested",
            )
            return False
        if not self.config.PROJECTION_AUTO_REFRESH_ON_MISSING_ENABLED:
            sentinel_projection_fallback_total.labels(
                kind="stale_running_refresh", result="skip_disabled"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="stale_running_refresh",
                result="skip_disabled",
            )
            return False

        stale_seconds = max(
            0, int(self.config.PROJECTION_AUTO_REFRESH_ON_MISSING_STALE_SECONDS)
        )
        now_utc = datetime.now(timezone.utc)
        stale_checks: list[dict[str, int | str]] = []
        for item in computed.in_progress:
            check_row = computed.check_by_name.get(item.name)
            if check_row is None:
                continue
            if check_row.status not in ("queued", "in_progress", "pending"):
                continue
            if check_row.last_seen_at is None:
                continue
            age_seconds = int((now_utc - check_row.last_seen_at).total_seconds())
            if age_seconds < stale_seconds:
                continue
            stale_checks.append({"name": item.name, "age_seconds": age_seconds})

        if not stale_checks:
            sentinel_projection_fallback_total.labels(
                kind="stale_running_refresh", result="skip_no_stale_running"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="stale_running_refresh",
                result="skip_no_stale_running",
            )
            return False

        now = time.monotonic()
        retry_after = self._auto_refresh_missing_cooldown.get(trigger.key, 0.0)
        if retry_after > now:
            sentinel_projection_fallback_total.labels(
                kind="stale_running_refresh", result="skip_cooldown"
            ).inc()
            self._record_activity(
                trigger=trigger,
                pr_number=pr_number,
                activity_type="stale_running_refresh",
                result="skip_cooldown",
            )
            return False

        self._auto_refresh_missing_cooldown[trigger.key] = now + max(
            0,
            int(self.config.PROJECTION_AUTO_REFRESH_ON_MISSING_COOLDOWN_SECONDS),
        )
        self._record_activity(
            trigger=trigger,
            pr_number=pr_number,
            activity_type="stale_running_refresh",
            result="triggered",
            detail="Refreshing projections from GitHub API due to stale running checks",
            metadata={"checks": stale_checks},
        )
        return True

    async def _load_head_rows_from_api(
        self, *, api: API, trigger: ProjectionTrigger, pr_number: int
    ) -> tuple[list[CheckRunRow], list[WorkflowRunRow], list[CommitStatusRow]]:
        self._record_activity(
            trigger=trigger,
            pr_number=pr_number,
            activity_type="api_snapshot_refresh",
            result="started",
            detail="Refreshing check runs/workflows/statuses from GitHub API",
        )
        repo = await api.get_repository(f"/repos/{trigger.repo_full_name}")
        check_runs = [
            check_run
            async for check_run in api.get_check_runs_for_ref(repo, trigger.head_sha)
        ]
        workflow_runs = [
            workflow_run
            async for workflow_run in api.get_workflow_runs_for_ref(
                repo, trigger.head_sha
            )
        ]
        statuses = [
            status async for status in api.get_status_for_ref(repo, trigger.head_sha)
        ]

        check_rows = [
            CheckRunRow.from_github_check_run(check_run) for check_run in check_runs
        ]
        workflow_rows = [
            WorkflowRunRow.from_github_actions_run(workflow_run)
            for workflow_run in workflow_runs
        ]
        status_rows = [
            CommitStatusRow.from_github_commit_status(status) for status in statuses
        ]
        persisted = self.store.upsert_head_snapshot_from_api(
            repo_id=trigger.repo_id,
            repo_full_name=trigger.repo_full_name,
            head_sha=trigger.head_sha,
            delivery_id=trigger.delivery_id,
            check_rows=check_rows,
            workflow_rows=workflow_rows,
            status_rows=status_rows,
        )
        logger.debug(
            "Projection eval API snapshot persisted repo=%s sha=%s checks=%d workflows=%d statuses=%d",
            trigger.repo_full_name,
            trigger.head_sha,
            persisted["check_runs"],
            persisted["workflow_runs"],
            persisted["commit_statuses"],
        )
        self._record_activity(
            trigger=trigger,
            pr_number=pr_number,
            activity_type="api_snapshot_refresh",
            result="persisted",
            detail=(
                f"checks={persisted['check_runs']} "
                f"workflows={persisted['workflow_runs']} "
                f"statuses={persisted['commit_statuses']}"
            ),
        )
        return check_rows, workflow_rows, status_rows

    @staticmethod
    def _dt_to_iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    @staticmethod
    def _status_from_check_run(row: CheckRunRow) -> str:
        status = row.status
        conclusion = row.conclusion
        if status == "completed" and conclusion in ("cancelled", "failure"):
            return "failure"
        if status in ("queued", "in_progress", "pending"):
            return "pending"
        if status == "completed" and conclusion == "success":
            return "success"
        if status == "completed" and conclusion in ("neutral", "skipped"):
            return "neutral"
        return "pending"

    @classmethod
    def _min_started_at(cls, rows: Iterable[CheckRunRow]) -> datetime | None:
        vals = [r.started_at for r in rows if r.started_at is not None]
        if not vals:
            return None
        return min(vals)

    @classmethod
    def _max_completed_at(cls, rows: Iterable[CheckRunRow]) -> datetime | None:
        vals = [r.completed_at for r in rows if r.completed_at is not None]
        if not vals:
            return None
        return max(vals)

    def _record_activity(
        self,
        *,
        trigger: ProjectionTrigger,
        pr_number: int,
        activity_type: str,
        result: str | None,
        detail: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        try:
            self.store.record_projection_activity_event(
                repo_id=trigger.repo_id,
                repo_full_name=trigger.repo_full_name,
                pr_number=pr_number,
                head_sha=trigger.head_sha,
                delivery_id=trigger.delivery_id,
                activity_type=activity_type,
                result=result,
                detail=detail,
                metadata=metadata,
            )
        except Exception:  # noqa: BLE001
            logger.debug(
                "Failed to record projection activity event type=%s result=%s",
                activity_type,
                result,
                exc_info=True,
            )
