from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from fnmatch import fnmatch
import hashlib
import io
import time
from typing import Any, Awaitable, Callable, Dict, Iterable, List

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
    check_by_name: Dict[str, Dict[str, Any]]
    result_items: Dict[str, _ResultItem]
    failures: list[_ResultItem]
    explicit_failures: list[_ResultItem]
    in_progress: list[_ResultItem]
    required_successes: list[_ResultItem]
    missing_pattern_failures: list[str]


class ProjectionEvaluator:
    def __init__(
        self,
        *,
        store: WebhookStore,
        app_id: int,
        check_run_name: str,
        publish_enabled: bool,
        manual_refresh_action_enabled: bool,
        manual_refresh_action_identifier: str,
        manual_refresh_action_label: str,
        manual_refresh_action_description: str,
        path_rule_fallback_enabled: bool,
        auto_refresh_on_missing_enabled: bool,
        auto_refresh_on_missing_stale_seconds: int,
        auto_refresh_on_missing_cooldown_seconds: int,
        config_cache_seconds: int,
        pr_files_cache_seconds: int,
        api_factory: Callable[[int], Awaitable[API]],
    ):
        self.store = store
        self.app_id = app_id
        self.check_run_name = check_run_name
        self.publish_enabled = publish_enabled
        self.manual_refresh_action_enabled = manual_refresh_action_enabled
        self.manual_refresh_action_identifier = manual_refresh_action_identifier
        self.manual_refresh_action_label = manual_refresh_action_label
        self.manual_refresh_action_description = manual_refresh_action_description
        self.path_rule_fallback_enabled = path_rule_fallback_enabled
        self.auto_refresh_on_missing_enabled = auto_refresh_on_missing_enabled
        self.auto_refresh_on_missing_stale_seconds = max(
            0, int(auto_refresh_on_missing_stale_seconds)
        )
        self.auto_refresh_on_missing_cooldown_seconds = max(
            0, int(auto_refresh_on_missing_cooldown_seconds)
        )
        self.config_cache_seconds = config_cache_seconds
        self.pr_files_cache_seconds = pr_files_cache_seconds
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
                open_prs[0].get("pr_number"),
            )
            sentinel_projection_eval_total.labels(result="ambiguous_pr").inc()
        pr_row = open_prs[0]

        api = await self.api_factory(trigger.installation_id)
        config = await self._get_repo_config(api, trigger)
        if config is None:
            logger.info(
                "Projection eval skip repo=%s sha=%s pr=%s reason=no_config",
                trigger.repo_full_name,
                head_sha,
                pr_row.get("pr_number"),
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
            pr_row.get("pr_number"),
            len(config.rules),
            has_path_rules,
        )
        if has_path_rules and self.path_rule_fallback_enabled:
            changed_files = await self._get_pr_files(
                api=api,
                trigger=trigger,
                pr_number=int(pr_row["pr_number"]),
            )
            logger.debug(
                "Projection eval path fallback repo=%s sha=%s pr=%s changed_files=%d",
                trigger.repo_full_name,
                head_sha,
                pr_row.get("pr_number"),
                len(changed_files),
            )

        pr_like = _RulePullRequestLike(base=_RuleBaseRef(ref=str(pr_row["base_ref"])))
        rules = determine_rules(changed_files, pr_like, config.rules)
        logger.debug(
            "Projection eval selected rules repo=%s sha=%s pr=%s selected=%d",
            trigger.repo_full_name,
            head_sha,
            pr_row.get("pr_number"),
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
        if self._should_auto_refresh_on_missing(
            trigger=trigger,
            pr_row=pr_row,
            missing_pattern_failures=computed.missing_pattern_failures,
            explicit_failures=computed.explicit_failures,
        ):
            logger.info(
                "Projection eval auto-refreshing from API repo=%s sha=%s missing_patterns=%d",
                trigger.repo_full_name,
                head_sha,
                len(computed.missing_pattern_failures),
            )
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="triggered"
            ).inc()
            check_rows, workflow_rows, status_rows = await self._load_head_rows_from_api(
                api=api,
                trigger=trigger,
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

        sentinel_row = self.store.get_sentinel_check_run(
            repo_id=repo_id,
            head_sha=head_sha,
            check_name=self.check_run_name,
            app_id=self.app_id,
        )

        if not self.store.get_open_pr_candidates(repo_id, head_sha):
            logger.info(
                "Projection eval skip repo=%s sha=%s reason=no_open_pr_before_publish",
                trigger.repo_full_name,
                head_sha,
            )
            sentinel_projection_eval_total.labels(result="no_pr").inc()
            return EvaluationResult(result="no_pr")

        previous_status = sentinel_row.get("status") if sentinel_row else None
        existing_id = sentinel_row.get("check_run_id") if sentinel_row else None
        check_run_id = (
            existing_id if isinstance(existing_id, int) and existing_id > 0 else None
        )

        if new_status == "in_progress" and previous_status != "in_progress":
            check_run_id = None

        unchanged = bool(
            sentinel_row
            and sentinel_row.get("status") == new_status
            and sentinel_row.get("conclusion") == new_conclusion
            and sentinel_row.get("output_title") == title
            and sentinel_row.get("output_summary_hash") == summary_hash
            and sentinel_row.get("output_text_hash") == text_hash
        )
        logger.debug(
            "Projection eval diff repo=%s sha=%s unchanged=%s prev_status=%s new_status=%s prev_conclusion=%s new_conclusion=%s",
            trigger.repo_full_name,
            head_sha,
            unchanged,
            sentinel_row.get("status") if sentinel_row else None,
            new_status,
            sentinel_row.get("conclusion") if sentinel_row else None,
            new_conclusion,
        )

        now_iso = utcnow_iso()
        started_at = self._min_started_at(computed.check_by_name.values())
        completed_at = (
            self._max_completed_at(computed.check_by_name.values())
            if new_status == "completed"
            else None
        )

        if unchanged:
            logger.info(
                "Projection eval unchanged repo=%s sha=%s pr=%s check_run_id=%s",
                trigger.repo_full_name,
                head_sha,
                pr_row.get("pr_number"),
                check_run_id,
            )
            sentinel_projection_publish_total.labels(result="unchanged").inc()
            self.store.upsert_sentinel_check_run(
                repo_id=repo_id,
                repo_full_name=trigger.repo_full_name,
                check_run_id=check_run_id,
                head_sha=head_sha,
                name=self.check_run_name,
                status=new_status,
                conclusion=new_conclusion,
                app_id=self.app_id,
                started_at=started_at,
                completed_at=completed_at,
                output_title=title,
                output_summary=output_summary,
                output_text=output_text,
                output_summary_hash=summary_hash,
                output_text_hash=text_hash,
                last_eval_at=now_iso,
                last_publish_at=sentinel_row.get("last_publish_at")
                if sentinel_row
                else None,
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

        if self.publish_enabled:
            logger.info(
                "Projection publish attempt repo=%s sha=%s pr=%s current_id=%s status=%s conclusion=%s",
                trigger.repo_full_name,
                head_sha,
                pr_row.get("pr_number"),
                check_run_id,
                new_status,
                new_conclusion,
            )
            check_run = CheckRun.make_app_check_run(
                id=check_run_id,
                head_sha=head_sha,
                status=new_status,
                conclusion=new_conclusion,
                started_at=self._parse_dt(started_at) or datetime.now(timezone.utc),
                completed_at=self._parse_dt(completed_at),
                output=CheckRunOutput(
                    title=title,
                    summary=output_summary,
                    text=output_text,
                ),
                actions=self._build_manual_refresh_actions(new_status),
            )

            if check_run.id is None:
                sentinel_projection_lookup_total.labels(result="local_id_miss").inc()
                logger.debug(
                    "Projection lookup local miss repo=%s sha=%s",
                    trigger.repo_full_name,
                    head_sha,
                )
                existing = await api.find_existing_sentinel_check_run(
                    repo_url=self._repo_url(trigger.repo_full_name),
                    head_sha=head_sha,
                    check_name=self.check_run_name,
                    app_id=self.app_id,
                )
                if existing is not None and existing.id is not None:
                    sentinel_projection_lookup_total.labels(
                        result="gh_lookup_hit"
                    ).inc()
                    check_run.id = int(existing.id)
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
                    logger.info(
                        "Projection lookup github miss repo=%s sha=%s",
                        trigger.repo_full_name,
                        head_sha,
                    )
            else:
                sentinel_projection_lookup_total.labels(result="local_id_hit").inc()
                logger.debug(
                    "Projection lookup local hit repo=%s sha=%s id=%s",
                    trigger.repo_full_name,
                    head_sha,
                    check_run.id,
                )

            try:
                posted_id = await api.post_check_run(
                    self._repo_url(trigger.repo_full_name), check_run
                )
                published_id = posted_id or check_run.id
                publish_result = "published"
                publish_at = now_iso
                sentinel_projection_publish_total.labels(result="published").inc()
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
                logger.error(
                    "Publishing projection check run failed repo=%s sha=%s pr=%s",
                    trigger.repo_full_name,
                    head_sha,
                    pr_row.get("pr_number"),
                    exc_info=True,
                )
        else:
            logger.info(
                "Projection publish skipped (dry_run) repo=%s sha=%s pr=%s status=%s conclusion=%s",
                trigger.repo_full_name,
                head_sha,
                pr_row.get("pr_number"),
                new_status,
                new_conclusion,
            )
            sentinel_projection_publish_total.labels(result="dry_run").inc()

        final_id = int(published_id) if published_id is not None else check_run_id
        self.store.upsert_sentinel_check_run(
            repo_id=repo_id,
            repo_full_name=trigger.repo_full_name,
            check_run_id=final_id,
            head_sha=head_sha,
            name=self.check_run_name,
            status=new_status,
            conclusion=new_conclusion,
            app_id=self.app_id,
            started_at=started_at,
            completed_at=completed_at,
            output_title=title,
            output_summary=output_summary,
            output_text=output_text,
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
        self, api: API, trigger: ProjectionTrigger
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
            return cached[1]

        sentinel_projection_fallback_total.labels(
            kind="config", result="cache_miss"
        ).inc()
        logger.debug(
            "Projection config cache miss repo=%s repo_id=%s",
            trigger.repo_full_name,
            trigger.repo_id,
        )
        try:
            content = await api.get_content(
                self._repo_url(trigger.repo_full_name), ".merge-sentinel.yml"
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
        except Exception as exc:  # noqa: BLE001
            if hasattr(exc, "status_code") and getattr(exc, "status_code") == 404:
                loaded = None
                logger.info(
                    "Projection config missing repo=%s repo_id=%s",
                    trigger.repo_full_name,
                    trigger.repo_id,
                )
            else:
                raise

        self._config_cache[trigger.repo_id] = (
            now + self.config_cache_seconds,
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
        pull = await api.get_pull(self._repo_url(trigger.repo_full_name), pr_number)
        files = [f.filename async for f in api.get_pull_request_files(pull)]
        self._pr_files_cache[key] = (now + self.pr_files_cache_seconds, list(files))
        logger.debug(
            "Projection pr_files fetched repo=%s sha=%s pr=%s count=%d",
            trigger.repo_full_name,
            trigger.head_sha,
            pr_number,
            len(files),
        )
        return files

    def _build_manual_refresh_actions(self, status: str) -> List[CheckRunAction]:
        if not self.manual_refresh_action_enabled:
            return []
        if status != "completed":
            return []
        return [
            CheckRunAction(
                label=self.manual_refresh_action_label,
                description=self.manual_refresh_action_description,
                identifier=self.manual_refresh_action_identifier,
            )
        ]

    def _evaluate_rows(
        self,
        *,
        check_rows: list[Dict[str, Any]],
        workflow_rows: list[Dict[str, Any]],
        status_rows: list[Dict[str, Any]],
        rules: list[Any],
    ) -> _ComputedEvaluation:
        filtered_check_rows = [
            row
            for row in check_rows
            if not (
                row.get("app_id") == self.app_id
                and row.get("name") == self.check_run_name
            )
        ]
        workflow_name_by_suite = {
            row.get("check_suite_id"): row.get("name")
            for row in workflow_rows
            if row.get("check_suite_id") is not None and row.get("name")
        }

        normalized_checks = []
        for row in filtered_check_rows:
            name = row["name"]
            check_suite_id = row.get("check_suite_id")
            workflow_name = workflow_name_by_suite.get(check_suite_id)
            if workflow_name:
                name = f"{workflow_name} / {name}"
            normalized_checks.append({**row, "normalized_name": name})

        check_by_name: Dict[str, Dict[str, Any]] = {}
        for row in normalized_checks:
            name = row["normalized_name"]
            existing = check_by_name.get(name)
            if existing is None:
                check_by_name[name] = row
                continue
            row_completed = row.get("completed_at")
            existing_completed = existing.get("completed_at")
            if row_completed is None or existing_completed is None:
                check_by_name[name] = row
                continue
            if row_completed > existing_completed:
                check_by_name[name] = row

        result_items: Dict[str, _ResultItem] = {}
        for row in check_by_name.values():
            item = _ResultItem(
                name=row["normalized_name"],
                status=self._status_from_check_run(row),
                required=False,
            )
            result_items[item.name] = item

        for row in status_rows:
            context = row["context"]
            if context == self.check_run_name:
                continue
            state = row["state"]
            status = "failure" if state in ("failure", "error") else state
            if status not in ("success", "pending", "failure"):
                continue
            result_items[context] = _ResultItem(
                name=context, status=status, required=False
            )

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
            failures=failures,
            explicit_failures=explicit_failures,
            in_progress=in_progress,
            required_successes=required_successes,
            missing_pattern_failures=missing_pattern_failures,
        )

    def _should_auto_refresh_on_missing(
        self,
        *,
        trigger: ProjectionTrigger,
        pr_row: Dict[str, Any],
        missing_pattern_failures: list[str],
        explicit_failures: list[_ResultItem],
    ) -> bool:
        if trigger.force_api_refresh:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_force_api_refresh"
            ).inc()
            return False
        if not self.auto_refresh_on_missing_enabled:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_disabled"
            ).inc()
            return False
        if not missing_pattern_failures:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_no_missing"
            ).inc()
            return False
        if explicit_failures:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_explicit_failure"
            ).inc()
            return False

        pr_updated_at = self._parse_dt(pr_row.get("updated_at"))
        if pr_updated_at is None:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_no_pr_updated_at"
            ).inc()
            return False
        pr_age_seconds = (
            datetime.now(timezone.utc) - pr_updated_at
        ).total_seconds()
        if pr_age_seconds < self.auto_refresh_on_missing_stale_seconds:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_recent_pr"
            ).inc()
            return False

        now = time.monotonic()
        retry_after = self._auto_refresh_missing_cooldown.get(trigger.key, 0.0)
        if retry_after > now:
            sentinel_projection_fallback_total.labels(
                kind="missing_refresh", result="skip_cooldown"
            ).inc()
            return False
        self._auto_refresh_missing_cooldown[trigger.key] = (
            now + self.auto_refresh_on_missing_cooldown_seconds
        )
        return True

    async def _load_head_rows_from_api(
        self, *, api: API, trigger: ProjectionTrigger
    ) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]]]:
        repo = await api.get_repository(self._repo_url(trigger.repo_full_name))
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

        check_rows = [self._check_run_to_row(check_run) for check_run in check_runs]
        workflow_rows = [
            self._workflow_run_to_row(workflow_run) for workflow_run in workflow_runs
        ]
        status_rows = [self._status_to_row(status) for status in statuses]
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
        return check_rows, workflow_rows, status_rows

    @staticmethod
    def _check_run_to_row(check_run: CheckRun) -> Dict[str, Any]:
        app = check_run.app
        check_suite = check_run.check_suite
        return {
            "check_run_id": check_run.id,
            "name": check_run.name,
            "status": check_run.status,
            "conclusion": check_run.conclusion,
            "app_id": app.id if app is not None else None,
            "app_slug": app.slug if app is not None else None,
            "check_suite_id": check_suite.id if check_suite is not None else None,
            "started_at": ProjectionEvaluator._dt_to_iso(check_run.started_at),
            "completed_at": ProjectionEvaluator._dt_to_iso(check_run.completed_at),
        }

    @staticmethod
    def _workflow_run_to_row(workflow_run: Any) -> Dict[str, Any]:
        return {
            "workflow_run_id": workflow_run.id,
            "name": workflow_run.name,
            "event": workflow_run.event,
            "status": workflow_run.status,
            "conclusion": workflow_run.conclusion,
            "run_number": workflow_run.run_number,
            "workflow_id": workflow_run.workflow_id,
            "check_suite_id": workflow_run.check_suite_id,
            "created_at": ProjectionEvaluator._dt_to_iso(workflow_run.created_at),
            "updated_at": ProjectionEvaluator._dt_to_iso(workflow_run.updated_at),
        }

    @staticmethod
    def _status_to_row(status: Any) -> Dict[str, Any]:
        return {
            "status_id": status.id,
            "url": status.url,
            "sha": status.sha,
            "context": status.context,
            "state": status.state,
            "created_at": ProjectionEvaluator._dt_to_iso(status.created_at),
            "updated_at": ProjectionEvaluator._dt_to_iso(status.updated_at),
        }

    @staticmethod
    def _dt_to_iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    @staticmethod
    def _status_from_check_run(row: Dict[str, Any]) -> str:
        status = row.get("status")
        conclusion = row.get("conclusion")
        if status == "completed" and conclusion in ("cancelled", "failure"):
            return "failure"
        if status in ("queued", "in_progress", "pending"):
            return "pending"
        if status == "completed" and conclusion == "success":
            return "success"
        if status == "completed" and conclusion in ("neutral", "skipped"):
            return "neutral"
        return "pending"

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if value is None:
            return None
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    @classmethod
    def _min_started_at(cls, rows: Iterable[Dict[str, Any]]) -> str | None:
        vals = [cls._parse_dt(r.get("started_at")) for r in rows if r.get("started_at")]
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        return min(vals).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    @classmethod
    def _max_completed_at(cls, rows: Iterable[Dict[str, Any]]) -> str | None:
        vals = [
            cls._parse_dt(r.get("completed_at")) for r in rows if r.get("completed_at")
        ]
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        return max(vals).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    @staticmethod
    def _repo_url(repo_full_name: str) -> str:
        return f"/repos/{repo_full_name}"
