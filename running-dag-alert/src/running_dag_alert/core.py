"""Pure logic: config parsing, pagination, counting, cooldown and formatting.

Nothing here calls AWS, so the unit tests need no mocks.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# The Airflow REST API caps page size with [api] maximum_page_limit,
# 100 by default in both Airflow 2 and 3.
DEFAULT_PAGE_SIZE = 100
# Safety stop. 50 pages of 100 is far beyond any sane number of running runs.
DEFAULT_MAX_PAGES = 50
# How many DAG lines to put in one alert before summarising the rest.
MAX_DAGS_LISTED = 25


class ConfigError(ValueError):
    pass


class ApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class Environment:
    label: str  # short name used in alerts and metrics, e.g. "dev"
    mwaa_name: str  # the MWAA environment name passed to InvokeRestApi


@dataclass(frozen=True)
class DagSummary:
    dag_id: str
    running_runs: int
    oldest_start: datetime | None


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------


def parse_environments(text: str) -> list[Environment]:
    """Parse "dev=mwaa-env-dev,preprod=mwaa-env-preprod".

    A bare name ("mwaa-env-dev") uses the MWAA name as the label too.
    """
    envs = []
    for item in (text or "").split(","):
        item = item.strip()
        if not item:
            continue
        label, sep, name = item.partition("=")
        label, name = label.strip(), name.strip()
        if not sep:
            name = label
        if not label or not name:
            raise ConfigError(f"bad environment entry: {item!r}")
        envs.append(Environment(label=label, mwaa_name=name))
    if not envs:
        raise ConfigError("no environments configured")
    labels = [e.label for e in envs]
    if len(set(labels)) != len(labels):
        raise ConfigError("environment labels must be unique")
    return envs


def airflow_major(version: str) -> int:
    """Return the major version from an MWAA AirflowVersion string like 2.10.3."""
    try:
        major = int(str(version).strip().split(".")[0])
    except ValueError as exc:
        raise ConfigError(f"cannot read Airflow version {version!r}") from exc
    if major not in (2, 3):
        raise ConfigError(f"unsupported Airflow major version {major}")
    return major


def rest_base_path(major: int) -> str:
    """Base path of the Airflow REST API on the web server.

    Only needed when calling the web server directly with a session token.
    InvokeRestApi takes the path without this prefix.
    """
    return "/api/v1" if major == 2 else "/api/v2"


def running_runs_request(offset: int, limit: int) -> dict:
    """Path and query for "list DAG runs across all DAGs, state=running".

    The same path works on Airflow 2 (/api/v1) and 3 (/api/v2): "~" as the
    dag_id means all DAGs, and state, limit and offset exist in both.
    order_by keeps paging stable while runs start and finish between pages.
    """
    return {
        "Path": "/dags/~/dagRuns",
        "Method": "GET",
        "QueryParameters": {
            "state": "running",
            "limit": limit,
            "offset": offset,
            "order_by": "start_date",
        },
    }


# --------------------------------------------------------------------------
# Pagination
# --------------------------------------------------------------------------


def iter_dag_runs(
    fetch_page: Callable[[int, int], dict],
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
) -> Iterator[dict]:
    """Yield every DAG run from a paginated /dagRuns endpoint.

    fetch_page(offset, limit) must return the decoded JSON body, which has
    "dag_runs" and "total_entries" in both API versions.

    The server silently caps the page at [api] maximum_page_limit, so a page
    shorter than requested does not mean the end when total_entries says
    otherwise. The short-page check is only used when total_entries is absent.
    """
    offset = 0
    for _ in range(max_pages):
        body = fetch_page(offset, page_size)
        runs = body.get("dag_runs")
        if runs is None:
            raise ApiError(f"response has no dag_runs field: {sorted(body)}")
        yield from runs
        offset += len(runs)
        total = body.get("total_entries")
        if not runs:
            return
        if total is not None:
            if offset >= total:
                return
        elif len(runs) < page_size:
            return
    raise ApiError(f"stopped after {max_pages} pages, more runs than expected")


# --------------------------------------------------------------------------
# Counting
# --------------------------------------------------------------------------


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def summarise(runs: list[dict]) -> list[DagSummary]:
    """Group running DAG runs by DAG id.

    Runs not in state "running" are ignored, in case the server ignored the
    state filter. Sorted by number of running runs, then oldest start first.
    """
    by_dag: dict[str, list[datetime | None]] = {}
    for run in runs:
        if run.get("state") != "running" or not run.get("dag_id"):
            continue
        by_dag.setdefault(run["dag_id"], []).append(_parse_ts(run.get("start_date")))

    summaries = []
    for dag_id, starts in by_dag.items():
        known = [s for s in starts if s is not None]
        summaries.append(DagSummary(dag_id, len(starts), min(known) if known else None))

    far_future = datetime.max.replace(tzinfo=timezone.utc)
    summaries.sort(key=lambda s: (-s.running_runs, s.oldest_start or far_future, s.dag_id))
    return summaries


def total_runs(summaries: list[DagSummary]) -> int:
    return sum(s.running_runs for s in summaries)


def is_over_threshold(summaries: list[DagSummary], threshold: int, count_by: str = "runs") -> bool:
    """True when the count is strictly greater than the threshold.

    count_by "runs" counts DAG runs. "dags" counts distinct DAGs, which
    matters for DAGs with max_active_runs above 1.
    """
    if count_by not in ("runs", "dags"):
        raise ConfigError("count_by must be 'runs' or 'dags'")
    count = total_runs(summaries) if count_by == "runs" else len(summaries)
    return count > threshold


def should_alert(over: bool, last_alert_at: datetime | None, now: datetime, cooldown: timedelta) -> bool:
    """Alert when over threshold, at most once per cooldown per environment."""
    if not over:
        return False
    if last_alert_at is None:
        return True
    return now - last_alert_at >= cooldown


# --------------------------------------------------------------------------
# Formatting
# --------------------------------------------------------------------------


def _age(start: datetime | None, now: datetime) -> str:
    if start is None:
        return "start unknown"
    minutes = max(0, int((now - start).total_seconds() // 60))
    hours, mins = divmod(minutes, 60)
    if hours >= 24:
        return f"running {hours // 24}d {hours % 24}h"
    if hours:
        return f"running {hours}h {mins}m"
    return f"running {mins}m"


def format_lines(summaries: list[DagSummary], now: datetime, limit: int = MAX_DAGS_LISTED) -> list[str]:
    lines = []
    for s in summaries[:limit]:
        runs = f"{s.running_runs} runs" if s.running_runs > 1 else "1 run"
        lines.append(f"{s.dag_id}  ({runs}, oldest {_age(s.oldest_start, now)})")
    hidden = len(summaries) - limit
    if hidden > 0:
        lines.append(f"...and {hidden} more DAGs")
    return lines


def format_alert(
    env_label: str,
    summaries: list[DagSummary],
    threshold: int,
    now: datetime,
    count_by: str = "runs",
    ui_url: str | None = None,
) -> dict:
    """Slack incoming-webhook payload listing what is running."""
    runs = total_runs(summaries)
    dags = len(summaries)
    unit = "DAG runs" if count_by == "runs" else "DAGs"
    count = runs if count_by == "runs" else dags
    title = f"[{env_label}] {count} {unit} running (threshold {threshold})"

    body = "\n".join(format_lines(summaries, now))
    action = (
        "Check whether these need to be running. Pause or mark failed the ones that do not, "
        "and look at why so many started together (catchup, a backfill, a shared upstream trigger)."
    )
    context = f"{runs} running runs across {dags} DAGs at {now.strftime('%Y-%m-%d %H:%M')} UTC"
    if ui_url:
        context += f" | <{ui_url}|Airflow UI>"

    return {
        "text": title,
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": title[:150]}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "```" + body[:2900] + "```"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": action}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": context}]},
        ],
    }


def build_emf(namespace: str, env_label: str, runs: int, dags: int, timestamp_ms: int) -> dict:
    """CloudWatch Embedded Metric Format record for the running counts.

    Published on every run, so a CloudWatch alarm or an external monitor
    can use the same numbers without calling Airflow.
    """
    return {
        "_aws": {
            "Timestamp": timestamp_ms,
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [["Environment"]],
                    "Metrics": [
                        {"Name": "RunningDagRuns", "Unit": "Count"},
                        {"Name": "RunningDags", "Unit": "Count"},
                    ],
                }
            ],
        },
        "Environment": env_label,
        "RunningDagRuns": runs,
        "RunningDags": dags,
    }
