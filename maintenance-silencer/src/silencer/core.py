"""Pure logic for the maintenance silencer.

Nothing in this module talks to AWS. The Lambda handler and the CLI both
import from here, and the unit tests cover it without mocks.

All times are handled as timezone-aware UTC datetimes. A timestamp written
without an offset is read as UTC, so a window typed by hand as
"2026-05-01T08:00:00" means 08:00 UTC, not local time.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# Airflow DAG ids may contain letters, digits, underscores, dots and dashes.
DAG_ID_PATTERN = r"[A-Za-z0-9_.\-]+"
_DAG_ID_RE = re.compile(rf"^{DAG_ID_PATTERN}$")

# Plain-text alert formats seen in the wild, tried in order. Each must
# capture the DAG id in group 1.
_TEXT_PATTERNS = [
    re.compile(rf"\bdag[_ ]?id\s*[:=]\s*[\"']?({DAG_ID_PATTERN})", re.IGNORECASE),
    re.compile(rf"\bDAG\s*:\s*[\"']?({DAG_ID_PATTERN})"),
    re.compile(rf"<DagRun\s+({DAG_ID_PATTERN})\s*@"),
]

# JSON keys that may hold the DAG id, checked in order.
_JSON_KEYS = ("dag_id", "dagId", "DagId", "dag")

STATE_ACTIVE = "active"
STATE_PENDING = "pending"
STATE_EXPIRED = "expired"

ACTION_FORWARD = "forward"
ACTION_SUPPRESS = "suppress"


class WindowError(ValueError):
    """Raised when a stored maintenance window cannot be used."""


@dataclass(frozen=True)
class MaintenanceWindow:
    start: datetime
    end: datetime
    reason: str
    set_by: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "start": format_utc(self.start),
                "end": format_utc(self.end),
                "reason": self.reason,
                "set_by": self.set_by,
            },
            sort_keys=True,
        )


@dataclass(frozen=True)
class Decision:
    action: str
    dag_id: str | None
    detail: str
    window: MaintenanceWindow | None = None


# --------------------------------------------------------------------------
# Time handling
# --------------------------------------------------------------------------


def parse_utc(value: str) -> datetime:
    """Parse an ISO 8601 timestamp and return it as an aware UTC datetime.

    Accepts a trailing "Z", an explicit offset, or no offset (read as UTC).
    """
    if not isinstance(value, str) or not value.strip():
        raise WindowError(f"timestamp must be a non-empty string, got {value!r}")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise WindowError(f"not an ISO 8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_utc(value: datetime) -> str:
    """Format an aware datetime as second-precision UTC with a Z suffix."""
    if value.tzinfo is None:
        raise ValueError("refusing to format a naive datetime")
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_duration(text: str) -> timedelta:
    """Parse a short duration such as 90m, 12h, 3d or 1d12h."""
    match = re.fullmatch(r"\s*(?:(\d+)d)?\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*", text or "")
    if not match or not any(match.groups()):
        raise WindowError(f"duration must look like 90m, 12h, 3d or 1d12h, got {text!r}")
    days, hours, minutes = (int(g) if g else 0 for g in match.groups())
    duration = timedelta(days=days, hours=hours, minutes=minutes)
    if duration <= timedelta(0):
        raise WindowError("duration must be greater than zero")
    return duration


def humanise(delta: timedelta) -> str:
    """Render a timedelta as a short string like 2d 3h or 45m."""
    total_minutes = int(delta.total_seconds() // 60)
    if total_minutes < 1:
        return "<1m"
    days, rem = divmod(total_minutes, 60 * 24)
    hours, minutes = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes and not days:
        parts.append(f"{minutes}m")
    return " ".join(parts)


# --------------------------------------------------------------------------
# Windows
# --------------------------------------------------------------------------


def parse_window(raw: str) -> MaintenanceWindow:
    """Parse the JSON stored in a maintenance parameter.

    Expected shape: {"start": ISO, "end": ISO, "reason": str, "set_by": str}.
    """
    try:
        data = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise WindowError(f"parameter value is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise WindowError("parameter value must be a JSON object")

    missing = [k for k in ("start", "end", "reason", "set_by") if not str(data.get(k) or "").strip()]
    if missing:
        raise WindowError(f"missing or empty field(s): {', '.join(missing)}")

    window = MaintenanceWindow(
        start=parse_utc(data["start"]),
        end=parse_utc(data["end"]),
        reason=str(data["reason"]).strip(),
        set_by=str(data["set_by"]).strip(),
    )
    if window.end <= window.start:
        raise WindowError("end must be after start")
    return window


def window_state(window: MaintenanceWindow, now: datetime) -> str:
    """Return pending, active or expired. The window is [start, end)."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if now < window.start:
        return STATE_PENDING
    if now < window.end:
        return STATE_ACTIVE
    return STATE_EXPIRED


def build_window(
    now: datetime,
    reason: str,
    set_by: str,
    start: datetime | None = None,
    end: datetime | None = None,
    duration: timedelta | None = None,
    max_duration: timedelta | None = None,
) -> MaintenanceWindow:
    """Build and validate a new window for the CLI.

    Exactly one of end or duration must be given. Start defaults to now.
    max_duration keeps windows time-boxed: a silence with no realistic end
    is the same as deleting the alert.
    """
    if (end is None) == (duration is None):
        raise WindowError("give exactly one of an end time or a duration")
    if not reason or not reason.strip():
        raise WindowError("a reason is required")
    if not set_by or not set_by.strip():
        raise WindowError("set_by is required")

    start = start or now
    end = end or start + duration
    if end <= start:
        raise WindowError("end must be after start")
    if end <= now:
        raise WindowError("window would already be expired")
    if max_duration is not None and end - start > max_duration:
        raise WindowError(
            f"window of {humanise(end - start)} exceeds the maximum of {humanise(max_duration)}"
        )
    return MaintenanceWindow(start=start, end=end, reason=reason.strip(), set_by=set_by.strip())


# --------------------------------------------------------------------------
# Message parsing
# --------------------------------------------------------------------------


def principal_name(arn: str) -> str:
    """Turn a caller identity ARN into a short name for set_by.

    assumed-role/<role>/<session> becomes "<role>/<session>",
    user/<name> becomes "<name>". Anything else is returned unchanged.
    """
    resource = arn.split(":", 5)[-1] if arn.count(":") >= 5 else arn
    parts = resource.split("/")
    if parts[0] == "assumed-role" and len(parts) >= 3:
        return f"{parts[1]}/{parts[-1]}"
    if parts[0] == "user" and len(parts) >= 2:
        return parts[-1]
    return arn


def is_valid_dag_id(value: object) -> bool:
    return isinstance(value, str) and bool(_DAG_ID_RE.match(value))


def extract_dag_id(message: str, subject: str | None = None) -> str | None:
    """Find the DAG id in an SNS alert.

    Tries a JSON body first (keys dag_id, dagId, DagId, dag, also one level
    down under "detail" or "context"), then known plain-text formats in the
    message, then the subject. Returns None if nothing matches, in which case
    the caller forwards the alert unchanged.
    """
    if message:
        try:
            data = json.loads(message)
        except (TypeError, json.JSONDecodeError):
            data = None
        if isinstance(data, dict):
            candidates = [data] + [
                data[k] for k in ("detail", "context") if isinstance(data.get(k), dict)
            ]
            for obj in candidates:
                for key in _JSON_KEYS:
                    if is_valid_dag_id(obj.get(key)):
                        return obj[key]

    for text in (message, subject):
        if not text:
            continue
        for pattern in _TEXT_PATTERNS:
            match = pattern.search(text)
            if match:
                return match.group(1)
    return None


def parameter_name(prefix: str, dag_id: str) -> str:
    """Join the parameter prefix and DAG id into an SSM parameter name."""
    if not is_valid_dag_id(dag_id):
        raise ValueError(f"invalid DAG id: {dag_id!r}")
    return prefix.rstrip("/") + "/" + dag_id


# --------------------------------------------------------------------------
# Decision
# --------------------------------------------------------------------------


def decide(dag_id: str | None, raw_window: str | None, now: datetime) -> Decision:
    """Decide whether to forward or suppress one alert.

    The silencer fails open: anything unexpected (no DAG id, a malformed
    window) results in the alert being forwarded with a note, never dropped.
    """
    if dag_id is None:
        return Decision(ACTION_FORWARD, None, "no DAG id found in alert")
    if raw_window is None:
        return Decision(ACTION_FORWARD, dag_id, "no maintenance window")

    try:
        window = parse_window(raw_window)
    except WindowError as exc:
        return Decision(ACTION_FORWARD, dag_id, f"maintenance window ignored, invalid: {exc}")

    state = window_state(window, now)
    if state == STATE_ACTIVE:
        return Decision(
            ACTION_SUPPRESS,
            dag_id,
            f"in maintenance until {format_utc(window.end)} ({humanise(window.end - now)} left)",
            window,
        )
    if state == STATE_PENDING:
        return Decision(
            ACTION_FORWARD,
            dag_id,
            f"maintenance window starts {format_utc(window.start)}",
            window,
        )
    return Decision(
        ACTION_FORWARD,
        dag_id,
        f"maintenance window expired {format_utc(window.end)}",
        window,
    )


# --------------------------------------------------------------------------
# Slack formatting
# --------------------------------------------------------------------------


def _truncate(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 15] + "\n...(truncated)"


def format_slack_payload(
    subject: str | None,
    message: str,
    decision: Decision,
    environment_label: str,
    silence_hint: str | None = None,
) -> dict:
    """Build a Slack incoming-webhook payload for a forwarded alert.

    The original alert text is kept intact. Notes from the silencer (expired
    window, invalid window, how to silence) are added as a context line so
    the people reading the channel can see the silencer's reasoning.
    """
    title = subject or (f"Airflow alert: {decision.dag_id}" if decision.dag_id else "Airflow alert")
    if environment_label:
        title = f"[{environment_label}] {title}"

    body = message
    try:
        parsed = json.loads(message)
        body = json.dumps(parsed, indent=2, sort_keys=True)
    except (TypeError, json.JSONDecodeError):
        pass

    notes = []
    if decision.detail and decision.detail != "no maintenance window":
        notes.append(decision.detail)
    if silence_hint:
        notes.append(silence_hint)

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": _truncate(title, 150)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "```" + _truncate(body, 2900) + "```"}},
    ]
    if notes:
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": _truncate(n, 300)} for n in notes],
            }
        )
    # "text" is the fallback shown in notifications and by clients without blocks.
    return {"text": _truncate(title, 300), "blocks": blocks}


def build_emf(
    namespace: str,
    metric_name: str,
    environment: str,
    dag_id: str,
    timestamp_ms: int,
    extra: dict | None = None,
) -> dict:
    """Build a CloudWatch Embedded Metric Format record with value 1.

    When printed as one JSON log line from Lambda, CloudWatch Logs extracts
    the metric. The metric has two dimension sets: by Environment, and by
    Environment and DagId, so a dashboard can show the total and the
    per-DAG breakdown.
    """
    record = {
        "_aws": {
            "Timestamp": timestamp_ms,
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [["Environment"], ["Environment", "DagId"]],
                    "Metrics": [{"Name": metric_name, "Unit": "Count"}],
                }
            ],
        },
        "Environment": environment,
        "DagId": dag_id,
        metric_name: 1,
    }
    for key, value in (extra or {}).items():
        if key not in record:
            record[key] = value
    return record


def silence_hint(dag_id: str | None) -> str | None:
    if not dag_id:
        return None
    return f"To silence: `python maintenance.py set {dag_id} --for 2d --reason \"...\"`"
