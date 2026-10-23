"""Lambda handler: sits between the Airflow alert SNS topic and Slack.

For each SNS record it finds the DAG id, looks up
<MAINTENANCE_PREFIX>/<dag_id> in SSM Parameter Store, and either suppresses
the alert (inside an active window) or forwards it to a Slack incoming
webhook. Suppressed alerts are written as a CloudWatch Embedded Metric Format
log line, which both logs the event and publishes the SuppressedAlerts metric.

The handler fails open. If the DAG id cannot be found, the window is
malformed, or SSM cannot be read, the alert is forwarded.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from silencer import core

# --------------------------------------------------------------------------
# Configuration (environment variables set by the template)
# --------------------------------------------------------------------------
MAINTENANCE_PREFIX = os.environ.get("MAINTENANCE_PREFIX", "/airflow/maintenance")
SLACK_SECRET_ID = os.environ.get("SLACK_WEBHOOK_SECRET_ID", "")
SLACK_SECRET_KEY = os.environ.get("SLACK_WEBHOOK_SECRET_KEY", "webhook_url")
ENVIRONMENT_LABEL = os.environ.get("ENVIRONMENT_LABEL", "")
METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "Airflow/Alerting")
DELETE_EXPIRED = os.environ.get("DELETE_EXPIRED_WINDOWS", "false").lower() == "true"
SECRET_CACHE_SECONDS = int(os.environ.get("SECRET_CACHE_SECONDS", "300"))
HTTP_TIMEOUT_SECONDS = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "5"))

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_ssm = boto3.client("ssm")
_secrets = boto3.client("secretsmanager")
_webhook_cache: dict[str, object] = {"url": None, "fetched_at": 0.0}


class SlackDeliveryError(RuntimeError):
    """Raised when Slack rejects a message, so Lambda retries the event."""


# --------------------------------------------------------------------------
# AWS helpers
# --------------------------------------------------------------------------


def read_window(dag_id: str) -> tuple[str | None, int | None]:
    """Return (value, version) of the maintenance parameter, or (None, None)."""
    name = core.parameter_name(MAINTENANCE_PREFIX, dag_id)
    try:
        resp = _ssm.get_parameter(Name=name)
    except _ssm.exceptions.ParameterNotFound:
        return None, None
    return resp["Parameter"]["Value"], resp["Parameter"]["Version"]


def delete_window_if_unchanged(dag_id: str, version: int) -> None:
    """Delete an expired window, unless someone replaced it since we read it."""
    name = core.parameter_name(MAINTENANCE_PREFIX, dag_id)
    try:
        current = _ssm.get_parameter(Name=name)["Parameter"]["Version"]
        if current != version:
            logger.info("Window for %s changed since read, not deleting", dag_id)
            return
        _ssm.delete_parameter(Name=name)
        logger.info("Deleted expired maintenance window %s", name)
    except _ssm.exceptions.ParameterNotFound:
        return
    except (ClientError, BotoCoreError):
        # Cleanup is best effort. The window is already ignored because it
        # has expired, so a failed delete only leaves clutter for `list`.
        logger.exception("Could not delete expired window %s", name)


def slack_webhook_url() -> str:
    """Fetch the webhook URL from Secrets Manager, cached for a few minutes.

    The secret may be a plain URL string or JSON with the URL under
    SLACK_WEBHOOK_SECRET_KEY.
    """
    now = time.monotonic()
    cached = _webhook_cache["url"]
    if cached and now - float(_webhook_cache["fetched_at"]) < SECRET_CACHE_SECONDS:
        return str(cached)

    secret = _secrets.get_secret_value(SecretId=SLACK_SECRET_ID)["SecretString"].strip()
    url = secret
    if secret.startswith("{"):
        url = json.loads(secret)[SLACK_SECRET_KEY]
    if not url.startswith("https://"):
        raise ValueError("Slack webhook secret does not contain an https URL")
    _webhook_cache.update(url=url, fetched_at=now)
    return url


def post_to_slack(payload: dict) -> None:
    req = urllib.request.Request(
        slack_webhook_url(),
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                raise SlackDeliveryError(f"Slack returned HTTP {resp.status}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:200]
        raise SlackDeliveryError(f"Slack returned HTTP {exc.code}: {body}") from exc


def emit_suppressed_metric(decision: core.Decision, subject: str | None) -> None:
    """Print one EMF line. CloudWatch turns it into the SuppressedAlerts metric."""
    record = core.build_emf(
        namespace=METRIC_NAMESPACE,
        metric_name="SuppressedAlerts",
        environment=ENVIRONMENT_LABEL or "default",
        dag_id=decision.dag_id or "unknown",
        timestamp_ms=int(time.time() * 1000),
        extra={
            "event": "alert_suppressed",
            "reason": decision.window.reason if decision.window else "",
            "set_by": decision.window.set_by if decision.window else "",
            "window_end": core.format_utc(decision.window.end) if decision.window else "",
            "subject": subject or "",
        },
    )
    # print, not logger: EMF needs the raw JSON as the whole log line.
    print(json.dumps(record))


# --------------------------------------------------------------------------
# Handler
# --------------------------------------------------------------------------


def handle_record(record: dict, now: datetime) -> str:
    sns = record["Sns"]
    message = sns.get("Message") or ""
    subject = sns.get("Subject")

    dag_id = core.extract_dag_id(message, subject)
    raw_window, version = None, None
    lookup_failed = False
    if dag_id:
        try:
            raw_window, version = read_window(dag_id)
        except (ClientError, BotoCoreError):
            logger.exception("Could not read maintenance window for %s, forwarding", dag_id)
            lookup_failed = True

    decision = core.decide(dag_id, raw_window, now)
    if lookup_failed:
        decision = core.Decision(core.ACTION_FORWARD, dag_id, "maintenance lookup failed, forwarded")

    if decision.action == core.ACTION_SUPPRESS:
        emit_suppressed_metric(decision, subject)
        return decision.action

    logger.info("Forwarding alert dag_id=%s detail=%s", dag_id, decision.detail)
    payload = core.format_slack_payload(
        subject=subject,
        message=message,
        decision=decision,
        environment_label=ENVIRONMENT_LABEL,
        silence_hint=core.silence_hint(dag_id),
    )
    post_to_slack(payload)

    expired = decision.window is not None and core.window_state(decision.window, now) == core.STATE_EXPIRED
    if DELETE_EXPIRED and expired and version is not None:
        delete_window_if_unchanged(dag_id, version)
    return decision.action


def lambda_handler(event: dict, context: object) -> dict:
    now = datetime.now(timezone.utc)
    results = {core.ACTION_FORWARD: 0, core.ACTION_SUPPRESS: 0}
    for record in event.get("Records", []):
        action = handle_record(record, now)
        results[action] += 1
    return results
