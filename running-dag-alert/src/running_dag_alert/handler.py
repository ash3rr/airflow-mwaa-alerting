"""Scheduled Lambda: count running DAG runs per MWAA environment, alert above a threshold.

Calls the Airflow REST API through the MWAA InvokeRestApi API, so there is no
Airflow username or password anywhere. Access is controlled by the IAM
permission airflow:InvokeRestApi on an Airflow role ARN
(arn:aws:airflow:<region>:<account>:role/<environment>/<Viewer|User|Op|Admin>).

Per environment, every run:
  1. list DAG runs in state running across all DAGs (paginated),
  2. publish RunningDagRuns and RunningDags as CloudWatch metrics (EMF log line),
  3. if the count is above the threshold and the cooldown has passed, post
     the list of DAGs to Slack and record the alert time in SSM.

One environment failing does not stop the others. The invocation fails at
the end if any environment failed, so the Lambda Errors metric shows it.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from running_dag_alert import core

# --------------------------------------------------------------------------
# Configuration (environment variables set by the template)
# --------------------------------------------------------------------------
ENVIRONMENTS = os.environ.get("MWAA_ENVIRONMENTS", "")  # "dev=mwaa-dev,preprod=mwaa-preprod"
THRESHOLD = int(os.environ.get("THRESHOLD", "10"))
COUNT_BY = os.environ.get("COUNT_BY", "runs")  # "runs" or "dags"
COOLDOWN = timedelta(minutes=int(os.environ.get("COOLDOWN_MINUTES", "60")))
STATE_PREFIX = os.environ.get("STATE_PREFIX", "/airflow/running-dag-alert")
SLACK_SECRET_ID = os.environ.get("SLACK_WEBHOOK_SECRET_ID", "")
SLACK_SECRET_KEY = os.environ.get("SLACK_WEBHOOK_SECRET_KEY", "webhook_url")
METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "Airflow/Alerting")
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", str(core.DEFAULT_PAGE_SIZE)))
HTTP_TIMEOUT_SECONDS = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "5"))

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_mwaa = boto3.client("mwaa")
_ssm = boto3.client("ssm")
_secrets = boto3.client("secretsmanager")


# --------------------------------------------------------------------------
# MWAA
# --------------------------------------------------------------------------


def describe_environment(name: str) -> tuple[int, str | None]:
    """Return (Airflow major version, web UI URL) for logging and the alert link."""
    env = _mwaa.get_environment(Name=name)["Environment"]
    major = core.airflow_major(env["AirflowVersion"])
    host = env.get("WebserverUrl")
    return major, f"https://{host}" if host else None


def invoke(name: str, request: dict) -> dict:
    """Call one Airflow REST endpoint through InvokeRestApi.

    Path is given without /api/v1 or /api/v2. MWAA routes it to the REST API
    of the environment's Airflow version.
    """
    try:
        resp = _mwaa.invoke_rest_api(Name=name, **request)
    except ClientError as exc:
        # RestApiClientException / RestApiServerException carry the Airflow
        # status and body in the error response.
        err = exc.response
        raise core.ApiError(
            f"{name}: {err.get('Error', {}).get('Code')} "
            f"status={err.get('RestApiStatusCode')} body={str(err.get('RestApiResponse'))[:300]}"
        ) from exc
    status = resp.get("RestApiStatusCode")
    if status != 200:
        raise core.ApiError(f"{name}: Airflow returned HTTP {status}")
    body = resp.get("RestApiResponse")
    if isinstance(body, str):
        body = json.loads(body)
    return body


def running_runs(name: str) -> list[dict]:
    def fetch(offset: int, limit: int) -> dict:
        return invoke(name, core.running_runs_request(offset, limit))

    return list(core.iter_dag_runs(fetch, page_size=PAGE_SIZE))


# --------------------------------------------------------------------------
# Alert state and delivery
# --------------------------------------------------------------------------


def _state_name(label: str) -> str:
    return f"{STATE_PREFIX.rstrip('/')}/{label}/last-alert"


def last_alert_at(label: str) -> datetime | None:
    try:
        value = _ssm.get_parameter(Name=_state_name(label))["Parameter"]["Value"]
    except _ssm.exceptions.ParameterNotFound:
        return None
    return datetime.fromisoformat(value)


def record_alert(label: str, when: datetime) -> None:
    _ssm.put_parameter(Name=_state_name(label), Value=when.isoformat(), Type="String", Overwrite=True)


def post_to_slack(payload: dict) -> None:
    secret = _secrets.get_secret_value(SecretId=SLACK_SECRET_ID)["SecretString"].strip()
    url = json.loads(secret)[SLACK_SECRET_KEY] if secret.startswith("{") else secret
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Slack returned HTTP {resp.status}")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Slack returned HTTP {exc.code}") from exc


# --------------------------------------------------------------------------
# Handler
# --------------------------------------------------------------------------


def check_environment(env: core.Environment, now: datetime) -> dict:
    major, ui_url = describe_environment(env.mwaa_name)
    summaries = core.summarise(running_runs(env.mwaa_name))
    runs, dags = core.total_runs(summaries), len(summaries)

    # print, not logger: EMF needs the raw JSON as the whole log line.
    print(json.dumps(core.build_emf(METRIC_NAMESPACE, env.label, runs, dags, int(time.time() * 1000))))
    logger.info("%s (Airflow %d): %d running runs across %d DAGs", env.label, major, runs, dags)

    over = core.is_over_threshold(summaries, THRESHOLD, COUNT_BY)
    alerted = False
    if core.should_alert(over, last_alert_at(env.label), now, COOLDOWN):
        post_to_slack(core.format_alert(env.label, summaries, THRESHOLD, now, COUNT_BY, ui_url))
        record_alert(env.label, now)
        alerted = True
    elif over:
        logger.info("%s over threshold, alert suppressed by %s cooldown", env.label, COOLDOWN)
    return {"runs": runs, "dags": dags, "over_threshold": over, "alerted": alerted}


def lambda_handler(event: dict, context: object) -> dict:
    now = datetime.now(timezone.utc)
    results, failed = {}, []
    for env in core.parse_environments(ENVIRONMENTS):
        try:
            results[env.label] = check_environment(env, now)
        except (core.ApiError, core.ConfigError, ClientError, BotoCoreError, RuntimeError, ValueError):
            logger.exception("Check failed for %s", env.label)
            failed.append(env.label)
    if failed:
        raise RuntimeError(f"running-DAG check failed for: {', '.join(failed)}")
    return results
