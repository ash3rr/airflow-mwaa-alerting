"""Example Airflow failure callback that publishes to the alert SNS topic.

Put this in a shared module in the DAGs folder and set it as
on_failure_callback in default_args. The JSON body includes dag_id as a
top-level key, which is what the silencer looks for first. Works on
Airflow 2.x and 3.x.

The MWAA execution role needs sns:Publish on the topic.
"""

import json
import logging
import os

import boto3

# The topic ARN is read from the environment (or an Airflow Variable) so the
# same DAG code runs in every environment.
ALERT_TOPIC_ARN = os.environ.get("AIRFLOW_ALERT_TOPIC_ARN", "")

log = logging.getLogger(__name__)


def notify_failed_task(context):
    ti = context["ti"]
    logical_date = context.get("logical_date")
    exception = context.get("exception")

    message = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "try_number": ti.try_number,
        "logical_date": logical_date.isoformat() if logical_date else None,
        "exception": str(exception)[:1000] if exception else None,
        "log_url": getattr(ti, "log_url", None),
    }
    subject = f"Airflow task failed: {ti.dag_id}.{ti.task_id}"[:100]

    if not ALERT_TOPIC_ARN:
        log.error("AIRFLOW_ALERT_TOPIC_ARN not set, alert not sent: %s", message)
        return
    boto3.client("sns").publish(TopicArn=ALERT_TOPIC_ARN, Subject=subject, Message=json.dumps(message))
